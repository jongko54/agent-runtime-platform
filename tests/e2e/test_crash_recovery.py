import asyncio
import os
import signal
import sys

import pytest
from sqlalchemy import text

from agent_platform.adapters.models.mock import MockModelGateway
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.adapters.tools.mock_evaluation import MockEvaluationTool
from agent_platform.application.ports import CreateRunCommand
from agent_platform.application.runtime_kernel import RuntimeKernel


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["after-claim", "after-tool-return"])
async def test_killed_process_recovers_from_persisted_step(
    admin_engine, runtime_engine, runtime_url, boundary
):
    seed = await seed_example(admin_engine)
    repository = PostgresRunRepository(runtime_engine)
    accepted = await repository.accept_run(
        command=CreateRunCommand(
            seed.principal,
            seed.agent_version_id,
            {"candidate_model_ref": "mock://candidate", "evaluation_suite_ref": "mock://suite"},
        ),
        idempotency_key="crash-boundary",
    )
    if boundary == "after-tool-return":
        model_work = await repository.claim_work()
        assert model_work is not None
        await RuntimeKernel(repository, MockModelGateway(), MockEvaluationTool()).execute(
            model_work
        )
    environment = {**os.environ, "AGENT_PLATFORM_DATABASE_URL": runtime_url}
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "tests/helpers/crash_worker.py",
        boundary,
        env=environment,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        _, error = await asyncio.wait_for(child.communicate(), timeout=15)
        assert child.returncode == -signal.SIGKILL, error.decode()
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()
    # Expire only the dead test worker's lease; do not wait a production TTL.
    async with admin_engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE work_items SET lease_expires_at=clock_timestamp()-interval '1 second' "
                "WHERE run_id=:run AND status='PROCESSING'"
            ),
            {"run": accepted.run.id},
        )
    replacement = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "agent_platform.worker.main",
        env={
            **environment,
            "AGENT_PLATFORM_RETRY_BASE_SECONDS": "0.01",
            "AGENT_PLATFORM_WORKER_POLL_INTERVAL_SECONDS": "0.01",
        },
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        for _ in range(200):
            run = await repository.get_run(seed.principal, accepted.run.id)
            if run.state == "COMPLETED":
                break
            if replacement.returncode is not None:
                _, error = await replacement.communicate()
                pytest.fail(f"Replacement worker exited: {error.decode()}")
            await asyncio.sleep(0.05)
        else:
            pytest.fail("Replacement worker failed to recover the killed attempt")
        assert run.result["decision"] == "EVALUATED"
        async with admin_engine.connect() as conn:
            # Provider success is independent of the killed runtime commit. A
            # replacement request uses the same key, not a second provider effect.
            assert await conn.scalar(text("SELECT count(*) FROM mock_provider_results")) == 1
            assert await conn.scalar(text("SELECT count(*) FROM tool_effects")) == 1
            assert (
                await conn.scalar(
                    text("SELECT count(*) FROM run_attempts WHERE run_id=:run"), {"run": run.id}
                )
                == 3
            )
            assert (
                await conn.scalar(
                    text("SELECT count(*) FROM checkpoints WHERE run_id=:run"), {"run": run.id}
                )
                == 2
            )
            assert (
                await conn.scalar(
                    text("SELECT count(*) FROM model_calls WHERE run_id=:run"), {"run": run.id}
                )
                == 1
            )
            assert (
                await conn.scalar(
                    text("SELECT count(*) FROM usage_entries WHERE run_id=:run"), {"run": run.id}
                )
                == 2
            )
            if boundary == "after-tool-return":
                assert (
                    await conn.scalar(
                        text(
                            "SELECT count(*) FROM run_attempts a "
                            "JOIN run_steps s ON s.id=a.step_id "
                            "WHERE a.run_id=:run AND s.kind='MODEL_CALL'"
                        ),
                        {"run": run.id},
                    )
                    == 1
                )
    finally:
        if replacement.returncode is None:
            replacement.terminate()
            try:
                await asyncio.wait_for(replacement.wait(), timeout=5)
            except TimeoutError:
                replacement.kill()
                await replacement.wait()
