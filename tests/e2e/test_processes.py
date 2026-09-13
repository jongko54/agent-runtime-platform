"""Real HTTP and separate worker processes against the disposable database."""

import asyncio
import os
import socket
import sys

import httpx
import pytest
from sqlalchemy import text

from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.application.ports import CreateRunCommand


def process_environment(runtime_url):
    return {
        **os.environ,
        "AGENT_PLATFORM_DATABASE_URL": runtime_url,
        "AGENT_PLATFORM_DEVELOPMENT_MODE": "true",
        "AGENT_PLATFORM_DEVELOPMENT_TOKEN": "process-test-local-token",
        "AGENT_PLATFORM_DEVELOPMENT_TENANT_ID": "demo",
        "AGENT_PLATFORM_DEVELOPMENT_PRINCIPAL_ID": "demo-user",
    }


async def stop_process(process):
    if process.returncode is None:
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except TimeoutError:
            process.kill()
            await process.wait()


@pytest.mark.asyncio
async def test_separate_api_and_worker_processes(admin_engine, runtime_url):
    seed = await seed_example(admin_engine)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    environment = process_environment(runtime_url)
    api = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "uvicorn",
        "agent_platform.api.app:create_app",
        "--factory",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--no-access-log",
        env=environment,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    worker = None
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client:
            for _ in range(100):
                try:
                    if (await client.get("/health/ready")).status_code == 200:
                        break
                except httpx.TransportError:
                    pass
                await asyncio.sleep(0.05)
            else:
                pytest.fail("API process did not become ready")
            headers = {
                "Authorization": "Bearer process-test-local-token",
                "Idempotency-Key": "process-smoke",
            }
            response = await client.post(
                "/v1/runs",
                headers=headers,
                json={
                    "agent_version_id": seed.agent_version_id,
                    "input": {
                        "candidate_model_ref": "mock://candidate",
                        "evaluation_suite_ref": "mock://suite",
                    },
                },
            )
            assert response.status_code == 202, response.text
            worker = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "agent_platform.worker.main",
                "--drain",
                env=environment,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, error = await asyncio.wait_for(worker.communicate(), timeout=15)
            assert worker.returncode == 0, error.decode()
            run = await client.get(f"/v1/runs/{response.json()['run_id']}", headers=headers)
            assert run.status_code == 200
            assert run.json()["state"] == "COMPLETED"
            assert run.json()["result"]["decision"] == "EVALUATED"
    finally:
        if worker is not None:
            await stop_process(worker)
        await stop_process(api)


@pytest.mark.asyncio
async def test_two_worker_processes_complete_without_global_lock(
    admin_engine, runtime_engine, runtime_url
):
    seed = await seed_example(admin_engine)
    repository = PostgresRunRepository(runtime_engine)
    command = CreateRunCommand(
        seed.principal,
        seed.agent_version_id,
        {"candidate_model_ref": "mock://candidate", "evaluation_suite_ref": "mock://suite"},
    )
    runs = [
        await repository.accept_run(command=command, idempotency_key=f"parallel-{i}")
        for i in range(10)
    ]
    # Holding the legacy singleton lock must not block independently leased workers.
    async with admin_engine.connect() as guard:
        await guard.execute(text("SELECT pg_advisory_lock(721643817)"))
        await guard.commit()
        processes = []
        try:
            for _ in range(2):
                processes.append(
                    await asyncio.create_subprocess_exec(
                        sys.executable,
                        "-m",
                        "agent_platform.worker.main",
                        "--drain",
                        env=process_environment(runtime_url),
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.PIPE,
                    )
                )
            for process in processes:
                _, error = await asyncio.wait_for(process.communicate(), timeout=15)
                assert process.returncode == 0, error.decode()
            for run in runs:
                assert (await repository.get_run(seed.principal, run.run.id)).state == "COMPLETED"
        finally:
            for process in processes:
                await stop_process(process)
            await guard.execute(text("SELECT pg_advisory_unlock(721643817)"))
            await guard.commit()
