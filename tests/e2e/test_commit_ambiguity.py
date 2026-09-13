import pytest
from sqlalchemy import event, text

from agent_platform.adapters.models.mock import MockModelGateway
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.adapters.tools.mock_evaluation import MockEvaluationTool
from agent_platform.application.ports import CreateRunCommand
from agent_platform.application.runtime_kernel import RuntimeKernel
from agent_platform.worker.poller import WorkerPoller


async def accept(repository, seed):
    return await repository.accept_run(
        command=CreateRunCommand(
            seed.principal,
            seed.agent_version_id,
            {"candidate_model_ref": "mock://candidate", "evaluation_suite_ref": "mock://suite"},
        ),
        idempotency_key="commit-boundary",
    )


@pytest.mark.asyncio
async def test_commit_ack_loss_does_not_fail_or_repeat_successful_model(
    admin_engine, runtime_engine, monkeypatch
):
    seed = await seed_example(admin_engine)
    repository = PostgresRunRepository(runtime_engine)
    accepted = await accept(repository, seed)
    model = await repository.claim_work()
    assert model is not None
    original_complete = repository.complete_model

    async def lose_ack(work, decision):
        await original_complete(work, decision)
        raise ConnectionError("Simulated post-commit acknowledgement loss")

    monkeypatch.setattr(repository, "complete_model", lose_ack)
    kernel = RuntimeKernel(repository, MockModelGateway(), MockEvaluationTool())
    with pytest.raises(ConnectionError, match="acknowledgement"):
        await kernel.execute(model)
    run = await repository.get_run(seed.principal, accepted.run.id)
    assert run.state == "QUEUED" and run.error is None
    assert await repository.recover_expired() == 0
    monkeypatch.setattr(repository, "complete_model", original_complete)
    assert await WorkerPoller(repository, kernel).poll_once()
    assert (await repository.get_run(seed.principal, run.id)).state == "COMPLETED"
    async with admin_engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM run_attempts")) == 2
        assert await conn.scalar(text("SELECT count(*) FROM checkpoints")) == 2


@pytest.mark.asyncio
async def test_checkpoint_failure_rolls_back_result_and_next_step(admin_engine, runtime_engine):
    seed = await seed_example(admin_engine)
    repository = PostgresRunRepository(runtime_engine)
    accepted = await accept(repository, seed)
    model = await repository.claim_work()
    assert model is not None

    def fail_checkpoint(connection, cursor, statement, parameters, context, executemany):
        if "INSERT INTO checkpoints" in statement:
            raise ConnectionError("Injected checkpoint write failure")

    event.listen(runtime_engine.sync_engine, "before_cursor_execute", fail_checkpoint)
    try:
        with pytest.raises(ConnectionError, match="checkpoint"):
            await RuntimeKernel(repository, MockModelGateway(), MockEvaluationTool()).execute(model)
    finally:
        event.remove(runtime_engine.sync_engine, "before_cursor_execute", fail_checkpoint)
    run = await repository.get_run(seed.principal, accepted.run.id)
    assert run.state == "WAITING_MODEL" and run.error is None
    async with admin_engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM checkpoints")) == 0
        assert await conn.scalar(text("SELECT count(*) FROM run_steps")) == 1
        assert await conn.scalar(text("SELECT count(*) FROM work_items")) == 1
        assert await conn.scalar(text("SELECT response FROM model_calls")) is None
        assert await conn.scalar(text("SELECT status FROM run_attempts")) == "RUNNING"
    # The unchanged live lease may still commit the same decision after DB recovery.
    await RuntimeKernel(repository, MockModelGateway(), MockEvaluationTool()).execute(model)
    assert (await repository.get_run(seed.principal, accepted.run.id)).state == "QUEUED"


@pytest.mark.asyncio
async def test_registered_tool_schema_error_is_terminal_not_a_crash_retry(
    admin_engine, runtime_engine
):
    seed = await seed_example(admin_engine)
    repository = PostgresRunRepository(runtime_engine)
    accepted = await accept(repository, seed)
    work = await repository.claim_work()
    assert work is not None

    class InvalidArgumentsModel:
        async def decide(self, *, input, allowed_tools):
            return {"tool_version": "evaluation.run_suite:v1", "arguments": {}}

    await RuntimeKernel(repository, InvalidArgumentsModel(), MockEvaluationTool()).execute(work)
    run = await repository.get_run(seed.principal, accepted.run.id)
    assert run.state == "FAILED"
    assert run.error["code"] == "CLIENT_INVALID"
    assert await repository.recover_expired() == 0
    async with admin_engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM checkpoints")) == 0
        assert await conn.scalar(text("SELECT count(*) FROM run_attempts")) == 1
