"""Run acceptance delegates its atomic boundary to the repository."""

from agent_platform.application.errors import InvalidInput
from agent_platform.application.ports import AcceptedRun, CreateRunCommand, RunRepository


class RunService:
    def __init__(self, repository: RunRepository) -> None:
        self.repository = repository

    async def create(self, command: CreateRunCommand, idempotency_key: str) -> AcceptedRun:
        if not idempotency_key.strip() or len(idempotency_key) > 200:
            raise InvalidInput("Invalid idempotency key")
        return await self.repository.accept_run(command=command, idempotency_key=idempotency_key)
