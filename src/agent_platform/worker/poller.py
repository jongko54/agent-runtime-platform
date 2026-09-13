from agent_platform.application.ports import RunRepository
from agent_platform.application.runtime_kernel import RuntimeKernel


class WorkerPoller:
    def __init__(self, repository: RunRepository, kernel: RuntimeKernel) -> None:
        self.repository = repository
        self.kernel = kernel

    async def poll_once(self) -> bool:
        work = await self.repository.claim_work()
        if work is None:
            return False
        await self.kernel.execute(work)
        return True
