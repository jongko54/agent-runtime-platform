import asyncio

from agent_platform.application.errors import RuntimeConflict
from agent_platform.application.ports import ClaimedWork, RunRepository
from agent_platform.application.runtime_kernel import RuntimeKernel


class WorkerPoller:
    def __init__(
        self,
        repository: RunRepository,
        kernel: RuntimeKernel,
        heartbeat_interval_seconds: float = 5,
    ) -> None:
        self.repository = repository
        self.kernel = kernel
        self.heartbeat_interval_seconds = heartbeat_interval_seconds

    async def poll_once(self) -> bool:
        await self.repository.recover_expired()
        work = await self.repository.claim_work()
        if work is None:
            return False
        execution = asyncio.create_task(self.kernel.execute(work))
        heartbeat = asyncio.create_task(self._heartbeat(work))
        try:
            done, _ = await asyncio.wait(
                (execution, heartbeat), return_when=asyncio.FIRST_COMPLETED
            )
            if heartbeat in done:
                # An uncertain renewal has the same fail-closed behavior as lost
                # ownership. The finally block cancels and awaits provider work.
                await heartbeat
            await execution
            return True
        finally:
            execution.cancel()
            heartbeat.cancel()
            await asyncio.gather(execution, heartbeat, return_exceptions=True)

    async def _heartbeat(self, work: ClaimedWork) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval_seconds)
            try:
                # A stalled DB call is uncertain ownership too. With heartbeat
                # < lease / 3, the renewal budget leaves room to stop execution.
                async with asyncio.timeout(self.heartbeat_interval_seconds):
                    renewed = await self.repository.heartbeat(work)
            except Exception:
                raise RuntimeConflict("Worker could not renew its execution lease") from None
            if not renewed:
                raise RuntimeConflict("Worker lost its execution lease")
