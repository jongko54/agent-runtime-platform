"""Test-only child: terminate at a precise durable execution boundary."""

import asyncio
import os
import signal
import sys

from agent_platform.adapters.postgres.database import create_engine
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.tools.mock_evaluation import MockEvaluationTool
from agent_platform.settings import Settings


async def main():
    engine = create_engine(Settings().database_url)
    repository = PostgresRunRepository(engine)
    work = await repository.claim_work()
    assert work is not None
    if sys.argv[1] == "after-tool-return":
        assert work.kind == "TOOL_CALL"
        result = await MockEvaluationTool().execute(
            tool_version="evaluation.run_suite:v1", arguments=work.input
        )
        assert result["decision"] == "EVALUATED"
    # The parent created this process solely for this kill/recovery test.
    os.kill(os.getpid(), signal.SIGKILL)


asyncio.run(main())
