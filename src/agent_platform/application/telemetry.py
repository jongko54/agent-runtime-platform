"""Small telemetry port. Execution owns its outcomes, never an exporter."""

import asyncio
import logging
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Protocol

from agent_platform.application.errors import (
    InvalidInput,
    PolicyDenied,
    RuntimeConflict,
)
from agent_platform.application.ports import ClaimedWork


class SpanHandle(Protocol):
    def finish(self, outcome: str) -> None: ...


class RuntimeTelemetry(Protocol):
    def start(self, operation: str, work: ClaimedWork) -> SpanHandle: ...

    def snapshot(self) -> dict[str, int]: ...

    def shutdown(self) -> None: ...


class NoopSpan:
    def finish(self, outcome: str) -> None:
        pass


class NoopTelemetry:
    def start(self, operation: str, work: ClaimedWork) -> SpanHandle:
        return NoopSpan()

    def snapshot(self) -> dict[str, int]:
        return {}

    def shutdown(self) -> None:
        pass


@dataclass(slots=True)
class Observation:
    outcome: str = "RETURNED"


def _warn() -> None:
    try:
        logging.getLogger(__name__).warning(
            "Telemetry instrumentation unavailable; execution continues"
        )
    except Exception:
        # A logging handler is also outside the execution correctness boundary.
        pass


@contextmanager
def observe(
    telemetry: RuntimeTelemetry, operation: str, work: ClaimedWork
) -> Generator[Observation]:
    handle: SpanHandle = NoopSpan()
    try:
        handle = telemetry.start(operation, work)
    except Exception:
        _warn()
    observed = Observation()
    try:
        yield observed
    except BaseException as error:
        if isinstance(error, asyncio.CancelledError):
            observed.outcome = "CANCELLED"
        elif isinstance(error, TimeoutError):
            observed.outcome = "TIMED_OUT"
        elif isinstance(error, RuntimeConflict):
            observed.outcome = "CONFLICT"
        elif isinstance(error, PolicyDenied):
            observed.outcome = "POLICY_DENIED"
        elif isinstance(error, InvalidInput):
            observed.outcome = "CLIENT_INVALID"
        else:
            observed.outcome = "ERROR"
        raise
    finally:
        try:
            handle.finish(observed.outcome)
        except Exception:
            _warn()
