import asyncio

import pytest

from agent_platform.application import telemetry
from agent_platform.application.ports import ClaimedWork

WORK = ClaimedWork("work", "tenant", "project", "user", "run", "step", "MODEL_CALL", {}, {})


class Recorder:
    def __init__(self, *, fail_start=False, fail_finish=False):
        self.fail_start = fail_start
        self.fail_finish = fail_finish
        self.outcomes = []

    def start(self, operation, work):
        if self.fail_start:
            raise RuntimeError("SECRET_start")
        return self

    def finish(self, outcome):
        self.outcomes.append(outcome)
        if self.fail_finish:
            raise RuntimeError("SECRET_finish")


@pytest.mark.parametrize("failure", ["start", "finish"])
def test_instrumentation_failure_does_not_replace_business_result(failure):
    recorder = Recorder(fail_start=failure == "start", fail_finish=failure == "finish")
    calls = []
    with telemetry.observe(recorder, "attempt", WORK) as observed:
        calls.append("ran")
        observed.outcome = "RECORDED"
    assert calls == ["ran"]


@pytest.mark.parametrize("exception", [RuntimeError("SECRET_business"), asyncio.CancelledError()])
def test_instrumentation_failure_preserves_original_exception_and_cancellation(exception):
    recorder = Recorder(fail_finish=True)
    with pytest.raises(type(exception)) as raised, telemetry.observe(recorder, "attempt", WORK):
        raise exception
    assert raised.value is exception
    assert recorder.outcomes == [
        "CANCELLED" if isinstance(exception, asyncio.CancelledError) else "ERROR"
    ]


def test_normal_scope_reports_explicit_outcome_without_raw_exception():
    recorder = Recorder()
    with telemetry.observe(recorder, "model", WORK) as observed:
        observed.outcome = "RETURNED"
    assert recorder.outcomes == ["RETURNED"]


def test_disabled_telemetry_has_no_side_effects():
    recorder = telemetry.NoopTelemetry()
    assert recorder.snapshot() == {}
    with telemetry.observe(recorder, "attempt", WORK):
        pass
    recorder.shutdown()
