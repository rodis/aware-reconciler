"""Recording a fact in the store, then telling Aware (`reconciler.recording`).

Every test here is about an ORDERING and its two failure directions, because that is the whole
content of the module: the store of record is written first and must succeed, the gateway POST
follows and is allowed to fail. Getting it backwards does not break a test somewhere else — it
sends the invoice twice, months later, on the one run where the network blinked.
"""

import logging

import pytest

from reconciler import app
from reconciler.core import Cycle
from reconciler.definition import GENESIS_STAGE
from reconciler.recording import DryRunWriter, StoreBackedEvents, StoreBackedMilestones

NOW = 1788000000


class Definition:
    name = "dreamhost_invoice"

    def event_name(self, stage):
        return f"{self.name}_{stage}"


class FakeWriter:
    """Records calls in order, and can be made to fail."""

    def __init__(self, fail=False):
        self.calls, self.fail = [], fail

    def _note(self, kind, *rest):
        if self.fail:
            raise RuntimeError("store unreachable")
        self.calls.append((kind, *rest))

    def put_cycle(self, cycle):
        self._note("cycle", cycle.key)

    def put_milestone(self, cycle, stage, occurred_at, body):
        self._note("milestone", cycle.key, stage, occurred_at)

    def open_task(self, body):
        self._note("open", body["upstream_id"])

    def close_task(self, body):
        self._note("close", body["upstream_id"])


class FakeSink:
    def __init__(self, fail=False):
        self.calls, self.fail = [], fail

    def record(self, cycle, stage, payload, *, now=None):
        if self.fail:
            raise RuntimeError("gateway unreachable")
        self.calls.append(("record", cycle.key, stage, now))

    def emit(self, payload):
        if self.fail:
            raise RuntimeError("gateway unreachable")
        self.calls.append(("emit", payload["event_name"], payload["upstream_id"]))


def cycle():
    return Cycle(key="dh_invoice_2026_011", process="dreamhost_invoice",
                 user_id="rods", opened_at=NOW, context={"invoice_number": 11})


def opened(uid="mail-1", when=NOW):
    return {"event_name": "email_labeled_todo", "user_id": "rods", "timestamp": when,
            "label": "aware/todo", "upstream_id": uid, "subject": "Renew insurance"}


def closed(uid="mail-1", when=NOW):
    return {"event_name": "email_task_closed", "user_id": "rods", "timestamp": when,
            "label": "aware/todo", "upstream_id": uid, "closed_via": "sweep"}


# --- the ordering -----------------------------------------------------------------------------

def test_the_store_is_written_before_the_gateway_is_told():
    writer, sink = FakeWriter(), FakeSink()
    StoreBackedMilestones(Definition(), writer, sink).record(cycle(), "total_computed", {})
    assert writer.calls and sink.calls, "both halves must run"
    assert writer.calls[0][0] == "milestone"
    assert sink.calls[0][0] == "record"


def test_a_failed_gateway_post_does_not_fail_the_run(caplog):
    """Best-effort, and the fact IS recorded — so raising would fail a run that succeeded, and
    the next run would skip the stage anyway because the store has it. The exception would buy
    a red workflow and nothing else."""
    writer, sink = FakeWriter(), FakeSink(fail=True)
    with caplog.at_level(logging.ERROR):
        milestone = StoreBackedMilestones(Definition(), writer, sink).record(
            cycle(), "total_computed", {})

    assert writer.calls, "the store write still happened"
    assert milestone.stage == "total_computed"
    assert "gateway POST failed" in caplog.text, "a silent drop would be the real bug"


def test_a_failed_store_write_stops_everything():
    """The opposite direction, and it must NOT be swallowed. Nothing was recorded, so nothing
    is gated — a run that carried on would leave the act un-recorded and the next run would
    redo it, which for `invoice_emailed` means sending it twice."""
    writer, sink = FakeWriter(fail=True), FakeSink()
    with pytest.raises(RuntimeError, match="store unreachable"):
        StoreBackedMilestones(Definition(), writer, sink).record(cycle(), "invoice_emailed", {})
    assert not sink.calls, "the gateway must not be told about a fact that was not recorded"


def test_both_halves_get_the_same_timestamp():
    """Two clocks would disagree by a second for no reason — and a satisfied `await` carries
    its EVIDENCE's time, not now, which is exactly the value that must not be recomputed."""
    writer, sink = FakeWriter(), FakeSink()
    evidence = NOW - 3600
    StoreBackedMilestones(Definition(), writer, sink).record(
        cycle(), "data_approved", {"matched_at": evidence})

    assert writer.calls[0][3] == evidence          # occurred_at
    assert sink.calls[0][3] == evidence            # now= passed through


def test_the_genesis_stage_writes_the_cycle_itself():
    """`cycles()` reads a table that only the genesis populates. Writing just a milestone row
    would leave a cycle whose milestones exist and whose cycle does not — invisible to every
    later run."""
    writer, sink = FakeWriter(), FakeSink()
    StoreBackedMilestones(Definition(), writer, sink).record(cycle(), GENESIS_STAGE, {})
    assert writer.calls[0][0] == "cycle"


# --- the task half ----------------------------------------------------------------------------

@pytest.mark.parametrize("body, expected", [(opened(), "open"), (closed(), "close")])
def test_task_events_route_to_the_right_write(body, expected):
    writer, sink = FakeWriter(), FakeSink()
    StoreBackedEvents(writer, sink).emit(body)
    assert writer.calls[0][0] == expected
    assert sink.calls[0][0] == "emit"


def test_a_failed_task_post_still_leaves_the_task_recorded(caplog):
    writer, sink = FakeWriter(), FakeSink(fail=True)
    with caplog.at_level(logging.ERROR):
        StoreBackedEvents(writer, sink).emit(closed())
    assert writer.calls[0] == ("close", "mail-1")
    assert "gateway POST failed" in caplog.text


# --- when the wrapping happens at all ----------------------------------------------------------

@pytest.fixture
def env(monkeypatch):
    for var in ("RECONCILER_STORE", "NEON_DATABASE_URL", "MYSQL_DATABASE_URL",
                "MYSQL_SSL_CA", "VECTOR_BASE_URL"):
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


def test_on_neon_nothing_wraps_the_sink(env):
    """THE rule this must not break: on Neon the gateway POST *is* the write, because Vector
    persists it. A writer there would make the tier a second writer competing with the
    persister — which ADR 0012 forbids and `adapters/neon.py` opens by saying."""
    env.setenv("NEON_DATABASE_URL", "postgres://x/y")
    env.setenv("VECTOR_BASE_URL", "http://vector")
    sink = app.sink_for(app.RunOptions(), Definition())
    assert not isinstance(sink, StoreBackedMilestones)


def test_on_mysql_the_sink_is_wrapped(env):
    env.setenv("RECONCILER_STORE", "mysql")
    env.setenv("MYSQL_DATABASE_URL", "mysql://u:p@h/db")
    env.setenv("MYSQL_SSL_CA", "tests/fixtures/mysql-auto-generated-ca.pem")
    env.setenv("VECTOR_BASE_URL", "http://vector")
    assert isinstance(app.sink_for(app.RunOptions(), Definition()), StoreBackedMilestones)


def test_a_dry_run_writes_to_neither(env):
    """A preview that populated the store while sending nothing would be the most misleading
    possible version of --dry-run."""
    env.setenv("RECONCILER_STORE", "mysql")
    env.setenv("MYSQL_DATABASE_URL", "mysql://u:p@h/db")
    sink = app.sink_for(app.RunOptions(dry_run=True), Definition())
    assert isinstance(sink, StoreBackedMilestones)
    sink.record(cycle(), "total_computed", {})
    assert isinstance(sink._writer, DryRunWriter)
    assert sink._writer.written == [("milestone", "dh_invoice_2026_011/total_computed")]


def test_a_dry_run_on_mysql_needs_no_connection_string(env):
    """`DryRunWriter` opens nothing, so demanding a DSN would make a preview impossible exactly
    where previewing matters most — before the store exists."""
    env.setenv("RECONCILER_STORE", "mysql")
    app.sink_for(app.RunOptions(dry_run=True), Definition())      # must not raise
