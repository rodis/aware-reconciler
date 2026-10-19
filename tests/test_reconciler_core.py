"""Tests for the pure reconciliation core (ADR 0012).

These run against a dict-backed `World`, with no runner, no HTTP and no LLM — which is the
point of keeping `core` import-clean. Everything asserted here is a property the design
argues for in prose, so a regression shows up as a failing test rather than as a process
that quietly takes a week longer than it should.
"""

import pytest

from reconciler.core import Cycle, Milestone, Status, reconcile
from reconciler.definition import ProcessDefinition


class FakeWorld:
    """Records what was asked of it; `find` answers from a scripted set of available facts."""

    def __init__(self, available: dict[str, dict] | None = None, clock: int = 1000):
        self.available = available or {}     # stage name -> payload the world will yield
        self.clock = clock
        self.acted: list[str] = []
        self.looked: list[tuple[str, int]] = []
        self.recorded: list[tuple[str, dict]] = []

    def act(self, action, cycle, milestones):
        self.acted.append(action)
        return {"action": action}

    def find(self, signal, cycle, since, milestones):
        stage = signal["_stage"]             # test-only: lets the fake key off the stage
        self.looked.append((stage, since))
        return self.available.get(stage)

    def record(self, cycle, stage, payload):
        self.clock += 10
        self.recorded.append((stage, payload))
        return Milestone(stage=stage, timestamp=self.clock, payload=payload)


def _proc(*stages) -> ProcessDefinition:
    return ProcessDefinition.model_validate(
        {"name": "p", "cycle_key": "p_{year}", "stages": list(stages)}
    )


def _act(name, after=()):
    return {"name": name, "kind": "act", "action": f"do.{name}", "after": list(after)}


def _await(name, after=()):
    return {"name": name, "kind": "await", "after": list(after),
            "signal": {"source": "test", "_stage": name}}


@pytest.fixture
def cycle():
    return Cycle(key="p_2026_001", process="p", user_id="u", opened_at=500)


# --- a run walks the whole ready chain --------------------------------------------------

def test_a_run_advances_through_every_consecutive_act(cycle):
    """The property the ADR's first pseudocode got wrong.

    Without advancing the local milestone view inside the loop, each run would perform
    exactly ONE stage — a four-act chain would take four days instead of one run.
    """
    proc = _proc(_act("a"), _act("b", ["a"]), _act("c", ["b"]), _act("d", ["c"]))
    world = FakeWorld()

    outcome = reconcile(proc, cycle, {}, world)

    assert outcome.status is Status.COMPLETE
    assert outcome.advanced == ["a", "b", "c", "d"]
    assert world.acted == ["do.a", "do.b", "do.c", "do.d"]


def test_it_stops_at_an_await_that_finds_nothing(cycle):
    proc = _proc(_act("a"), _await("b", ["a"]), _act("c", ["b"]))
    world = FakeWorld()

    outcome = reconcile(proc, cycle, {}, world)

    assert outcome.status is Status.ADVANCED
    assert outcome.advanced == ["a"]
    assert outcome.waiting_on == ["b"]
    assert "do.c" not in world.acted        # c must not run ahead of its predecessor


def test_a_satisfied_await_lets_the_rest_of_the_chain_complete(cycle):
    proc = _proc(_act("a"), _await("b", ["a"]), _act("c", ["b"]))
    world = FakeWorld(available={"b": {"evidence": "mail-1"}})

    outcome = reconcile(proc, cycle, {}, world)

    assert outcome.status is Status.COMPLETE
    assert outcome.advanced == ["a", "b", "c"]


# --- idempotence and resumption ---------------------------------------------------------

def test_recorded_stages_are_never_redone(cycle):
    proc = _proc(_act("a"), _act("b", ["a"]))
    world = FakeWorld()
    done = {"a": Milestone("a", 600, {})}

    outcome = reconcile(proc, cycle, done, world)

    assert outcome.advanced == ["b"]
    assert world.acted == ["do.b"]


def test_a_second_run_over_a_complete_cycle_does_nothing(cycle):
    proc = _proc(_act("a"), _act("b", ["a"]))
    world = FakeWorld()
    done = {"a": Milestone("a", 600, {}), "b": Milestone("b", 700, {})}

    outcome = reconcile(proc, cycle, done, world)

    assert outcome.status is Status.COMPLETE
    assert outcome.advanced == []
    assert world.acted == []


def test_a_run_that_died_midway_resumes_from_what_was_recorded(cycle):
    """Crash-safety: whatever the dead run emitted is simply the next run's starting point."""
    proc = _proc(_act("a"), _act("b", ["a"]), _act("c", ["b"]))
    world = FakeWorld()

    reconcile(proc, cycle, {}, world)                       # imagine this died after "b"
    survived = {"a": Milestone("a", 600, {}), "b": Milestone("b", 700, {})}

    world2 = FakeWorld()
    outcome = reconcile(proc, cycle, survived, world2)

    assert outcome.advanced == ["c"]
    assert world2.acted == ["do.c"]


# --- voiding is terminal ----------------------------------------------------------------

def test_a_voided_cycle_is_inert(cycle):
    proc = _proc(_act("a"), _act("b", ["a"]))
    world = FakeWorld()

    outcome = reconcile(proc, cycle, {"cycle_voided": Milestone("cycle_voided", 600, {})},
                        world)

    assert outcome.status is Status.VOIDED
    assert world.acted == []
    assert world.recorded == []


def test_voiding_a_partly_done_cycle_stops_it(cycle):
    # Correction is re-running, never amending: no stage needs an "unless superseded" path.
    proc = _proc(_act("a"), _act("b", ["a"]))
    world = FakeWorld()
    state = {"a": Milestone("a", 600, {}), "cycle_voided": Milestone("cycle_voided", 650, {})}

    outcome = reconcile(proc, cycle, state, world)

    assert outcome.status is Status.VOIDED
    assert world.acted == []


# --- DAG semantics ----------------------------------------------------------------------

def test_a_waiting_branch_does_not_block_an_independent_one(cycle):
    """`after` is a list so branches are parallel — a stalled branch must not stall a peer."""
    proc = _proc(_act("a"), _await("waits", ["a"]), _act("peer", ["a"]))
    world = FakeWorld()

    outcome = reconcile(proc, cycle, {}, world)

    assert outcome.advanced == ["a", "peer"]
    assert outcome.waiting_on == ["waits"]
    assert outcome.status is Status.ADVANCED


def test_a_join_waits_for_every_branch(cycle):
    proc = _proc(_act("a"), _await("left", ["a"]), _act("right", ["a"]),
                 _act("join", ["left", "right"]))
    world = FakeWorld()

    outcome = reconcile(proc, cycle, {}, world)

    assert "join" not in outcome.advanced
    assert outcome.advanced == ["a", "right"]


def test_a_join_runs_once_every_branch_lands(cycle):
    proc = _proc(_act("a"), _await("left", ["a"]), _act("right", ["a"]),
                 _act("join", ["left", "right"]))
    world = FakeWorld(available={"left": {"evidence": "x"}})

    outcome = reconcile(proc, cycle, {}, world)

    assert outcome.status is Status.COMPLETE
    assert outcome.advanced == ["a", "left", "right", "join"]


# --- where an await starts looking ------------------------------------------------------

def test_an_await_looks_from_its_latest_predecessor(cycle):
    """Not `after[-1]`: in a DAG the list order carries no meaning, and looking from an
    earlier branch would re-examine evidence predating the stage becoming ready."""
    proc = _proc(_act("a"), _act("b", ["a"]), _await("w", ["a", "b"]))
    world = FakeWorld()

    reconcile(proc, cycle, {}, world)

    (stage, since), = world.looked
    assert stage == "w"
    assert since == 1020        # b's timestamp (the later), not a's 1010


def test_an_await_with_no_predecessor_looks_from_cycle_open(cycle):
    proc = _proc(_await("w"))
    world = FakeWorld()

    reconcile(proc, cycle, {}, world)

    assert world.looked == [("w", 500)]


# --- a stage may legitimately contribute nothing -----------------------------------------

def test_an_act_returning_an_empty_payload_still_completes(cycle):
    """A bonus invoice has no worked days, so `computed_lines` yields nothing — and nothing
    branches on that. Absence is graceful everywhere, or the definition grows conditionals."""

    class EmptyWorld(FakeWorld):
        def act(self, action, cycle, milestones):
            self.acted.append(action)
            return {}

    proc = _proc(_act("computed_lines"), _act("next", ["computed_lines"]))
    world = EmptyWorld()

    outcome = reconcile(proc, cycle, {}, world)

    assert outcome.status is Status.COMPLETE
    assert outcome.advanced == ["computed_lines", "next"]
