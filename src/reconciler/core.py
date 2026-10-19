"""Pure reconciliation core (ADR 0012).

Given a process definition, a cycle, and the milestones recorded for it so far, decide
what to do next and drive it through a `World` port. There is no in-flight state: a run is
a function of what is already recorded, which is what makes the tier crash-safe,
idempotent and re-runnable, and is why the saga problem is *avoided* here rather than
solved — nothing is suspended, so nothing needs a resume token.

**INVARIANT: this module MUST NOT import the runner, an HTTP client, or an LLM SDK.**
Deciding *what* to do is pure logic over recorded events; only *doing* it needs the world,
and that arrives through the `World` protocol below. This is the same seam
`inference.runtime.core` keeps against `quixstreams`, for the same three reasons: the tier
stays testable under CI's bare install, the runner stays swappable (only `flow.py` knows
about Prefect), and the logic stays readable without a transport in the way.
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol

from reconciler.definition import VOID_STAGE, ProcessDefinition, Stage

logger = logging.getLogger("reconciler.core")

# When an `await` is satisfied, the fact happened when the EVIDENCE happened — not when a run
# noticed it. A finder therefore reports the evidence's time under this key, and a sink stamps
# the milestone with it instead of "now".
#
# This is not tidiness; it is load-bearing. DreamHost's two payment mails arrive ~3 hours
# apart (submitted 15:17, processed 18:12 on 2026-07-15) — comfortably between two runs of a
# daily reconciler. If `payment_submitted` were stamped with the run's clock, the next stage
# would look for the processed mail from *the following morning* and never see the 18:12 one,
# stalling forever in a way indistinguishable from legitimately waiting.
#
# It is also just the house rule: event-time is the fact's time, and "when the system handled
# it" is the DB's `ingested_at`.
EVIDENCE_TIME_KEY = "matched_at"


@dataclass(frozen=True)
class Cycle:
    """One run of a process — an invoice, not a month.

    `key` is the identity (`dh_invoice_2026_004`) and rides in the *body* of every emitted
    milestone. It is deliberately NOT the entity key: Aware keys state by `user_id`, and a
    cycle key in that slot would fragment one person's state across as many buckets as they
    have ever had cycles (see `inference.runtime.core.Router.key_for`).
    """

    key: str
    process: str
    user_id: str
    opened_at: int
    context: dict = field(default_factory=dict)   # whatever `cycle_opened` carried


@dataclass(frozen=True)
class Milestone:
    """One recorded process event, as the reconciler sees it."""

    stage: str
    timestamp: int
    payload: dict = field(default_factory=dict)


class Status(str, Enum):
    ADVANCED = "advanced"     # did work and there is more to do
    WAITING = "waiting"       # nothing left that is ready; something is being awaited
    COMPLETE = "complete"     # every stage recorded
    VOIDED = "voided"         # terminal: this cycle was abandoned, a re-run supersedes it


@dataclass(frozen=True)
class Outcome:
    status: Status
    advanced: list[str] = field(default_factory=list)   # stages completed THIS run
    waiting_on: list[str] = field(default_factory=list)  # awaits that found nothing


class World(Protocol):
    """Everything the reconciler needs from outside itself — the seam that keeps `core` pure.

    A real implementation calls SMTP, createmypdf, an LLM and the ingest gateway; a test
    passes a dict-backed fake. Mirrors the `StateStore` port in `inference.runtime.core`.
    """

    def act(self, action: str, cycle: Cycle, milestones: dict[str, Milestone]) -> dict:
        """Perform `action` and return its payload. Side effects live here."""
        ...

    def find(self, signal: dict, cycle: Cycle, since: int,
             milestones: dict[str, "Milestone"]) -> dict | None:
        """Look for evidence satisfying `signal` at or after `since`; None if not yet.

        `signal` is opaque to the core — the implementation parses its own config, as
        engines parse their own `engine_config`. `milestones` is passed because evidence
        usually has to be tied back to *this* cycle, and what identifies it (a subject line,
        an upstream id) is something an earlier stage recorded.
        """
        ...

    def record(self, cycle: Cycle, stage: str, payload: dict) -> Milestone:
        """Emit the milestone (POST to the ingest gateway) and return it as recorded.

        Returns the `Milestone` rather than None so the core never needs a clock — the
        timestamp comes from whoever actually wrote the event.
        """
        ...


class MilestoneStore(Protocol):
    """Where this tier's own recorded state is read back from — cycles and their milestones.

    The read half of ADR 0012's "a run is a pure function of what is already recorded", and
    deliberately **read-only**: a milestone is written by POSTing it at the ingest gateway
    (`World.record`), never by this port, so a store can never become a second writer
    competing with the persister.

    The task sweep's half of the same idea is `tasks.OpenTaskStore`, declared beside the code
    that consumes it. Two ports rather than one because **no implementation has ever held
    both** — a process run needs cycles and milestones, a sweep needs open tasks, and nothing
    needs all of it.
    """

    def cycles(self, definition: ProcessDefinition) -> list[Cycle]:
        """Every cycle ever opened for this process, oldest genesis first.

        Completed and voided cycles included — deciding what to do with them is `reconcile`'s
        job, and it already skips both cheaply.
        """
        ...

    def milestones(self, definition: ProcessDefinition,
                   cycle: Cycle) -> dict[str, Milestone]:
        """The milestones recorded for one cycle, keyed by stage name.

        Includes the genesis event under `GENESIS_STAGE`, so a stage may depend on nothing and
        still have somewhere to look back to. Last write wins: a milestone re-emitted by a
        retry must not produce two conflicting views of one stage.
        """
        ...


class EventLog(Protocol):
    """Raw events of one name, at or after `since` — **Aware's log, not this tier's state**.

    Separate from `MilestoneStore` on purpose, and the separation is the point rather than
    tidiness: a milestone is a fact this tier recorded about itself, while a signal is a fact
    recorded by *Aware* that a process is waiting on ("you got home"). They live in different
    systems the moment this tier owns a store of its own, so a store that cannot see Aware's
    log should be unable to claim this port rather than implement it and raise.

    Consumed through `finder.NeonEvents`, which adapts it to `finder.CandidateSource`.
    """

    def signals(self, name: str, since: int) -> list[tuple[int, dict]]:
        ...


def _ready(stage: Stage, milestones: dict[str, Milestone]) -> bool:
    return all(dep in milestones for dep in stage.after)


def _since(stage: Stage, cycle: Cycle, milestones: dict[str, Milestone]) -> int:
    """The instant an `await` should start looking from.

    The *latest* predecessor, not `after[-1]`: with a DAG the list order carries no meaning,
    and looking from an earlier branch's timestamp would re-examine evidence that predates
    the stage actually becoming ready. A stage with no predecessors looks from cycle open.
    """
    if not stage.after:
        return cycle.opened_at
    return max(milestones[dep].timestamp for dep in stage.after)


def reconcile(
    definition: ProcessDefinition,
    cycle: Cycle,
    milestones: dict[str, Milestone],
    world: World,
) -> Outcome:
    """Advance `cycle` as far as it will go, then stop at the first genuine wait.

    The loop walks stages in definition order — valid as a topological walk because
    `ProcessDefinition` enforces that `after` names earlier stages — and keeps a **local**
    view of milestones that advances as it goes.

    That local advance is not bookkeeping. Without it a run performs exactly one stage,
    because the next stage's `after` is tested against a view that has not moved: a
    seven-stage process would take seven days, and the four `act` stages after approval
    (collect lines, total, render, send) would trickle out one a day instead of completing
    the moment approval lands. It is also why the emit path's latency is irrelevant — a
    milestone reaches Neon through Vector and Kafka, which this run would not see on a
    re-read. The local view is *this* run's truth; Neon is the next run's.
    """
    if VOID_STAGE in milestones:
        # Correction is re-running, never amending: a voided cycle is inert and a fresh
        # cycle supersedes it. Nothing else in the loop needs to know about voiding.
        logger.info("cycle %s is voided; skipping", cycle.key)
        return Outcome(Status.VOIDED)

    milestones = dict(milestones)          # local view — mutated as stages complete
    advanced: list[str] = []
    waiting_on: list[str] = []

    for stage in definition.stages:
        if stage.name in milestones:
            continue
        if not _ready(stage, milestones):
            # A predecessor is still unfinished. `continue`, don't stop: an independent
            # branch further down may well be ready, which is what makes `after` a DAG
            # rather than a chain.
            continue

        if stage.kind == "act":
            payload = world.act(stage.action, cycle, milestones)
        else:
            payload = world.find(stage.signal, cycle,
                                 _since(stage, cycle, milestones), milestones)
            if payload is None:
                waiting_on.append(stage.name)
                continue

        milestones[stage.name] = world.record(cycle, stage.name, payload)
        advanced.append(stage.name)
        logger.info("cycle %s advanced to %s", cycle.key, stage.name)

    outstanding = [s.name for s in definition.stages if s.name not in milestones]
    if not outstanding:
        return Outcome(Status.COMPLETE, advanced)
    if advanced:
        return Outcome(Status.ADVANCED, advanced, waiting_on)
    return Outcome(Status.WAITING, advanced, waiting_on)
