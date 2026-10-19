"""Recording a fact in the store, then telling Aware about it.

The seam that implements one decision: **the store of record is MySQL; the gateway POST is
observation, and it is best-effort** — the reconciler is a producer like any other, and a
producer whose event is dropped has lost an event, not corrupted a process.

That ordering is not arbitrary. Each fact has to land in two places, and whichever goes second
can fail:

- **Store first, POST second** (what this does): a lost POST costs a gap in the Aware timeline
  and one row missing from a rebuild. The process stays correct, because the reconciler reads
  the store and the store has it.
- **POST first, store second**: a lost write means the next run cannot see the stage, so it
  *redoes* it — re-rendering an invoice, re-sending it. ADR 0012's "safe to run twice" holds
  precisely because recorded milestones gate the acts; a record that did not land removes the
  gate.

A visible gap beats a second invoice email, so the store goes first.

**No retry on the POST, deliberately** — the same conclusion `adapters/ingest.py` already
reached, now load-bearing for a second reason. A retry with a fresh `message.id` duplicates the
row when the first POST actually landed and only its response was lost. A retry with a *stable*
id (Vector honours a producer-supplied one — `enrich_sensor.yml` mints only `if !exists`)
collides with the `events.id` primary key, and the Postgres sink has no `ON CONFLICT`, so a
poisoned batch retried forever would stall ingest. One POST, logged loudly if it fails.

**The cost, stated plainly:** Neon is no longer guaranteed to be a complete copy, which weakens
"the backfill is the restore path". A rebuild would be missing exactly the milestones whose
POST was lost. The repair, if it is ever wanted, is a daily job that reads Neon once, diffs
against the store, and re-POSTs only what is genuinely absent — one Neon wake a day, and this
tier's own idiom. Not built; noted so the trade is a decision rather than a discovery.
"""

import logging
from datetime import UTC, datetime

from reconciler.adapters.gateway import event_time, milestone_body
from reconciler.core import Cycle, Milestone
from reconciler.definition import GENESIS_STAGE, ProcessDefinition
from reconciler.tasks import OPENED_EVENT

logger = logging.getLogger("reconciler.recording")


class StoreBackedMilestones:
    """Writes a milestone to the store, then POSTs it at the gateway.

    Wraps a sink rather than replacing it, so `--dry-run` keeps working unchanged: the CLI
    hands it `DryRunMilestones` and a dry-run writer, and nothing downstream knows.
    """

    def __init__(self, definition: ProcessDefinition, writer, downstream):
        self._definition = definition
        self._writer = writer
        self._downstream = downstream

    def record(self, cycle: Cycle, stage: str, payload: dict) -> Milestone:
        # Computed ONCE and used for both writes. Letting each side call its own clock would
        # put a one-second disagreement between the store and the timeline for no reason, and
        # a satisfied `await` carries its evidence's time rather than now (EVIDENCE_TIME_KEY),
        # which is exactly the kind of value that must not be recomputed.
        now = event_time(payload, int(datetime.now(UTC).timestamp()))
        body = milestone_body(self._definition, cycle, stage, payload, now)

        # The store first, and unguarded: if this raises, the run fails and the act is retried
        # by Argo. That is the correct failure — nothing was recorded, so nothing is skipped.
        if stage == GENESIS_STAGE:
            self._writer.put_cycle(cycle)
        else:
            self._writer.put_milestone(cycle, stage, now, body)

        self._emit(lambda: self._downstream.record(cycle, stage, payload, now=now),
                   what=body.get("event_name", stage), key=cycle.key)
        return Milestone(stage=stage, timestamp=now, payload=body)

    @staticmethod
    def _emit(send, *, what: str, key: str) -> None:
        try:
            send()
        except Exception:
            # Swallowed on purpose, and loudly. The fact IS recorded; what failed is telling
            # Aware about it. Raising here would fail a run that actually succeeded, and the
            # next run — reading the store — would skip the stage anyway, so the exception
            # would achieve nothing except a red workflow.
            logger.error("recorded %s for %s but the gateway POST failed — Aware will not "
                         "show it and a rebuild from the event log would miss it", what, key,
                         exc_info=True)


class StoreBackedEvents(StoreBackedMilestones):
    """The same contract for the task sweep, whose sink is an `EventSink`.

    Inherits only `_emit`; the sweep hands over a finished wire body rather than a cycle and a
    stage, so there is nothing else in common.
    """

    def __init__(self, writer, downstream):
        self._writer = writer
        self._downstream = downstream

    def emit(self, payload: dict) -> None:
        if payload.get("event_name") == OPENED_EVENT:
            self._writer.open_task(payload)
        else:
            self._writer.close_task(payload)
        self._emit(lambda: self._downstream.emit(payload),
                   what=payload.get("event_name", "?"), key=payload.get("upstream_id", "?"))


class DryRunWriter:
    """Writes nothing; keeps what it was asked to write.

    The store half of `--dry-run`. Without it a dry run would populate the store while
    pretending to send nothing, which is the most misleading possible version of a preview.
    """

    def __init__(self):
        self.written: list[tuple[str, str]] = []

    def put_cycle(self, cycle) -> None:
        self.written.append(("cycle", cycle.key))

    def put_milestone(self, cycle, stage, occurred_at, body) -> None:
        self.written.append(("milestone", f"{cycle.key}/{stage}"))

    def open_task(self, body) -> None:
        self.written.append(("task_open", body.get("upstream_id", "?")))

    def close_task(self, body) -> None:
        self.written.append(("task_close", body.get("upstream_id", "?")))
