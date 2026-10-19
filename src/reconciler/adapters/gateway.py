"""Emitting milestones through the existing ingest gateway (ADR 0012).

The reconciler is a producer like any other: it POSTs to `/sensors/<app>` and Vector does the
rest — mint `message.id`, wrap, produce to `raw_sensors`, persist to Neon. That is normative
rule 1 in practice ("the reconciler acts; Aware observes"): no new topic, no new transform, no
new component, and the milestone is a raw row in Neon exactly like a car door opening.

The contract, from `shape_sensor.yml`:

    POST /sensors/<app>   {"payload": {"event_name": ..., "user_id": ..., ...}}

`event_name` is renamed to `name`; `user_id` is required and events without one are dropped
(with a Vector error log, *not* a 4xx — the HTTP source has already answered 200). So the
absence of an error here does not prove the event landed.
"""

import json
import logging
import urllib.error
import urllib.request
from datetime import UTC, datetime

from reconciler.core import EVIDENCE_TIME_KEY, Cycle, Milestone
from reconciler.definition import ProcessDefinition

logger = logging.getLogger("reconciler.adapters.gateway")

DEFAULT_APP = "process"


def event_time(payload: dict, now: int) -> int:
    """When the milestone's fact actually happened.

    For an `act` that is now — the reconciler did it. For a satisfied `await` it is the
    evidence's own time, which a finder reports under `EVIDENCE_TIME_KEY`. See the comment on
    that constant: with a daily run and two mails hours apart, stamping the run's clock makes
    the next stage look past evidence that already arrived.
    """
    matched = payload.get(EVIDENCE_TIME_KEY)
    return int(matched) if isinstance(matched, int | float) else now


def milestone_body(definition: ProcessDefinition, cycle: Cycle, stage: str,
                   payload: dict, now: int) -> dict:
    """The `payload` object a milestone is POSTed as — the tier's whole wire contract.

    Extracted so it can be asserted directly: everything that makes a milestone routable,
    attributable and non-colliding is decided here.
    """
    return {
        # Prefixed by `event_name`, so a milestone can never collide with a definition's
        # input_event_names() and be routed into an engine not expecting it.
        "event_name": definition.event_name(stage),
        # Required by shape_sensor; also Aware's entity key. The cycle NEVER goes here.
        "user_id": cycle.user_id,
        # Event time is ours to state; the DB stamps ingested_at separately.
        "timestamp": now,
        "process": cycle.process,
        "cycle_key": cycle.key,      # identity rides in the BODY, never in user_id
        **payload,
    }


class GatewayMilestones:
    """Records milestones by POSTing them at Vector.

    stdlib `urllib` rather than a client library: this is one POST with a timeout, and
    keeping it dependency-free is what lets the whole tier install with `--no-deps`.
    """

    def __init__(self, base_url: str, definition: ProcessDefinition,
                 app: str = DEFAULT_APP, timeout: float = 10.0):
        self._url = f"{base_url.rstrip('/')}/sensors/{app}"
        self._definition = definition
        self._timeout = timeout

    def record(self, cycle: Cycle, stage: str, payload: dict,
               *, now: int | None = None) -> Milestone:
        # `now` is passed in when a store already recorded this fact (see `recording.py`), so
        # the timeline and the store agree to the second rather than each calling the clock.
        now = now if now is not None else event_time(payload,
                                                     int(datetime.now(UTC).timestamp()))
        body = milestone_body(self._definition, cycle, stage, payload, now)
        request = urllib.request.Request(
            self._url,
            data=json.dumps({"payload": body}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                status = response.status
        except urllib.error.URLError as e:
            raise RuntimeError(f"could not record {stage} for {cycle.key}: {e}") from e

        logger.info("recorded %s for %s (HTTP %s)",
                    body["event_name"], cycle.key, status)
        return Milestone(stage=stage, timestamp=now, payload=body)


class DryRunMilestones:
    """Records nothing; keeps what it was asked to record.

    Not a test double — the CLI's `--dry-run` uses it so a cycle can be walked end to end,
    and the mail it would send rendered, without writing a single event into history. A
    process's first run is exactly when you want to look before committing.
    """

    def __init__(self, definition: ProcessDefinition):
        self._definition = definition
        self.written: list[tuple[str, dict]] = []

    def record(self, cycle: Cycle, stage: str, payload: dict,
               *, now: int | None = None) -> Milestone:
        # `now` is passed in when a store already recorded this fact (see `recording.py`), so
        # the timeline and the store agree to the second rather than each calling the clock.
        now = now if now is not None else event_time(payload,
                                                     int(datetime.now(UTC).timestamp()))
        self.written.append((self._definition.event_name(stage), payload))
        logger.info("[dry-run] would record %s for %s",
                    self._definition.event_name(stage), cycle.key)
        return Milestone(stage=stage, timestamp=now, payload=payload)
