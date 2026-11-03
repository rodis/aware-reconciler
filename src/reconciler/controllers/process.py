"""`Process` — open and advance every cycle of one `processes/*.yml` definition.

The generic engine that `advance-cycles` (Argo) and Prefect's opener ran, as one controller. The
process itself — the invoice — is data: `spec.definition` names the YAML, so a second process is a
second `Process` object, never a second controller.

Same two rules as the other controllers:

- **Writes follow `spec.dryRun`, and a missing field means dry-run.** A process sends mail and
  invoices, so an object that does not say `dryRun: false` only reports what it would open and do.
- **It never reads `.status` to decide.** Opening is idempotent by worked period against the store
  (`reconciler.schedule`) and advancing is a pure function of recorded milestones, so a missed
  pass is caught by the next and a repeated pass changes nothing.

**Hourly**, as `advance-cycles` was: every stage this waits on moves on human timescales, and the
scheduled open (09:00 on the 1st) lands within the hour after it is due.
"""

from __future__ import annotations

import datetime
import logging

from reconciler import app
from reconciler.app import RunOptions
from reconciler.core import Status

log = logging.getLogger(__name__)

GROUP, VERSION, PLURAL = "aware.rods.me", "v1", "processes"
INTERVAL_SECONDS = 3600


def plan(spec: dict):
    """One pass. Opens what the schedule owes and advances everything, unless dry-run."""
    return app.run_process(
        spec["definition"],
        tz=spec.get("timezone", "Europe/Zurich"),
        open_scheduled=spec.get("openScheduled", True),
        # `True` when absent, matching the CRD default: acting must be asked for explicitly.
        options=RunOptions(dry_run=spec.get("dryRun", True)),
    )


def _now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds")


def _cycle(cycle, outcome) -> dict:
    if outcome is None:
        return {"key": cycle.key, "status": "stopped",
                "message": "reached machinery that is not built yet"}
    entry = {"key": cycle.key, "status": outcome.status.value}
    if outcome.advanced:
        entry["advanced"] = list(outcome.advanced)
    if outcome.waiting_on:
        entry["waitingOn"] = list(outcome.waiting_on)
    return entry


def _status(result, *, dry_run: bool = True) -> dict:
    live = [(c, o) for c, o in result.results if o is None or o.status != Status.COMPLETE]
    complete = len(result.results) - len(live)
    status: dict = {
        "lastPass": _now(),
        "cycles": [_cycle(c, o) for c, o in live],
        "complete": complete,
    }
    if result.opened:
        status["open"] = {"key": result.opened.key,
                          "period": (result.opened.context or {}).get("worked_period")}
    advanced = any(o is not None and o.advanced for _, o in live)
    waiting = [c.key for c, o in live if o is not None and o.status == Status.WAITING]

    if result.opened:
        verb = "would open" if dry_run else "opened"
        period = status["open"]["period"] or {}
        status["phase"] = "Opening" if dry_run else "Opened"
        status["message"] = f"{verb} {result.opened.key} for {period.get('start')}..{period.get('end')}"
    elif advanced:
        status["phase"] = "Advanced"
        status["message"] = ("would advance " if dry_run else "advanced ") + ", ".join(
            f"{c.key} ({', '.join(o.advanced)})" for c, o in live if o is not None and o.advanced)
    elif waiting:
        status["phase"] = "Waiting"
        status["message"] = "waiting: " + ", ".join(
            f"{c.key} on {', '.join(o.waiting_on) or '?'}" for c, o in live
            if o is not None and o.status == Status.WAITING)
    else:
        status["phase"] = "Idle"
        status["message"] = f"nothing open; {complete} complete"
    if dry_run:
        status["message"] += " (dry run, nothing written)"
    return status


def _failed(exc: Exception) -> dict:
    return {"phase": "Failed", "lastPass": _now(), "message": str(exc)[:200]}


def register() -> None:
    """Attach the handler. Called by the operator entrypoint, never at import."""
    import kopf                                  # lazy — see task_sweep's docstring

    @kopf.timer(GROUP, VERSION, PLURAL, id="process", interval=INTERVAL_SECONDS, initial_delay=60)
    def reconcile(spec, patch, name, **_):
        try:
            result = plan(spec)
        except Exception as exc:                 # noqa: BLE001 — a failed pass is a status, not a crash
            log.warning("process %s failed: %s", name, exc)
            patch.status.update(_failed(exc))
            return
        patch.status.update(_status(result, dry_run=spec.get("dryRun", True)))
