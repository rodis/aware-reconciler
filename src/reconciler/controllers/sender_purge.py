"""`SenderPurge` — trash mail from listed senders once it has had its time.

Read `deploy/crds/senderpurge.yml` and `reconciler/purge.py` first. This module is only the
driver, and the same two rules as `task_sweep` hold:

- **Writes follow `spec.dryRun`, and a missing field means dry-run.** This controller destroys
  mail (to the Bin, recoverable for 30 days, but still), so an object that does not say
  `dryRun: false` only reports what it would trash.
- **It never reads `.status` to decide.** The cutoff is a pure function of the clock, so there is
  no "last run" to remember: the timer runs every 10 minutes, a missed pass is caught by the
  next, and a repeated pass finds nothing left to trash. That is what let a requirement first
  phrased as "every day at 7 am" run on a controller framework with no cron at all.
"""

from __future__ import annotations

import datetime
import logging

from reconciler import app
from reconciler.app import RunOptions

log = logging.getLogger(__name__)

GROUP, VERSION, PLURAL = "aware.rods.me", "v1", "senderpurges"
INTERVAL_SECONDS = 600


def plan(spec: dict):
    """One pass. Searches, guards, and trashes unless `spec.dryRun` is not false."""
    return app.purge_senders(
        senders=spec["senders"],
        tz=spec.get("timezone", "Europe/Zurich"),
        min_age_hours=spec.get("minAgeHours", 12),
        max_per_run=spec.get("maxPerRun", 100),
        # `True` when absent, matching the CRD default: trashing must be asked for explicitly.
        options=RunOptions(dry_run=spec.get("dryRun", True)),
    )


def _now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds")


def _status(result, *, dry_run: bool = True) -> dict:
    """`Clean` when nothing is due; otherwise `Pending` (dry run) or `Purged` (trashed)."""
    due = len(result.to_trash)
    more = " (more remain; the next pass continues)" if result.truncated else ""
    if not due:
        phase, message = "Clean", "nothing older than the cutoff"
    elif dry_run:
        phase, message = "Pending", f"would trash {due}{more} (dry run, nothing written)"
    else:
        phase, message = "Purged", f"trashed {result.trashed}{more}"
    if result.refused:
        message += f"; refused {len(result.refused)} the search matched but the rule does not"
    return {
        "phase": phase,
        "lastPass": _now(),
        "cutoff": result.cutoff.isoformat(timespec="seconds"),
        "due": due,
        "trashed": result.trashed,
        "refused": len(result.refused),
        "message": message,
    }


def _failed(exc: Exception) -> dict:
    return {"phase": "Failed", "lastPass": _now(), "message": str(exc)[:200]}


def register() -> None:
    """Attach the handler. Called by the operator entrypoint, never at import."""
    import kopf                                  # lazy — see task_sweep's docstring

    @kopf.timer(GROUP, VERSION, PLURAL, id="purge", interval=INTERVAL_SECONDS, initial_delay=30)
    def reconcile(spec, patch, name, **_):
        try:
            result = plan(spec)
        except Exception as exc:                 # noqa: BLE001 — a failed pass is a status, not a crash
            log.warning("purge %s failed: %s", name, exc)
            patch.status.update(_failed(exc))
            return
        patch.status.update(_status(result, dry_run=spec.get("dryRun", True)))
