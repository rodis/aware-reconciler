"""`sweep-tasks` as a Kubernetes controller — the production driver since 2026-11-02.

Read `deploy/crds/tasksweep.yml` first; it carries the why. This module is the other half,
and it is deliberately TINY: the domain lives in `tasks.sweep`, which already takes its ports
injected and returns a plan, so the controller only decides when to run it and where to
report.

**The package is `controllers`, not `operator`.** `operator` is a stdlib module and a package
of that name shadows it on `sys.path` — a failure that shows up far from its cause.

**kopf is imported lazily**, inside the function, for the same reason `adapters/mysql.py`
imports pymysql lazily: CI runs the suite without requiring kopf, and a module-scope
`import kopf` would break collection of tests that never touch Kubernetes.

Two rules:

- **Writes follow `spec.dryRun`, and a missing field means dry-run.** The CRD defaults it to
  true and so does `plan()`, so an object that does not say `dryRun: false` observes only.
  There must be exactly ONE producer of `email_task_closed` (the duplicate-close bug fixed on
  2026-10-18): the production object turned writes on in the same change that removed the
  Argo `sweep-tasks` CronWorkflow from `inference`.
- **It never reads `.status` to decide.** The timer is unconditional. The tempting
  optimisation — `when=lambda status: status.get('phase') != 'InSync'` — would make a wrong
  status permanent, which is the silent stall this tier exists to avoid. Reconciling an
  already-in-sync sweep costs one Gmail query and one MySQL read, and the function is pure, so
  a wasted pass changes nothing.
"""

from __future__ import annotations

import datetime
import logging

from reconciler import app
from reconciler.app import RunOptions

log = logging.getLogger(__name__)

GROUP, VERSION, PLURAL = "aware.rods.me", "v1", "tasksweeps"


def plan(spec: dict) -> tuple[int, int]:
    """One sweep. Two reads, and — only when `spec.dryRun` is false — the writes that repair the
    drift (`tasks.sweep` emits them itself).

    Returns `(to_open, to_close)`: what was found, and therefore what was written unless dry. This is the whole domain contribution of the controller, and
    the fact that it is one call is the point: `tasks.sweep` already takes its ports injected
    and RETURNS a plan rather than performing it (`SweepPlan` — "returned rather than
    performed, so it can be printed"). The CLI prints it; a controller puts it in `.status`.
    Neither owns the logic, which is what makes this a fair comparison rather than a rewrite.

    FINDING (2026-10-21): the status would be more useful with `labelled` and `open` — "4
    labelled in gmail, 4 open in aware" is what the log line says and what you want in
    `kubectl get`. `SweepPlan` does not carry them; it only carries the difference. Getting
    them means either a second pair of reads here (duplicating what `sweep` just did) or
    widening `SweepPlan` by two fields. The latter is right and is a one-line change to the
    core — recorded rather than done, because a spike that edits the thing it is measuring
    stops measuring it.
    """
    sweep = app.sweep_tasks(
        label=spec["label"],
        user=spec["userId"],
        lookback_days=spec.get("lookbackDays", 30),
        # `True` when absent, matching the CRD default: writing must be asked for explicitly.
        options=RunOptions(dry_run=spec.get("dryRun", True)),
    )
    return len(sweep.to_open), len(sweep.to_close)


def _now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds")


def _status(to_open: int, to_close: int, *, dry_run: bool = True) -> dict:
    """`Drifted` means this pass FOUND drift. With writes on it also repaired it, and the
    message says which — so a pass that wrote reads differently from one that only looked."""
    drifted = bool(to_open or to_close)
    if not drifted:
        message = "the label and the event log agree"
    elif dry_run:
        message = f"{to_open} to open, {to_close} to close (dry run, nothing written)"
    else:
        message = f"opened {to_open}, closed {to_close}"
    return {
        "phase": "Drifted" if drifted else "InSync",
        "lastSweep": _now(),
        "toOpen": to_open,
        "toClose": to_close,
        "message": message,
    }


def _failed(exc: Exception) -> dict:
    """A failure is a status WITH A TIMESTAMP, found by running it (2026-10-21).

    The first version wrote only phase and message, and `kubectl get` rendered an empty `LAST`
    column — so "failed an hour ago" and "failing right now" looked identical. Stamping every
    outcome is what makes the age of the last attempt readable, which is the whole reason the
    column exists.
    """
    return {"phase": "Failed", "lastSweep": _now(), "message": str(exc)[:200]}


def register() -> None:
    """Attach the handler. Called by the operator entrypoint, never at import."""
    import kopf                                  # lazy — see the module docstring

    # FINDING (2026-10-21): kopf's `interval` is fixed when the handler is DECORATED, so
    # `spec.intervalSeconds` cannot drive it per resource. The honest options are one handler
    # per cadence, a short tick that checks elapsed time itself, or accepting one interval for
    # every TaskSweep. Minor here (there would be one object), but it is a real way the
    # platform is less flexible than a CronWorkflow per schedule, and it belongs in the
    # comparison rather than in a footnote.
    # `id=` is not cosmetic: kopf writes handler ids into the object's progress annotations,
    # and the default for a nested function is `register.<locals>.reconcile`. That ties stored
    # state to the shape of the Python, so renaming or un-nesting the function orphans the
    # progress it already wrote. Name it once, explicitly.
    @kopf.timer(GROUP, VERSION, PLURAL, id="sweep", interval=300, initial_delay=10)
    def reconcile(spec, patch, name, **_):
        try:
            to_open, to_close = plan(spec)
        except Exception as exc:                 # noqa: BLE001 — a failed pass is a status, not a crash
            # A controller that dies on a bad pass stops reconciling everything else it owns.
            # Record and let the next timer try: the sweep is a pure function, so there is no
            # partial state to unwind.
            log.warning("sweep %s failed: %s", name, exc)
            patch.status.update(_failed(exc))
            return
        patch.status.update(_status(to_open, to_close, dry_run=spec.get("dryRun", True)))
