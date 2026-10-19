"""The Prefect entry point (ADR 0012).

**The only file in the tier that imports Prefect**, and the reason that matters is the runner
question is unsettled by design: Prefect Cloud's Managed pool was chosen because its free tier
*executes* code rather than only scheduling it, so nothing lands on the cluster — but if that
changes, the swap is this file. Everything below it (`app`, `core`, the adapters) stays put,
which is also why the tier's tests need no Prefect installed.

Two deployments, because a cycle is opened by an event and advanced by a loop, and conflating
them would make the loop a scheduler:

- `open_cycle_flow`  — fires on the process's own `opens:` cron. Creates one cycle. Nothing else.
- `advance_flow`     — fires often. Advances whatever exists. Creates nothing, ever.

That split is what makes a missed run harmless: `advance` is idempotent by construction (it is
a pure function of recorded milestones, so re-running it re-derives the same frontier), and a
missed `open` is visible as a month with no cycle rather than as a silently skipped invoice.

Deployed from `prefect.yaml` at the repo root.
"""

import logging
import os
import pathlib
import sys
from datetime import UTC, datetime

from prefect import flow

# Put `src/` on the path before importing the package this module belongs to.
#
# Necessary because the Managed pool runs this file **as an entrypoint from a git clone**, with
# `reconciler` neither pip-installed nor importable: Prefect resolves
# `src/reconciler/flow.py:advance_flow` by loading the file directly, so `from reconciler import
# app` runs with only Prefect's own site-packages on `sys.path`.
#
# `PYTHONPATH` was the obvious lever and does NOT work — verified 2026-08-16 against the live
# pool, with both a relative `src` and the absolute clone path (`/opt/prefect/inference-main/src`).
# Both crashed identically with `ModuleNotFoundError: No module named 'reconciler'`, so
# `job_variables.env` is not reaching the process that loads the flow. Bootstrapping here needs
# no environment cooperation at all, and is correct in every context — an installed package
# finds itself first and this is a no-op.
_SRC = str(pathlib.Path(__file__).resolve().parents[1])
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from reconciler import app  # noqa: E402
from reconciler.adapters import doppler  # noqa: E402  — must follow the sys.path bootstrap above

logger = logging.getLogger("reconciler.flow")


# The Prefect Secret block holding the read-only Doppler service token. Named, not derived,
# because it is the one piece of configuration that cannot itself come from Doppler.
DOPPLER_TOKEN_BLOCK = "doppler-token"


def _load_config() -> None:
    """Populate `os.environ` from Doppler, for anything not already set.

    `setdefault` semantics, and the order matters: a local run keeps using `workers/.env`
    untouched, while a scheduled run starts with an almost-empty environment and takes
    everything from Doppler. Same code path either way, so there is no "works locally" gap.

    Getting the *token* is the only Prefect-specific step — it comes from a Secret block, since
    it is the one credential that cannot be stored in the thing it unlocks. Everything after
    that is `adapters.doppler`, which knows nothing about any runner.

    A missing token warns rather than raises: `app.py` already raises a `ConfigurationError`
    naming the exact variable a stage needed, which is a better message than a generic
    block-not-found. A token that exists but *fails* does raise — an unreadable secret store is
    a real fault, and pretending otherwise would let a run proceed half-configured.
    """
    token = os.environ.get("DOPPLER_TOKEN")
    if not token:
        try:
            from prefect.blocks.system import Secret

            token = Secret.load(DOPPLER_TOKEN_BLOCK).get()
        except Exception as e:      # noqa: BLE001 — no block, or no Prefect context
            logger.warning("no DOPPLER_TOKEN and no %r Secret block (%s); relying on whatever "
                           "is already in the environment", DOPPLER_TOKEN_BLOCK, e)
            return

    for name, value in doppler.fetch(token).items():
        os.environ.setdefault(name, value)


def _wire_logging() -> None:
    """Let the tier's narration reach the Prefect run log.

    It is worth the two lines: which candidate matched, what the classifier read, and why a
    stage stopped are the whole diagnostic story, and without them a run that *decided* nothing
    had happened looks identical to one that never looked.

    Delivery is Prefect's own `PREFECT_LOGGING_EXTRA_LOGGERS=reconciler` (set on the deployment
    in `prefect.yaml`) — an earlier version forwarded records by hand through `get_run_logger`,
    which worked but emitted everything twice, once through the bridge and once through the
    handler Prefect had already attached. All that is actually missing is the level: the
    `reconciler` logger is never configured by this package (a library must not call
    `basicConfig`), so it inherits WARNING and its INFO records die before any handler runs.
    """
    logging.getLogger("reconciler").setLevel(logging.INFO)


@flow(name="open-process-cycle")
def open_cycle_flow(process: str, seq: int | None = None, period: dict | None = None,
                    year: int | None = None, user: str | None = None,
                    use_previous_month: bool = True, dry_run: bool = False) -> str:
    """Open one cycle. Returns its key.

    `seq` is omitted by the schedule and resolved from the cycles already recorded — see
    `app.next_sequence`. A human opening one by hand passes it, because the number is often the
    thing they care about.

    `use_previous_month` fills the worked period from the calendar month that just ended, which
    is what a cron firing on the 1st means. A bonus invoice — no worked days at all — is opened
    manually with it off.
    """
    _wire_logging()
    _load_config()
    if period is None and use_previous_month:
        period = app.previous_month(datetime.now(UTC).date())

    cycle = app.open_cycle(process, seq=seq, period=period, year=year, user=user,
                           options=app.RunOptions(dry_run=dry_run))
    return cycle.key


@flow(name="advance-process-cycles")
def advance_flow(process: str, cycle_key: str | None = None, dry_run: bool = False) -> dict:
    """Advance every cycle of a process as far as it will go.

    Returns a per-cycle summary rather than nothing, so the Prefect UI's result view answers
    "what moved?" without opening the logs.

    **Does not fail when a cycle stops.** A stage waiting on a human, or on machinery that does
    not exist yet, is the normal resting state of a long-running process — failing the run
    would turn every ordinary Tuesday into an alert, and then nobody would read the alerts. A
    genuine fault (an unreachable relay, a bad credential) still raises out of `app` and fails
    the run, which is the distinction worth alerting on.
    """
    _wire_logging()
    _load_config()
    results = app.advance(process, cycle_key=cycle_key,
                          options=app.RunOptions(dry_run=dry_run))

    summary = {}
    for cycle, outcome in results:
        summary[cycle.key] = {
            "status": outcome.status.value if outcome else "stopped",
            "advanced": list(outcome.advanced) if outcome else [],
            "waiting_on": list(outcome.waiting_on) if outcome else [],
        }
    logger.info("advanced %d cycle(s) of %s", len(summary), process)
    return summary


@flow(name="open-then-advance")
def open_and_advance_flow(process: str, seq: int | None = None, period: dict | None = None,
                          year: int | None = None, user: str | None = None,
                          use_previous_month: bool = True, dry_run: bool = False) -> dict:
    """Convenience for a manual run: open a cycle and immediately walk it.

    Not scheduled. It exists because opening a cycle and then waiting an hour to see whether
    the approval mail rendered correctly is a poor feedback loop for the *first* cycle of a new
    process — which is exactly when you most want to look.

    **Every parameter is spelled out, and `**kwargs` is banned here.** Prefect derives a
    deployment's parameter schema from the signature, and it renders `**kwargs` as a property
    named `kwargs` that is *required* — so the first attempt to run this returned
    `Validation failed. Failure reason: 'kwargs' is a required property` and no run was created
    at all. The deployment was unusable from the moment it was created (found 2026-09-03, by
    trying to open August's cycle with it). `tests/test_reconciler_app.py` now fails on any
    deployed entrypoint that takes `**kwargs`.

    **It walks the cycle it just made, rather than looking it back up.** Composing the two
    flows instead — open, then `advance_flow` — was the second bug in this function, and the
    more insidious one: `advance` reads cycles from Neon, but a milestone reaches Neon
    asynchronously (gateway -> Kafka -> Vector -> persister). So the lookup raced the write and
    failed with `no cycle 'dh_invoice_2026_010'`, and under `dry_run` it could never succeed at
    all, because a dry open writes nothing to look up. `app.walk_fresh` takes the Cycle object
    and an empty milestone set, which is exactly right for something opened one line ago.
    """
    _wire_logging()
    _load_config()
    if period is None and use_previous_month:
        period = app.previous_month(datetime.now(UTC).date())

    options = app.RunOptions(dry_run=dry_run)
    cycle = app.open_cycle(process, seq=seq, period=period, year=year, user=user,
                           options=options)
    outcome = app.walk_fresh(process, cycle, options=options)
    return {cycle.key: {
        "status": outcome.status.value if outcome else "stopped",
        "advanced": list(outcome.advanced) if outcome else [],
        "waiting_on": list(outcome.waiting_on) if outcome else [],
    }}


@flow(name="sweep-email-tasks")
def sweep_tasks_flow(label: str = "aware/todo", user: str | None = None,
                     lookback_days: int = 365, dry_run: bool = False) -> dict:
    """Reconcile the Gmail todo label against the recorded task events.

    The third deployment, and the first that is not a *process*. It belongs here anyway: it is
    the same idea the tier is built on — a scheduled job that is a pure function of what has
    been recorded, and therefore safe to re-run — applied to something with two states instead
    of eleven stages. Modelling a todo as a process would mint a cycle per email.

    Returns counts rather than nothing so the Prefect UI answers "did anything move?" without
    opening the logs, exactly as `advance_flow` does.

    **Never fails on a quiet sweep.** Most hours the label and the event log agree, and that is
    the normal resting state, not an error. A genuine fault — an unreachable relay, a bad
    token — still raises out of `app` and fails the run.
    """
    _wire_logging()
    _load_config()
    plan = app.sweep_tasks(label=label, user=user, lookback_days=lookback_days,
                           options=app.RunOptions(dry_run=dry_run))
    summary = {
        "opened": [c.get("subject", "")[:80] for c in plan.to_open],
        "closed": [t.subject[:80] for t in plan.to_close],
    }
    logger.info("sweep: opened %d, closed %d", len(plan.to_open), len(plan.to_close))
    return summary

if __name__ == "__main__":
    # Local smoke run against the ephemeral server Prefect starts on its own — proves the
    # deployment's entrypoint resolves and the flow executes, without touching Prefect Cloud.
    import sys

    print(advance_flow(sys.argv[1] if len(sys.argv) > 1 else "dreamhost_invoice",
                       dry_run=True))
