"""Command-line entry point for the reconciler (ADR 0012).

Deliberately built before the Prefect entry point, and still the one to reach for. A reconciler
run is a plain function of recorded events, so it must be runnable by hand — that is what makes
the first cycle of a new process reviewable, and what keeps the runner swappable.

The wiring lives in `app.py`; this is argparse and printing. `flow.py` is the same shape for
Prefect, and neither knows the other exists.

    # See exactly what July's approval mail would say — writes nothing, sends nothing
    python -m reconciler.run open --process dreamhost_invoice --seq 7 \\
        --period 2026-07-01:2026-07-31 --dry-run

    # Open the cycle for real, then advance it
    python -m reconciler.run open --process dreamhost_invoice --seq 7 \\
        --period 2026-07-01:2026-07-31
    python -m reconciler.run reconcile --process dreamhost_invoice

    # See which tasks the Gmail label and the event log disagree about — writes nothing
    python -m reconciler.run sweep-tasks --dry-run
"""

import argparse
import logging
import os
import pathlib
import sys
from datetime import date

from reconciler.app import (
    DEFAULT_PROCESSES_DIR,
    ConfigurationError,
    RunOptions,
    advance,
    open_cycle,
    sweep_tasks,
    walk_fresh,
)
from reconciler.tasks import DEFAULT_LABEL, DEFAULT_LOOKBACK_DAYS

logger = logging.getLogger("reconciler.run")


def _options(args) -> RunOptions:
    return RunOptions(dry_run=args.dry_run, mail_to_file=args.mail_to_file,
                      processes_dir=args.processes_dir)


def _period(raw: str | None) -> dict | None:
    if not raw:
        return None
    start, _, end = raw.partition(":")
    date.fromisoformat(start), date.fromisoformat(end)      # validate, fail loudly
    return {"start": start, "end": end}


def cmd_open(args) -> int:
    options = _options(args)
    cycle = open_cycle(args.process, seq=args.seq, period=_period(args.period),
                       year=args.year, user=args.user, options=options)

    if args.dry_run:
        # A dry-run open is only useful if it also shows what would happen next, so walk the
        # cycle immediately against an empty milestone set — which also keeps the preview
        # database-free, since a cycle opened one line ago has nothing recorded to read.
        _report([(cycle, walk_fresh(args.process, cycle, options=options))])
        print(f"\ndry run — nothing written, nothing sent. Cycle would be {cycle.key}.")
        return 0

    print(f"opened cycle {cycle.key}; run `reconcile` to advance it")
    return 0


def cmd_reconcile(args) -> int:
    results = advance(args.process, cycle_key=args.cycle, options=_options(args))
    if not results:
        print(f"no cycles open for {args.process}")
        return 0
    _report(results)
    return 0


def cmd_sweep_tasks(args) -> int:
    """Reconcile the Gmail todo label against the recorded task events."""
    plan = sweep_tasks(label=args.label, user=args.user, lookback_days=args.lookback,
                       options=_options(args))
    verb = "would open" if args.dry_run else "opened"
    for candidate in plan.to_open:
        print(f"  {verb}: {candidate.get('from_name') or candidate.get('from', '?')} "
              f"— {candidate.get('subject', '')[:70]}")
    verb = "would close" if args.dry_run else "closed"
    for task in plan.to_close:
        print(f"  {verb}: {task.from_name or '?'} — {task.subject[:70]}")
    if plan.empty:
        print("nothing to do — the label and the event log agree")
    elif args.dry_run:
        print("\ndry run — nothing written.")
    return 0


def _report(results) -> None:
    for cycle, outcome in results:
        if outcome is None:
            print(f"\n{cycle.key}: stopped — this is as far as the tier reaches today")
            continue
        print(f"\n{cycle.key}: {outcome.status.value}")
        if outcome.advanced:
            print(f"  advanced: {', '.join(outcome.advanced)}")
        if outcome.waiting_on:
            print(f"  waiting on: {', '.join(outcome.waiting_on)}")


def _load_config_from_doppler() -> None:
    """Populate `os.environ` from Doppler when a token is present, for anything not already set.

    A CONVENIENCE FOR AD-HOC RUNS, and worth being precise that it is not load-bearing for
    either deployed runner. Prefect fetches its own config in `flow.py` (which never calls
    through here), and the Argo Workflows pods get theirs from the Doppler *operator*, which
    syncs the tier's `reconciler` config into a Secret the workflow container takes via
    `envFrom` — the Kubernetes-native path, and one that needs no bootstrap credential and no
    run-time call to Doppler at all.

    What it buys is that `export DOPPLER_TOKEN=... && python -m reconciler.run ...` works on any
    box without maintaining a `workers/.env`, and that a future runner which simply shells the
    CLI needs no code. Fifteen lines for that seems fair; delete it if it is still unused when
    someone next reads this.

    `setdefault`, and after `load_dotenv`, so precedence reads local-first: a developer's
    `workers/.env` still wins and a local run never silently talks to production credentials it
    did not ask for. With no token this is a no-op, which is what keeps `--dry-run` on a laptop
    working exactly as before.

    A token that FAILS raises, unlike `flow.py`'s equivalent — there the token is optional
    because a local Prefect run may legitimately have no block, whereas here the token's mere
    presence is the caller saying "this is where config comes from". An unreadable secret store
    is a real fault, and proceeding half-configured would surface it far downstream as a stage
    mysteriously deciding nothing had happened.
    """
    token = os.environ.get("DOPPLER_TOKEN")
    if not token:
        return

    # Imported here for the same reason as dotenv below: module scope must stay importable
    # with nothing third-party installed. (`adapters.doppler` is pure stdlib urllib, so this
    # is about consistency rather than necessity.)
    from reconciler.adapters import doppler

    secrets = doppler.fetch(token)
    for name, value in secrets.items():
        os.environ.setdefault(name, value)
    logger.info("loaded %d values from Doppler", len(secrets))


def main(argv=None) -> int:
    # On the SUBcommands, not the top level: `run open --dry-run` is the order everyone
    # actually types, and argparse only accepts a top-level flag *before* the subcommand — so
    # the obvious invocation failed with "unrecognized arguments". Sharing one parent parser
    # puts them after the subcommand instead, where they read naturally.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--processes-dir", type=pathlib.Path, default=DEFAULT_PROCESSES_DIR)
    common.add_argument("--dry-run", action="store_true",
                        help="write no events and send no mail; print what would happen")
    common.add_argument("--mail-to-file", metavar="PATH",
                        help="write the rendered HTML mail here instead of sending it")
    common.add_argument("-v", "--verbose", action="store_true")

    parser = argparse.ArgumentParser(prog="reconciler.run", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    opener = sub.add_parser("open", help="open a new cycle", parents=[common])
    opener.add_argument("--process", required=True)
    opener.add_argument("--seq", type=int,
                        help="per-year sequence number; omit to take the next unused one")
    opener.add_argument("--year", type=int)
    opener.add_argument("--period", metavar="START:END",
                        help="worked period, e.g. 2026-07-01:2026-07-31; omit for a "
                             "manual-lines-only invoice such as a bonus")
    opener.add_argument("--user", default=os.environ.get("AWARE_USER_ID", "rods"))
    opener.set_defaults(func=cmd_open)

    runner = sub.add_parser("reconcile", help="advance existing cycles", parents=[common])
    runner.add_argument("--process", required=True)
    runner.add_argument("--cycle", help="restrict to one cycle key")
    runner.set_defaults(func=cmd_reconcile)

    sweeper = sub.add_parser("sweep-tasks", parents=[common],
                             help="reconcile the Gmail todo label against recorded events")
    sweeper.add_argument("--label", default=DEFAULT_LABEL)
    sweeper.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK_DAYS,
                         metavar="DAYS",
                         help="how far back to search; a task opened before this is never "
                              "closed by the sweep, since Gmail's silence about it is not "
                              "evidence (see tasks.diff)")
    sweeper.add_argument("--user", default=os.environ.get("AWARE_USER_ID", "rods"))
    sweeper.set_defaults(func=cmd_sweep_tasks)

    args = parser.parse_args(argv)

    # Imported here rather than at module scope so this module stays importable with nothing
    # third-party installed — CI runs `pip install -e . --no-deps` plus only pytest, ruff,
    # pydantic and pyyaml, and a top-level `from dotenv import ...` breaks collection there
    # while passing locally. Same reasoning as `adapters/neon.py`'s psycopg import.
    from dotenv import find_dotenv, load_dotenv

    # Same convention as the Quix entrypoint: walk upward from the CWD for a .env, so
    # credentials live in `workers/.env` and never in the repo. In a deployed runner
    # find_dotenv returns "" and the environment already carries the values.
    load_dotenv(find_dotenv(usecwd=True))
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s %(name)s: %(message)s")
    # After logging is configured, so the "loaded N values" line is actually visible — that
    # count is the difference between "misconfigured" and "configured but the process
    # legitimately decided nothing had happened".
    _load_config_from_doppler()
    try:
        return args.func(args)
    except ConfigurationError as e:
        raise SystemExit(str(e)) from None


if __name__ == "__main__":
    sys.exit(main())
