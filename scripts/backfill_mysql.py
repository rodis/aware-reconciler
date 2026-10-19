#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["pymysql>=1.1", "psycopg[binary]>=3.1", "pyyaml", "pydantic>=2.6"]
# ///
"""Rebuild the reconciler's MySQL from Aware's event log in Neon.

Run once before the cutover — and then kept, because **this is also the restore path**. The
tier's store has no backup (there is none anywhere in this estate; AWX's own Postgres has none
either), and what makes that survivable is that nothing here is original: every milestone and
task event still reaches Neon through the ingest gateway, so this database is a projection and
this script is how it is rebuilt. Delete the schema, re-migrate, re-run this, and the
reconciler should produce byte-identical plans. Phase 2's verification is exactly that drill.

    NEON_DATABASE_URL=postgres://... MYSQL_DATABASE_URL=mysql://... \\
        MYSQL_SSL_CA=deploy/kustomize/base/reconciler/mysql-ca.pem \\
        uv run scripts/backfill_mysql.py [--dry-run]

IDEMPOTENT BY UPSERT, deliberately. A backfill you are afraid to re-run is useless as a restore
path, and the natural failure here is a partial run — so every write is `ON DUPLICATE KEY
UPDATE` and the script may be run as many times as you like. It never deletes: a row in MySQL
with no counterpart in Neon is left alone and reported, because the one way that happens today
is a write this tier made and Neon dropped, which is a thing to investigate rather than erase.

THE TRANSFORMS ARE PURE AND TESTED (tests/test_backfill_mysql.py). That is not tidiness: the
whole claim above — that the projection can be rebuilt faithfully — rests on them, and they
encode two rules that are easy to get subtly wrong, the context-key stripping and the
latest-open-vs-latest-close task collapse.
"""

import argparse
import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))

from reconciler.adapters.mysql_dsn import connection_args   # noqa: E402
from reconciler.definition import GENESIS_STAGE         # noqa: E402
from reconciler.tasks import CLOSED_EVENT, OPENED_EVENT  # noqa: E402

# Mirrors `NeonMilestones.cycles`: everything that is envelope rather than context. Kept
# identical on purpose — the backfilled `context` column must equal what the Postgres reader
# would have built in memory, or the replay diff that validates this migration compares two
# different things and passes for the wrong reason.
ENVELOPE_KEYS = {"id", "name", "user_id", "timestamp", "cycle_key", "process"}

# `source_app` is the discriminator, NOT a `name LIKE 'dreamhost_%'` prefix — the dashboard
# makes the same choice. A prefix match would silently adopt any future event whose name began
# with a process's name, and miss a process whose milestones were posted under another app.
_PROCESS_EVENTS_SQL = """
    SELECT name, user_id, EXTRACT(EPOCH FROM occurred_at)::bigint AS ts, message
      FROM events
     WHERE source_app = 'process'
       AND message->>'cycle_key' IS NOT NULL
       AND message->>'process' IS NOT NULL
     ORDER BY occurred_at
"""

_TASK_EVENTS_SQL = """
    SELECT name, user_id, EXTRACT(EPOCH FROM occurred_at)::bigint AS ts, message
      FROM events
     WHERE name IN (%(opened)s, %(closed)s)
       AND message->>'upstream_id' IS NOT NULL
     ORDER BY occurred_at
"""


def _body(message) -> dict:
    return message if isinstance(message, dict) else json.loads(message)


def cycle_rows(events: list[tuple]) -> list[tuple]:
    """Genesis events -> `cycles` rows.

    A cycle exists because `<process>_cycle_opened` was recorded; nothing else creates one
    (ADR 0012: the reconciler advances cycles, it never creates them). Later events for the
    same cycle are milestones and are ignored here.
    """
    rows: dict[str, tuple] = {}
    for name, user_id, ts, message in events:
        body = _body(message)
        process = body["process"]
        if name != f"{process}_{GENESIS_STAGE}":
            continue
        context = {k: v for k, v in body.items() if k not in ENVELOPE_KEYS}
        # Keyed by cycle_key so a genesis emitted twice (a retry that got through) collapses
        # to one row rather than failing the insert, matching the reader's last-write-wins.
        rows[body["cycle_key"]] = (body["cycle_key"], process, user_id, int(ts),
                                   json.dumps(context, sort_keys=True))
    return sorted(rows.values(), key=lambda r: r[3])


def milestone_rows(events: list[tuple]) -> list[tuple]:
    """Every process event -> `milestones` rows, keyed by (cycle_key, stage).

    The genesis event becomes a milestone too, under `cycle_opened` — a stage may depend on
    nothing and still need somewhere to look back to, and the Postgres reader includes it for
    the same reason. Last write wins, which the table's primary key then enforces.
    """
    rows: dict[tuple[str, str], tuple] = {}
    for name, _user_id, ts, message in events:
        body = _body(message)
        process = body["process"]
        prefix = f"{process}_"
        if not name.startswith(prefix):
            # A process event whose name does not carry its own process prefix cannot be
            # placed on a stage. Loud, because silently dropping it would make a cycle look
            # like it stalled at whatever stage came before.
            raise ValueError(f"event {name!r} claims process {process!r} but is not prefixed "
                             f"with it — cannot derive a stage name")
        stage = name[len(prefix):]
        rows[(body["cycle_key"], stage)] = (body["cycle_key"], stage, int(ts),
                                            json.dumps(body, sort_keys=True))
    return sorted(rows.values(), key=lambda r: r[2])


def task_rows(events: list[tuple]) -> list[tuple]:
    """Two append-only event streams -> one row per task.

    This is the anti-join, evaluated once at migration time instead of on every read — and it
    keeps the rule that made the Postgres version compare timestamps rather than test for the
    absence of a close: **latest open versus latest close**. Re-labelling a mail finished last
    month legitimately reopens it, so a close OLDER than the newest open is spent and the task
    is open again. Getting this backwards would quietly bury every reopened task at the moment
    of migration, with nothing to notice it.
    """
    opens: dict[str, tuple[int, dict, str]] = {}
    closes: dict[str, tuple[int, dict]] = {}
    for name, user_id, ts, message in events:
        body = _body(message)
        uid = body["upstream_id"]
        if name == OPENED_EVENT:
            if uid not in opens or ts >= opens[uid][0]:
                opens[uid] = (int(ts), body, user_id)
        elif name == CLOSED_EVENT and (uid not in closes or ts >= closes[uid][0]):
            closes[uid] = (int(ts), body)

    rows = []
    for uid, (opened_at, body, user_id) in opens.items():
        closed_at, closed_via = None, None
        if uid in closes and closes[uid][0] >= opened_at:
            closed_at, closed_via = closes[uid][0], closes[uid][1].get("closed_via")
        rows.append((uid, user_id, body.get("label") or "", body.get("subject"),
                     body.get("from_name"), body.get("from"),
                     opened_at, closed_at, closed_via))
    # A close with no open is NOT invented as a task: it would have to guess an opened_at, and
    # a task whose age is fiction is worse on the board than one that is absent.
    orphans = set(closes) - set(opens)
    if orphans:
        print(f"  note: {len(orphans)} close(s) with no matching open, skipped: "
              f"{', '.join(sorted(orphans))}")
    return sorted(rows, key=lambda r: r[6])


_UPSERTS = {
    "cycles": ("INSERT INTO cycles (cycle_key, process, user_id, opened_at, context) "
               "VALUES (%s, %s, %s, %s, %s) AS new "
               "ON DUPLICATE KEY UPDATE process=new.process, user_id=new.user_id, "
               "opened_at=new.opened_at, context=new.context"),
    "milestones": ("INSERT INTO milestones (cycle_key, stage, occurred_at, payload) "
                   "VALUES (%s, %s, %s, %s) AS new "
                   "ON DUPLICATE KEY UPDATE occurred_at=new.occurred_at, payload=new.payload"),
    "tasks": ("INSERT INTO tasks (upstream_id, user_id, label, subject, from_name, "
              "from_address, opened_at, closed_at, closed_via) "
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) AS new "
              "ON DUPLICATE KEY UPDATE user_id=new.user_id, label=new.label, "
              "subject=new.subject, from_name=new.from_name, from_address=new.from_address, "
              "opened_at=new.opened_at, closed_at=new.closed_at, closed_via=new.closed_via"),
}


def read_neon(dsn: str) -> tuple[list, list]:
    import psycopg
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(_PROCESS_EVENTS_SQL)
        process_events = cur.fetchall()
        cur.execute(_TASK_EVENTS_SQL, {"opened": OPENED_EVENT, "closed": CLOSED_EVENT})
        task_events = cur.fetchall()
    return process_events, task_events


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="read Neon and report what would be written; touch MySQL not at all")
    args = parser.parse_args()

    neon_dsn = os.environ.get("NEON_DATABASE_URL")
    mysql_dsn = os.environ.get("MYSQL_DATABASE_URL")
    if not neon_dsn:
        raise SystemExit("NEON_DATABASE_URL is not set")
    if not mysql_dsn and not args.dry_run:
        raise SystemExit("MYSQL_DATABASE_URL is not set (use --dry-run to preview)")

    process_events, task_events = read_neon(neon_dsn)
    print(f"read {len(process_events)} process event(s), {len(task_events)} task event(s)")

    batches = {
        "cycles": cycle_rows(process_events),
        "milestones": milestone_rows(process_events),
        "tasks": task_rows(task_events),
    }
    for table, rows in batches.items():
        print(f"  {table:12} {len(rows):4} row(s)")

    if args.dry_run:
        print("[dry-run] nothing written")
        return 0

    import pymysql
    connection = pymysql.connect(
        **connection_args(mysql_dsn, ssl_ca=os.environ.get("MYSQL_SSL_CA") or None))
    try:
        with connection.cursor() as cursor:
            # Cycles first: `milestones` has a foreign key onto them, so the order is a
            # constraint rather than a preference.
            for table in ("cycles", "milestones", "tasks"):
                if batches[table]:
                    cursor.executemany(_UPSERTS[table], batches[table])
                print(f"  wrote {table}")
        connection.commit()
    finally:
        connection.close()
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
