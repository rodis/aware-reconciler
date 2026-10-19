"""Reading recorded milestones back out of Neon (ADR 0012).

Neon is the process's state store, and it is already there: Vector persists every raw event
it receives, so a milestone POSTed to the ingest gateway lands in `events` without this tier
owning a table. That is the whole reason the reconciler needs no store of its own — and why
"what state is this process in?" is a query rather than a runtime introspection.

Reads only. Nothing here writes: milestones reach Neon the same way every other event does,
through the gateway (`adapters.gateway`), so the tier never becomes a second writer competing
with the persister.

Two readers live here now — `NeonMilestones` for process cycles and `NeonTasks` for the email
todo sweep. They answer different questions but share the reason for being in one module: every
SQL statement the tier runs is in one file, so a schema change has one place to look.
"""

import json
import logging

from reconciler.core import Cycle, Milestone
from reconciler.definition import GENESIS_STAGE, ProcessDefinition
from reconciler.tasks import CLOSED_EVENT, OPENED_EVENT, OpenTask

logger = logging.getLogger("reconciler.adapters.neon")

# Milestones are raw events named `<process>_<stage>` carrying `cycle_key` in the body. The
# jsonb GIN index on `message` makes the containment test cheap.
_MILESTONES_SQL = """
    SELECT name, user_id,
           EXTRACT(EPOCH FROM occurred_at)::bigint AS ts,
           message
      FROM events
     WHERE event_class = 'raw'
       AND name LIKE %(prefix)s
       AND message @> %(match)s
     ORDER BY occurred_at
"""

_SIGNAL_SQL = """
    SELECT EXTRACT(EPOCH FROM occurred_at)::bigint AS ts, message
      FROM events
     WHERE name = %(name)s
       AND occurred_at >= to_timestamp(%(since)s)
     ORDER BY occurred_at
"""

_OPEN_CYCLES_SQL = """
    SELECT DISTINCT ON (message->>'cycle_key')
           message->>'cycle_key' AS cycle_key,
           user_id,
           EXTRACT(EPOCH FROM occurred_at)::bigint AS ts,
           message
      FROM events
     WHERE event_class = 'raw'
       AND name = %(genesis)s
     ORDER BY message->>'cycle_key', occurred_at
"""


class NeonMilestones:
    """Loads cycles and their milestones for a process."""

    def __init__(self, dsn: str):
        self._dsn = dsn

    def _query(self, sql: str, params: dict) -> list[tuple]:
        # psycopg is imported HERE, not at module scope, and that is a CI contract rather than
        # a style choice: the checks workflow installs with `pip --no-deps`, so anything a test
        # imports transitively must be importable with no third-party packages present. This
        # module is reached from `reconciler.app`, which the flow and CLI tests both import —
        # a module-level `import psycopg` collapsed the whole pytest collection.
        import psycopg

        # A fresh connection per call, deliberately. Neon's compute suspends when idle
        # (suspend_timeout=0), so a long-lived pooled connection is a dead socket waiting to
        # happen — the failure the dashboard hit. A reconciler run is short and infrequent;
        # connecting per query costs nothing worth optimising.
        with psycopg.connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def cycles(self, definition: ProcessDefinition) -> list[Cycle]:
        """Every cycle ever opened for this process, oldest genesis first.

        Returns cycles regardless of state — completed and voided ones included. Deciding
        what to do with them is `core.reconcile`'s job, and it already skips both cheaply.
        """
        rows = self._query(_OPEN_CYCLES_SQL,
                           {"genesis": definition.event_name(GENESIS_STAGE)})
        cycles = []
        for cycle_key, user_id, ts, message in rows:
            body = message if isinstance(message, dict) else json.loads(message)
            context = {k: v for k, v in body.items()
                       if k not in {"id", "name", "user_id", "timestamp",
                                    "cycle_key", "process"}}
            cycles.append(Cycle(key=cycle_key, process=definition.name, user_id=user_id,
                                opened_at=int(ts), context=context))
        logger.info("process %s has %d cycle(s)", definition.name, len(cycles))
        return cycles

    def milestones(self, definition: ProcessDefinition, cycle: Cycle) -> dict[str, Milestone]:
        """The milestones recorded for one cycle, keyed by stage name.

        The genesis event is included under its own reserved name so a stage may depend on
        nothing and still have somewhere to look back to.
        """
        prefix_len = len(definition.name) + 1
        rows = self._query(_MILESTONES_SQL, {
            "prefix": f"{definition.name}\\_%",
            "match": json.dumps({"cycle_key": cycle.key}),
        })
        found: dict[str, Milestone] = {}
        for name, _user_id, ts, message in rows:
            body = message if isinstance(message, dict) else json.loads(message)
            stage = name[prefix_len:]
            # Last write wins: a re-emitted milestone (a retry that got through twice)
            # should not produce two conflicting views of the same stage.
            found[stage] = Milestone(stage=stage, timestamp=int(ts), payload=body)
        return found

    def signals(self, name: str, since: int) -> list[tuple[int, dict]]:
        """Raw events of one name at or after `since`, oldest first.

        The finder's read side. Deliberately dumb — no matching, no interpretation: deciding
        whether one of these *satisfies* a stage is `finder`'s job, and keeping the SQL free
        of that keeps the correlation rules testable without a database.
        """
        rows = self._query(_SIGNAL_SQL, {"name": name, "since": since})
        return [(int(ts), message if isinstance(message, dict) else json.loads(message))
                for ts, message in rows]


# --- email todo tasks (see reconciler.tasks) ---------------------------------------------------
#
# A task is open when its latest `email_labeled_todo` is newer than its latest
# `email_task_closed` — or when there is no close at all.
#
# **Latest-vs-latest, not "has ever been closed".** Re-applying the label to a mail you closed
# last month is a legitimate way to reopen a task, and an `IS NULL` anti-join would silently
# refuse to let you: the old close would suppress the new open forever, and the task would be
# invisible in the UI while being visibly labelled in Gmail. The sweep would then see it
# labelled with no open event, emit another open, and do so again every hour — a permanent
# event-per-hour loop with no symptom other than a growing table. Comparing timestamps costs one
# extra DISTINCT ON and makes reopening work by construction.
_OPEN_TASKS_SQL = """
    WITH opened AS (
      SELECT DISTINCT ON (message->>'upstream_id')
             message->>'upstream_id' AS uid,
             EXTRACT(EPOCH FROM occurred_at)::bigint AS ts,
             message
        FROM events
       WHERE name = %(opened)s
         AND user_id = %(user_id)s
         AND message->>'label' = %(label)s
         AND message->>'upstream_id' IS NOT NULL
       ORDER BY message->>'upstream_id', occurred_at DESC
    ),
    closed AS (
      SELECT DISTINCT ON (message->>'upstream_id')
             message->>'upstream_id' AS uid,
             EXTRACT(EPOCH FROM occurred_at)::bigint AS ts
        FROM events
       WHERE name = %(closed)s
         AND user_id = %(user_id)s
         AND message->>'label' = %(label)s
       ORDER BY message->>'upstream_id', occurred_at DESC
    )
    SELECT o.uid, o.ts, o.message
      FROM opened o
      LEFT JOIN closed c ON c.uid = o.uid
     WHERE c.uid IS NULL OR c.ts < o.ts
"""


class NeonTasks:
    """The open half of the email todo list, read back out of the event log."""

    def __init__(self, dsn: str):
        self._dsn = dsn

    def open_tasks(self, user_id: str, label: str) -> dict[str, OpenTask]:
        import psycopg   # lazy, for the same CI reason as NeonMilestones._query

        with psycopg.connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(_OPEN_TASKS_SQL, {
                "user_id": user_id, "label": label,
                "opened": OPENED_EVENT, "closed": CLOSED_EVENT,
            })
            rows = cur.fetchall()

        tasks = {}
        for uid, ts, message in rows:
            body = message if isinstance(message, dict) else json.loads(message)
            tasks[uid] = OpenTask(
                upstream_id=uid,
                subject=body.get("subject") or "",
                from_name=body.get("from_name") or "",
                from_address=body.get("from") or "",
                opened_at=int(ts or 0),
            )
        logger.info("%d open task(s) for %s", len(tasks), user_id)
        return tasks
