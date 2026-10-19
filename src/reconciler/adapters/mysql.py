"""Reading recorded state back out of MySQL — the tier's own store (ADR 0012, amended).

The counterpart of `adapters.neon`, and the reason it exists is a measurement rather than a
preference: the reconciler's *reads* are the only thing that wakes Neon's compute on a clock.
A `sweep-tasks` run does ten seconds of work and buys a five-minute wake, so a `*/15` cron can
never let the compute sleep more than ten minutes at a stretch — a 33% duty cycle that holds on
a day when nothing happened at all. Everything else touching Neon is proportional to real
activity. Moving the reads here removes the clock-driven waker; the writes stay where they are.

**This reverses ADR 0012's "the tier needs no store of its own", deliberately.** What it does
NOT reverse is normative rule 1: the reconciler still acts and Aware still observes. Every
milestone is still POSTed at the ingest gateway (`adapters.gateway`), so Neon keeps a complete
copy, the Aware timeline and both dashboard boards keep working untouched — and MySQL stays
*reconstructible from the event log*, which is what keeps a store with no backup story from
being a new single point of data loss.

**Reads AND writes, unlike `adapters.neon`.** This is the store of record, so the reconciler
writes it directly — see `recording.py` for the ordering and why the gateway POST that follows
is best-effort. `adapters.neon` stays read-only because Neon's writer is Vector.

**Two ports, not one** (see `core.MilestoneStore`): `MysqlMilestones` serves this tier's own
state, `MysqlTasks` serves the sweep. Neither implements `core.EventLog` — `signals()` reads
*Aware's* raw event log, which lives in Neon and which this store genuinely cannot see. That
absence is structural rather than an oversight: a process whose `await` names an Aware event
must keep a Neon reader, and finding out at wiring time is the point.

**Schema expected** (authored in Phase 2; this adapter is written against it):

    cycles     (cycle_key PK, process, user_id, opened_at, context JSON)
    milestones (cycle_key, stage, occurred_at, payload JSON, PRIMARY KEY (cycle_key, stage))
    tasks      (upstream_id PK, user_id, label, subject, from_name, from_address,
                opened_at, closed_at NULL, closed_via)

Two shapes are simpler here than in Postgres, and both are the schema doing work Python used
to do: `PRIMARY KEY (cycle_key, stage)` *is* the last-write-wins rule `NeonMilestones.milestones`
implements with a dict, and `WHERE closed_at IS NULL` replaces the latest-open-vs-latest-close
anti-join wholesale. The reopen semantics that anti-join protected survive as a write-side rule
(clear `closed_at`, bump `opened_at`) — see `tasks.py` for why re-labelling a finished mail has
to work.

**Until Phase 2 backfills these tables they are empty**, and an empty `tasks` table reads as
"nothing is open" rather than as an error — which would make a sweep re-open every labelled
mail. That is why the store is selected by an env var that defaults to Neon: flipping it is a
deliberate act that comes *after* the backfill, never before.

**The target, verified from inside the cluster.** MySQL **8.4.8** at `mysql.example.com`,
database `rods_reconciler`, user `rods_reconciler`, negotiating TLSv1.3 /
TLS_AES_256_GCM_SHA384. Two properties of it shape the code here rather than the deployment:

- The certificate is MySQL 8.4's **auto-generated, self-signed** one, so the CA must be pinned
  and `ssl_verify_identity` must stay off — its CN is not the hostname.
- The account uses **`caching_sha2_password`** (8.4 dropped `mysql_native_password`). This
  does NOT make TLS mandatory — tested, the handshake falls back to an RSA public-key
  exchange and a plaintext login succeeds. TLS is refused here as **policy**, because the
  link crosses the public internet, and a CA is **required** because the only concise way
  to ask PyMySQL for CA-less TLS silently gives you no TLS at all (see `tls_context`).

**Allowable Hosts is enforced before the handshake and matches on reverse DNS.** The three
current k8s worker IPs are allow-listed. The consequence worth knowing at 3am: a connection
from a new or rebuilt node is refused in a way that looks like an **auth** failure, not a
network one — so "bad credentials" after a node replacement is the allow-list, not the
password.

`pymysql` is a dependency of the *runner image* (`workers/reconciler/requirements.txt`) and
deliberately not of the project, so a local checkout does not have it — `pip install -e .`
would otherwise put a MySQL driver in the inference runtime's image too. To run this adapter
by hand, bring it along for the one command, the way the tier already borrows Prefect:

    (cd workers && uv run --with pymysql python -m reconciler.run sweep-tasks --dry-run)
"""

import json
import logging

# Re-exported: `connection_args` is part of this module's public surface (tests and the
# migration runner both reach for it), it just no longer lives here — see mysql_dsn.
from reconciler.adapters.mysql_dsn import connection_args, tls_context  # noqa: F401
from reconciler.core import Cycle, Milestone
from reconciler.definition import GENESIS_STAGE, ProcessDefinition
from reconciler.tasks import OpenTask

logger = logging.getLogger("reconciler.adapters.mysql")

_CYCLES_SQL = """
    SELECT cycle_key, user_id, opened_at, context
      FROM cycles
     WHERE process = %(process)s
     ORDER BY opened_at
"""

# No `process` predicate: `cycles.cycle_key` is the primary key, so it is unique across every
# process and the cycle already names exactly one.
_MILESTONES_SQL = """
    SELECT stage, occurred_at, payload
      FROM milestones
     WHERE cycle_key = %(cycle_key)s
     ORDER BY occurred_at
"""

# The whole anti-join, collapsed. A task is open when it has no close — which is a column here
# rather than a second event to out-join against.
_OPEN_TASKS_SQL = """
    SELECT upstream_id, subject, from_name, from_address, opened_at
      FROM tasks
     WHERE user_id = %(user_id)s
       AND label = %(label)s
       AND closed_at IS NULL
"""


class _Reader:
    """A fresh connection per query, and the shared parsing both readers need."""

    def __init__(self, dsn: str, ssl_ca: str | None = None):
        self._args = connection_args(dsn, ssl_ca=ssl_ca)

    def _query(self, sql: str, params: dict) -> list[tuple]:
        # pymysql is imported HERE, not at module scope, and that is a CI contract rather than
        # a style choice: the checks workflow installs with `pip --no-deps`, so anything a test
        # imports transitively must be importable with no third-party packages present. This
        # module is reached from `reconciler.app`, which the flow and CLI tests both import —
        # the same trap `adapters/neon.py` fell into with a module-level `import psycopg`,
        # which collapsed the whole pytest collection.
        import pymysql

        connection = pymysql.connect(**self._args)
        try:
            with connection.cursor() as cur:
                cur.execute(sql, params)
                return list(cur.fetchall())
        finally:
            connection.close()

    def _execute(self, statements: list[tuple[str, dict]]) -> None:
        """Run several statements in ONE transaction, or none of them.

        Atomicity matters for exactly one case and it is the important one: opening a cycle
        writes a `cycles` row *and* its genesis `milestones` row, and a crash between them
        would leave a cycle whose own opening is not among its milestones — so `reconcile`
        would see an empty milestone set and try to re-open a cycle that already exists.
        """
        import pymysql

        connection = pymysql.connect(**self._args)
        try:
            with connection.cursor() as cur:
                for sql, params in statements:
                    cur.execute(sql, params)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _json(value) -> dict:
        """A JSON column, whatever the driver handed back.

        PyMySQL returns a MySQL `JSON` column as text rather than decoding it, and which of
        `str`/`bytes` depends on the connection charset — so normalise here instead of at four
        call sites. `None` is a column that was never written, not a parse failure.
        """
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        if isinstance(value, bytes | bytearray):
            value = value.decode("utf-8")
        return json.loads(value)


class MysqlMilestones(_Reader):
    """Loads cycles and their milestones for a process. Satisfies `core.MilestoneStore`."""

    def cycles(self, definition: ProcessDefinition) -> list[Cycle]:
        rows = self._query(_CYCLES_SQL, {"process": definition.name})
        cycles = [
            Cycle(key=cycle_key, process=definition.name, user_id=user_id,
                  opened_at=int(opened_at), context=self._json(context))
            for cycle_key, user_id, opened_at, context in rows
        ]
        logger.info("process %s has %d cycle(s)", definition.name, len(cycles))
        return cycles

    def milestones(self, definition: ProcessDefinition,
                   cycle: Cycle) -> dict[str, Milestone]:
        rows = self._query(_MILESTONES_SQL, {"cycle_key": cycle.key})
        # No last-write-wins pass: `PRIMARY KEY (cycle_key, stage)` means the row IS the
        # resolution, so a re-emitted milestone updated its row rather than adding one.
        return {
            stage: Milestone(stage=stage, timestamp=int(occurred_at),
                             payload=self._json(payload))
            for stage, occurred_at, payload in rows
        }


class MysqlTasks(_Reader):
    """The open half of the email todo list. Satisfies `tasks.OpenTaskStore`."""

    def open_tasks(self, user_id: str, label: str) -> dict[str, OpenTask]:
        rows = self._query(_OPEN_TASKS_SQL, {"user_id": user_id, "label": label})
        found = {
            upstream_id: OpenTask(
                upstream_id=upstream_id,
                subject=subject or "",
                from_name=from_name or "",
                from_address=from_address or "",
                opened_at=int(opened_at or 0),
            )
            for upstream_id, subject, from_name, from_address, opened_at in rows
        }
        logger.info("%d open task(s) for %s", len(found), user_id)
        return found


# --- writing (the store of record) -------------------------------------------------------------
#
# Every statement is an UPSERT rather than an INSERT, and that is the same property the whole
# tier is built on rather than a convenience: a run is a pure function of recorded state and is
# always safe to repeat, so a retry that re-records a milestone it already recorded must be a
# no-op, not a duplicate-key crash.

_PUT_CYCLE_SQL = """
    INSERT INTO cycles (cycle_key, process, user_id, opened_at, context)
    VALUES (%(cycle_key)s, %(process)s, %(user_id)s, %(opened_at)s, %(context)s) AS new
    ON DUPLICATE KEY UPDATE process=new.process, user_id=new.user_id,
                            opened_at=new.opened_at, context=new.context
"""

# The primary key (cycle_key, stage) IS the last-write-wins rule the Postgres reader applies in
# Python; here the upsert expresses it directly.
_PUT_MILESTONE_SQL = """
    INSERT INTO milestones (cycle_key, stage, occurred_at, payload)
    VALUES (%(cycle_key)s, %(stage)s, %(occurred_at)s, %(payload)s) AS new
    ON DUPLICATE KEY UPDATE occurred_at=new.occurred_at, payload=new.payload
"""

# REOPENING LIVES HERE. Re-labelling a mail finished last month is a legitimate way to reopen
# it, so an open must clear `closed_at` rather than be refused — the Postgres version preserves
# the same rule by comparing latest-open against latest-close instead of testing for absence.
_OPEN_TASK_SQL = """
    INSERT INTO tasks (upstream_id, user_id, label, subject, from_name, from_address,
                       opened_at, closed_at, closed_via)
    VALUES (%(upstream_id)s, %(user_id)s, %(label)s, %(subject)s, %(from_name)s,
            %(from_address)s, %(opened_at)s, NULL, NULL) AS new
    ON DUPLICATE KEY UPDATE user_id=new.user_id, label=new.label, subject=new.subject,
                            from_name=new.from_name, from_address=new.from_address,
                            opened_at=new.opened_at, closed_at=NULL, closed_via=NULL
"""

# Closing a task that is not open is a no-op (zero rows matched), not an error: the sweep
# derives closes from Gmail's current state, so a close for something already closed is a
# legitimate repeat rather than a fault.
_CLOSE_TASK_SQL = """
    UPDATE tasks SET closed_at = %(closed_at)s, closed_via = %(closed_via)s
     WHERE upstream_id = %(upstream_id)s
"""


class MysqlWriter(_Reader):
    """Writes the facts the reconciler reads back. The store-of-record half.

    Deliberately separate from the two readers rather than folded into them: a reader is handed
    to `core` as a port and must stay incapable of writing, while this is handed to the sink.
    """

    def put_cycle(self, cycle: Cycle) -> None:
        """A cycle and its genesis milestone, together or not at all."""
        body = {"process": cycle.process, "cycle_key": cycle.key,
                "user_id": cycle.user_id, "timestamp": cycle.opened_at, **cycle.context}
        self._execute([
            (_PUT_CYCLE_SQL, {"cycle_key": cycle.key, "process": cycle.process,
                              "user_id": cycle.user_id, "opened_at": cycle.opened_at,
                              "context": json.dumps(cycle.context, sort_keys=True)}),
            (_PUT_MILESTONE_SQL, {"cycle_key": cycle.key, "stage": GENESIS_STAGE,
                                  "occurred_at": cycle.opened_at,
                                  "payload": json.dumps(body, sort_keys=True)}),
        ])

    def put_milestone(self, cycle: Cycle, stage: str, occurred_at: int, body: dict) -> None:
        self._execute([(_PUT_MILESTONE_SQL, {
            "cycle_key": cycle.key, "stage": stage, "occurred_at": int(occurred_at),
            "payload": json.dumps(body, sort_keys=True)})])

    def open_task(self, body: dict) -> None:
        self._execute([(_OPEN_TASK_SQL, {
            "upstream_id": body["upstream_id"], "user_id": body["user_id"],
            "label": body.get("label") or "", "subject": body.get("subject"),
            "from_name": body.get("from_name"), "from_address": body.get("from"),
            "opened_at": int(body["timestamp"])})])

    def close_task(self, body: dict) -> None:
        self._execute([(_CLOSE_TASK_SQL, {
            "upstream_id": body["upstream_id"], "closed_at": int(body["timestamp"]),
            "closed_via": body.get("closed_via")})])
