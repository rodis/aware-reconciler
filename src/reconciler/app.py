"""Composition: turning environment into a wired process run (ADR 0012).

The seam between `core` (pure) and the two ways a run is started — `run.py` by hand, `flow.py`
on a schedule. Both call the functions here, which is what keeps the runner swappable: swapping
Prefect for something else touches `flow.py` and nothing beneath it, and neither entry point
duplicates the wiring.

Nothing here knows about argparse, and nothing here knows about Prefect. That is the test for
whether a thing belongs in this file.
"""

import logging
import os
import pathlib
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from reconciler.actions import Services
from reconciler.adapters.craftmypdf import CraftMyPdf
from reconciler.adapters.gateway import DryRunMilestones, GatewayMilestones
from reconciler.adapters.gmail import N8nGmailQuery
from reconciler.adapters.llm import N8nGeminiRelay
from reconciler.adapters.mail import (
    ConsoleMailer,
    FileMailer,
    N8nRelayMailer,
    SmtpMailer,
)
from reconciler.adapters.ingest import DryRunEvents, GatewayEvents
from reconciler.adapters.mysql import MysqlMilestones, MysqlTasks, MysqlWriter
from reconciler.adapters.neon import NeonMilestones, NeonTasks
from reconciler.core import Cycle, Milestone, MilestoneStore, Outcome, reconcile
from reconciler.definition import GENESIS_STAGE, ProcessDefinition, load_definitions
from reconciler import tasks
from reconciler.tasks import TASK_INGEST_APP
from reconciler.finder import SignalFinder
from reconciler.recording import (
    DryRunWriter,
    StoreBackedEvents,
    StoreBackedMilestones,
)
from reconciler.world import NotYetImplemented, RealWorld

logger = logging.getLogger("reconciler.app")

DEFAULT_PROCESSES_DIR = pathlib.Path(__file__).resolve().parents[2] / "processes"

# `{seq:03d}` in the shipped cycle_key, so three digits is the natural ceiling. Used only to
# enumerate candidate keys when reading a sequence back out — see `sequence_of`.
MAX_SEQUENCE = 999


class ConfigurationError(RuntimeError):
    """A required credential or endpoint is missing.

    Its own type because the two entry points report it differently — the CLI exits with a
    message, the flow fails the run so Prefect surfaces it — and neither should have to
    string-match a generic error to tell "misconfigured" from "the process legitimately
    stopped".
    """


@dataclass(frozen=True)
class RunOptions:
    """How a run should behave, independent of what it is running."""

    dry_run: bool = False
    mail_to_file: str | None = None
    processes_dir: pathlib.Path = DEFAULT_PROCESSES_DIR


def definition_named(name: str, processes_dir: pathlib.Path) -> ProcessDefinition:
    for definition in load_definitions(processes_dir):
        if definition.name == name:
            return definition
    raise ConfigurationError(f"no enabled process named {name!r} in {processes_dir}")


# --- wiring ---------------------------------------------------------------------------------

def mailer_for(options: RunOptions):
    """Pick a transport: file > console(dry-run) > n8n relay > SMTP.

    The relay is the normal path — it keeps the SMTP credential in n8n's store rather than in
    this repo. Direct SMTP stays available for local-only testing, and is only reached when no
    relay is configured.
    """
    if options.mail_to_file:
        return FileMailer(pathlib.Path(options.mail_to_file))
    if options.dry_run:
        return ConsoleMailer()
    if os.environ.get("MAIL_RELAY_URL"):
        return N8nRelayMailer(
            url=os.environ["MAIL_RELAY_URL"],
            token=os.environ["MAIL_RELAY_TOKEN"],
            header=os.environ.get("MAIL_RELAY_HEADER", "X-Relay-Token"),
            recipient=os.environ["MAIL_TO"],
            sender=os.environ.get("MAIL_FROM"),
        )
    if os.environ.get("SMTP_HOST"):
        return SmtpMailer(
            host=os.environ["SMTP_HOST"],
            port=int(os.environ.get("SMTP_PORT", 587)),
            username=os.environ["SMTP_USERNAME"],
            password=os.environ["SMTP_PASSWORD"],
            sender=os.environ["SMTP_SENDER"],
            recipient=os.environ["MAIL_TO"],
        )
    return ConsoleMailer()


def services_for(options: RunOptions) -> Services:
    # `extras` stays unwired: ADR 0012 open question 8 (where manual lines live) is unsettled.
    key = os.environ.get("CRAFTMYPDF_API_KEY")
    return Services(mailer=mailer_for(options),
                    pdf=CraftMyPdf(api_key=key) if key else None)


def sink_for(options: RunOptions, definition: ProcessDefinition):
    """Where a milestone goes: the store of record, then the gateway.

    Wrapped ONLY when MySQL is selected. On Neon the gateway POST *is* the write — Vector
    persists it — so wrapping there would make the tier a second writer competing with the
    persister, which ADR 0012 forbids and which `adapters/neon.py` says in its first paragraph.
    """
    downstream = (DryRunMilestones(definition) if options.dry_run
                  else GatewayMilestones(_vector_base(), definition))
    writer = _writer(options)
    return (StoreBackedMilestones(definition, writer, downstream) if writer
            else downstream)


def _vector_base() -> str:
    base = os.environ.get("VECTOR_BASE_URL")
    if not base:
        raise ConfigurationError("VECTOR_BASE_URL is not set (use dry_run to preview)")
    return base


def _writer(options: RunOptions):
    """The store-of-record writer, or None when the store writes itself.

    None on Neon is the whole point: there the gateway POST already reaches the store, via
    Kafka and Vector. On MySQL nothing else writes, so the tier must.
    """
    if _store_choice() != STORE_MYSQL:
        return None
    if options.dry_run:
        return DryRunWriter()          # before the DSN: a preview needs no connection either
    _, dsn = _store_dsn()
    return MysqlWriter(dsn, ssl_ca=_mysql_ssl_ca())


# Which store the tier reads its own recorded state from. Neon is the default because the
# migration to MySQL is a cutover that happens AFTER a backfill — an empty `tasks` table reads
# as "nothing is open" rather than as an error, so a sweep run against an unpopulated MySQL
# would re-open every labelled mail. Flipping this is therefore a deliberate act, and a
# rollback is the same act in reverse: config, not a deploy.
STORE_NEON = "neon"
STORE_MYSQL = "mysql"
_DSN_VAR = {STORE_NEON: "NEON_DATABASE_URL", STORE_MYSQL: "MYSQL_DATABASE_URL"}


def _store_choice() -> str:
    """Which store is selected — answerable WITHOUT a DSN, deliberately.

    Separate from `_store_dsn` because `sink_for` needs to know whether a writer is required
    before it knows whether it can build one, and on Neon the answer is "no writer" — at which
    point demanding a connection string would make a path that touches no database fail for
    lack of database configuration. `walk_fresh` is exactly that path, and a test pins it.
    """
    choice = os.environ.get("RECONCILER_STORE", STORE_NEON).strip().lower()
    if choice not in _DSN_VAR:
        raise ConfigurationError(
            f"RECONCILER_STORE={choice!r} is not one of {sorted(_DSN_VAR)}")
    return choice


def _store_dsn() -> tuple[str, str]:
    """The selected store and its DSN, or a `ConfigurationError` naming what is missing."""
    choice = _store_choice()
    dsn = os.environ.get(_DSN_VAR[choice])
    if not dsn:
        raise ConfigurationError(f"{_DSN_VAR[choice]} is not set (RECONCILER_STORE={choice})")
    return choice, dsn


def _mysql_ssl_ca() -> str | None:
    """Where the MySQL server's CA is mounted — manifest config, not a credential.

    Read here rather than inside the adapter so `connection_args` stays a pure function of its
    arguments, and kept out of the DSN so Doppler holds a credential and nothing else: moving
    the mount is then a manifest diff rather than a secret edit with no history. See
    `deploy/kustomize/base/reconciler/values.yml`, which sets this beside
    `RECONCILER_STORE` and owns the volume it points at.
    """
    return os.environ.get("MYSQL_SSL_CA") or None


def milestone_store() -> MilestoneStore:
    """The store a process run reads cycles and milestones from."""
    choice, dsn = _store_dsn()
    if choice == STORE_MYSQL:
        return MysqlMilestones(dsn, ssl_ca=_mysql_ssl_ca())
    return NeonMilestones(dsn)


def task_store() -> tasks.OpenTaskStore:
    """The store the sweep reads open tasks from."""
    choice, dsn = _store_dsn()
    if choice == STORE_MYSQL:
        return MysqlTasks(dsn, ssl_ca=_mysql_ssl_ca())
    return NeonTasks(dsn)


def finders() -> dict:
    """Finders by signal `source`.

    Both read the same Gmail mailbox through the same n8n question — asked at decision time, so
    every loop stays on this side and an unreachable n8n raises instead of looking like
    "nothing labelled yet". They differ only in what settles the match:

    - `gmail`    — a label somebody applied. The label IS the decision; nothing to interpret.
    - `classify` — the same candidates, plus a reading, for the one distinction a label cannot
      carry (a payment *sent* vs the same payment *completed*).

    An unwired `classify` is left absent rather than degraded to `gmail`: `world.find` then
    raises `NotYetImplemented` naming the source, which is the loud stop. Silently falling back
    would match the first Tipalti mail mentioning the invoice and record the wrong step.
    """
    url = os.environ.get("GMAIL_QUERY_URL")
    if not url:
        return {}
    query = N8nGmailQuery(
        url=url,
        token=os.environ["MAIL_RELAY_TOKEN"],
        header=os.environ.get("MAIL_RELAY_HEADER", "X-Relay-Token"),
    )
    built = {"gmail": SignalFinder(query)}

    if os.environ.get("LLM_RELAY_URL"):
        built["classify"] = SignalFinder(query, classifier=N8nGeminiRelay(
            url=os.environ["LLM_RELAY_URL"],
            token=os.environ["MAIL_RELAY_TOKEN"],
            header=os.environ.get("MAIL_RELAY_HEADER", "X-Relay-Token"),
        ))
    else:
        logger.warning("LLM_RELAY_URL is not set; `classify` stages will stop rather than "
                       "advance (see connectors/n8n/llm-relay.workflow.ts)")
    return built


# --- email todo tasks -----------------------------------------------------------------------
#
# A second job in this tier, and deliberately NOT a process: a task has two states, not stages,
# so `processes/*.yml` would mint a cycle per email. What the two share is the tier's actual
# idea — run on a schedule, be a pure function of what is recorded, and therefore be safe to
# re-run. See `reconciler.tasks`.

def task_query() -> "N8nGmailQuery":
    """The Gmail client, asked for a whole label rather than for one gate's candidates.

    Same object the invoice's gates use. Reusing it rather than writing a second client is what
    keeps mailparser's `from` object flattened in exactly one tested place.
    """
    url = os.environ.get("GMAIL_QUERY_URL")
    if not url:
        raise ConfigurationError("GMAIL_QUERY_URL is not set")
    return N8nGmailQuery(
        url=url,
        token=os.environ["MAIL_RELAY_TOKEN"],
        header=os.environ.get("MAIL_RELAY_HEADER", "X-Relay-Token"),
    )


def task_sink(options: RunOptions):
    if options.dry_run:
        writer = _writer(options)
        return StoreBackedEvents(writer, DryRunEvents()) if writer else DryRunEvents()
    base = os.environ.get("VECTOR_BASE_URL")
    if not base:
        raise ConfigurationError("VECTOR_BASE_URL is not set (use dry_run to preview)")
    writer = _writer(options)
    sink = GatewayEvents(base, app=TASK_INGEST_APP)
    return StoreBackedEvents(writer, sink) if writer else sink


def sweep_tasks(*, label: str = tasks.DEFAULT_LABEL, user: str | None = None,
                lookback_days: int = tasks.DEFAULT_LOOKBACK_DAYS,
                options: RunOptions | None = None) -> tasks.SweepPlan:
    """One sweep: reconcile the Gmail label against the recorded task events."""
    options = options or RunOptions()
    return tasks.sweep(
        source=task_query(),
        store=task_store(),
        sink=task_sink(options),
        user_id=user or os.environ.get("AWARE_USER_ID", "rods"),
        label=label,
        lookback_days=lookback_days,
    )

# --- sequence -------------------------------------------------------------------------------

def sequence_of(definition: ProcessDefinition, cycle_key: str, year: int) -> int | None:
    """The sequence number inside a cycle key, or None if it is not this year's.

    Read by *generating* candidate keys and comparing, rather than by parsing. The template is
    a format string with an arbitrary spec (`{seq:03d}` today), and a regex reconstructed from
    it would have to re-implement that spec — this way the same `str.format` that minted the
    key is the one that reads it back, so the two cannot disagree.
    """
    for n in range(1, MAX_SEQUENCE + 1):
        if definition.cycle_key.format(year=year, seq=n) == cycle_key:
            return n
    return None


def next_sequence(definition: ProcessDefinition, cycles: list[Cycle], year: int) -> int:
    """The sequence a new cycle should take: one past the highest already used this year.

    **Derived from the cycles themselves, not from a counter.** A counter would be state the
    tier does not otherwise keep, and would drift the moment a cycle was opened by hand — which
    is a supported way to open one (`opens: {via: manual}`). This is the same property the rest
    of the tier has: the answer is a function of what has been recorded.

    Resolves ADR 0012's open question 10. Note it reads keys, not bodies, so it stays generic:
    nothing here knows the number is called an invoice number.
    """
    used = [n for cycle in cycles
            if (n := sequence_of(definition, cycle.key, year)) is not None]
    return max(used, default=0) + 1


def previous_month(today: date) -> dict[str, str]:
    """The calendar month before `today`, as a worked period.

    The default a *cron* opener implies: a schedule that fires on the 1st is invoicing the
    month that just ended, not the one starting that morning. Passing `period` explicitly
    overrides it, which is what a process on a different cadence would do.
    """
    end = today.replace(day=1) - timedelta(days=1)
    return {"start": end.replace(day=1).isoformat(), "end": end.isoformat()}


# --- the two things a run can do -------------------------------------------------------------

def open_cycle(process: str, *, seq: int | None = None, period: dict | None = None,
               year: int | None = None, user: str | None = None,
               options: RunOptions | None = None) -> Cycle:
    """Record `cycle_opened` — the genesis fact a cycle exists because of.

    Separate from `advance` on purpose, and the separation is normative: **the reconciler
    advances cycles, it never creates them** (ADR 0012). A cycle comes into being because
    something happened — a schedule fired, or a person asked — and that event is recorded like
    any other. A reconcile loop that could also open cycles would quietly become a scheduler,
    and "why does this process have two Decembers?" would be a question about a race.
    """
    options = options or RunOptions()
    definition = definition_named(process, options.processes_dir)
    year = year or datetime.now(UTC).year

    if seq is None:
        # Only reachable when a schedule opened this cycle; a human always names the number.
        seq = next_sequence(definition, milestone_store().cycles(definition), year)
        logger.info("no sequence given; next unused for %d is %d", year, seq)

    context: dict = {"invoice_number": seq}
    if period:
        date.fromisoformat(period["start"]), date.fromisoformat(period["end"])   # validate
        context["worked_period"] = period

    cycle = Cycle(key=definition.cycle_key.format(year=year, seq=seq),
                  process=definition.name,
                  user_id=user or os.environ.get("AWARE_USER_ID", "rods"),
                  opened_at=int(datetime.now(UTC).timestamp()), context=context)

    sink_for(options, definition).record(cycle, GENESIS_STAGE, context)
    logger.info("opened cycle %s", cycle.key)
    return cycle


def advance(process: str, *, cycle_key: str | None = None,
            options: RunOptions | None = None) -> list[tuple[Cycle, Outcome | None]]:
    """Advance every cycle of a process as far as it will go.

    A cycle that reaches unbuilt machinery yields `None` rather than raising: everything before
    that point was already recorded (the emit happens before the next stage is attempted, which
    is what makes a crashed run resumable), so the run is a partial success, not a failure.
    """
    options = options or RunOptions()
    definition = definition_named(process, options.processes_dir)
    store = milestone_store()
    sink = sink_for(options, definition)

    cycles = store.cycles(definition)
    if cycle_key:
        cycles = [c for c in cycles if c.key == cycle_key]
        if not cycles:
            raise ConfigurationError(f"no cycle {cycle_key!r} for process {definition.name}")

    results: list[tuple[Cycle, Outcome | None]] = []
    for cycle in cycles:
        results.append((cycle, advance_one(definition, cycle,
                                           store.milestones(definition, cycle),
                                           options, sink)))
    return results


def walk_fresh(process: str, cycle: Cycle, *,
               options: RunOptions | None = None) -> Outcome | None:
    """Advance a cycle that has no recorded milestones yet.

    The preview path, and the reason it exists separately from `advance`: a just-opened cycle
    has nothing in Neon to read, so walking it needs **no database at all**. That is most of
    the value of `--dry-run` — seeing exactly what the approval mail would say, with no
    credentials beyond the ones the mail itself needs.
    """
    options = options or RunOptions()
    definition = definition_named(process, options.processes_dir)
    return advance_one(definition, cycle, {}, options, sink_for(options, definition))


def advance_one(definition: ProcessDefinition, cycle: Cycle,
                milestones: dict[str, Milestone], options: RunOptions, sink) -> Outcome | None:
    world = RealWorld(definition, sink=sink, services=services_for(options), finders=finders())
    try:
        return reconcile(definition, cycle, milestones, world)
    except NotYetImplemented as e:
        logger.info("%s: stopped — %s", cycle.key, e)
        return None
