"""Email todo tasks: two events, and an hourly sweep that keeps them honest.

A task is a mail you labelled. It exists as **two raw events**, never as a mutable row:

    email_labeled_todo   the task opened   (upstream_id, from, subject, ...)
    email_task_closed    the task closed   (the same upstream_id, closed_via)

The open list is then an anti-join — labelled messages with no close for their `upstream_id` —
which is the same move the process board makes over `cycle_key`, and it needs no engine and no
session pairing. That matters: `session_window` holds exactly one open slot per (user,
definition), so it structurally cannot track a dozen concurrent tasks; ADR 0012 refused it for
process cycles for the same reason.

**Why this is a sweep and not a process.** `processes/*.yml` describes a cycle with stages and
waits, and the reconciler advances it. A task has no stages — it has two states. Modelling one
task as a process would mint a cycle per email and hand eleven-stage machinery to a two-state
thing. What the two jobs *share* is the tier's actual idea: run on a schedule, be a pure
function of the facts already recorded, and therefore be safe to re-run. This is that idea in
its second shape.

**Why the sweep is the authority, and the connector only an optimisation.** Gmail tells us a
label was *added* (the n8n Gmail Trigger polls for it, ~60s) and never that one was *removed*.
So closing cannot come from a trigger at all. The sweep asks the only question that has a
complete answer — *what is labelled right now?* — and derives both directions from it:

    labelled in Gmail, no open event   ->  emit email_labeled_todo   (a trigger we missed)
    open event, not labelled in Gmail  ->  emit email_task_closed    (done, however it was done)

That second line is what makes unlabelling on your phone work, and the first is a real repair:
the Gmail Trigger node is documented to drop messages, and `connectors/n8n/gmail-query`'s own
header records why a polling connector could not be trusted as the only witness. Being a pure
function of "now" versus "recorded" means the sweep cannot drift — there is no cursor to lose.

**The sweep is the SOLE producer of `email_task_closed`** (since 2026-10-13). The dashboard's
tick used to emit one too, immediately, with the sweep agreeing afterwards — safe while both
read the same Neon log. It stopped being safe when this sweep's store of record moved to MySQL,
which the dashboard cannot reach: a close emitted there would never land in the store the sweep
reads, so the next sweep would find the task open-and-unlabelled and close it a second time,
every time. The tick now only removes the label, which *is* the close as far as this file is
concerned — unlabelling from the board and unlabelling from the Gmail app became the same act,
and there is nothing left for two producers to disagree about. The cost is latency: a tick shows
up on the Aware timeline on the next sweep rather than at once, which is what the interval buys.

Historical rows still carry `closed_via: "dashboard"`; nothing emits it any more.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

logger = logging.getLogger("reconciler.tasks")

# The Gmail label that makes a mail a task. One label, and applying it IS the decision — no
# classifier, no interpretation, the pattern already proven by `aware/invoice-approved`.
DEFAULT_LABEL = "aware/todo"

OPENED_EVENT = "email_labeled_todo"
CLOSED_EVENT = "email_task_closed"

# How far back a sweep looks. A task can sit for months (an insurance renewal, a passport
# appointment), so this is a *task lifetime* rather than a freshness window — but it is bounded
# rather than unlimited, because the answer has to fit in one relay response.
#
# The consequence to know: a mail older than this that is still labelled looks to the sweep like
# "not labelled", and would be closed. That is why the diff below refuses to close anything it
# did not look far enough back to see (see `stale_horizon`).
DEFAULT_LOOKBACK_DAYS = 365

# Bounds one relay answer. The Gmail node pages, but a single response has to stay under the
# ~1 MiB ingress ceiling once each item carries a 1000-char snippet.
DEFAULT_LIMIT = 200

# The `<app>` in `/sensors/<app>`, and therefore the `source_app` column on every task event.
# A distinct app rather than reusing `gmail`, because it is the discriminator the dashboard and
# the timeline filter on — the same role `process` plays for milestones. `route_by_app.yml`
# sends anything that is not `overland` to the standard adapter, so this needs no Vector change.
TASK_INGEST_APP = "tasks"

# How a close came about. Only one value is ever written now — the sweep is the sole producer
# (see the header) — but it stays on the wire because recorded rows carry it and because a
# second producer, if one is ever justified again, must be nameable rather than anonymous.
CLOSED_VIA_SWEEP = "sweep"


@dataclass(frozen=True)
class OpenTask:
    """A task the events say is open. Enough to close it and to read the close afterwards."""

    upstream_id: str
    subject: str = ""
    from_name: str = ""
    from_address: str = ""
    opened_at: int = 0


@dataclass
class SweepPlan:
    """What a sweep would do. Returned rather than performed, so it can be printed."""

    to_open: list[dict] = field(default_factory=list)
    to_close: list[OpenTask] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not self.to_open and not self.to_close


class OpenTaskStore(Protocol):
    """Where the currently-open tasks are read from (Neon, in production)."""

    def open_tasks(self, user_id: str, label: str) -> dict[str, OpenTask]:
        ...


class EventSink(Protocol):
    def emit(self, payload: dict) -> None:
        ...


def opened_body(candidate: dict, *, user_id: str, label: str, when: int) -> dict:
    """The wire body for a task opening.

    **Field-for-field identical to what the n8n connector emits**, and that is a requirement,
    not tidiness: the same event name is produced by two producers, so a consumer that could
    tell them apart would be reading one of them wrong. `closed_via` is absent here by
    design — an open has no such notion, and inventing `opened_via` would invite a consumer to
    branch on which producer won a race.
    """
    return {
        "event_name": OPENED_EVENT,
        "user_id": user_id,
        # The mail's own `Date` header, so a task's age is how long the MAIL has been sitting,
        # not how long since we noticed it. The whole point of the board is spotting what is
        # rotting; stamping discovery time would reset every age to zero on a repair sweep.
        "timestamp": when,
        "label": label,
        "upstream_id": candidate["upstream_id"],
        "gmail_thread_id": candidate.get("gmail_thread_id"),
        "from": candidate.get("from", ""),
        "from_name": candidate.get("from_name", ""),
        "from_domain": candidate.get("from_domain", ""),
        "subject": candidate.get("subject", ""),
        "snippet": candidate.get("snippet", ""),
    }


def closed_body(task: OpenTask, *, user_id: str, label: str, when: int,
                closed_via: str) -> dict:
    """The wire body for a task closing.

    Carries `subject` even though `upstream_id` already identifies the task, because the close
    lands on the Day timeline as its own row — and a row reading "Email task closed" with no
    subject would force a join to be legible at all. Denormalising one string is cheaper than
    making the timeline query smarter.
    """
    return {
        "event_name": CLOSED_EVENT,
        "user_id": user_id,
        # Now, not the mail's date: the fact being recorded is *you finishing it*, which
        # happened today. This is the one place the two events legitimately disagree about
        # whose clock matters.
        "timestamp": when,
        "label": label,
        "upstream_id": task.upstream_id,
        "closed_via": closed_via,
        "subject": task.subject,
        "from_name": task.from_name,
        "open_seconds": max(0, when - task.opened_at) if task.opened_at else None,
    }


def diff(labelled: dict[str, dict], open_tasks: dict[str, OpenTask],
         *, stale_horizon: int = 0) -> SweepPlan:
    """Compare what Gmail says now with what the events say. Pure.

    `labelled` is keyed by `upstream_id`; `open_tasks` likewise. Both directions fall out of
    set arithmetic, which is the reason this job needs no state of its own.

    `stale_horizon` is the epoch second the Gmail query looked back to, and it exists to stop
    the one destructive failure this function can have. The search is bounded
    (`DEFAULT_LOOKBACK_DAYS`), so a task older than the window is simply *not in the answer* —
    indistinguishable from unlabelled. Closing on that basis would silently retire every
    long-lived task the moment the lookback was shortened, or the moment one aged out. So a task
    that opened before the horizon is left alone: absence of evidence, not evidence of absence.
    Pass 0 to disable the guard (only correct when the query was genuinely unbounded).
    """
    plan = SweepPlan()

    for upstream_id, candidate in labelled.items():
        if upstream_id not in open_tasks:
            plan.to_open.append(candidate)

    for upstream_id, task in open_tasks.items():
        if upstream_id in labelled:
            continue
        if stale_horizon and task.opened_at and task.opened_at < stale_horizon:
            logger.info("leaving %r open: it predates the search horizon, so Gmail's silence "
                        "about it means nothing", task.subject[:60])
            continue
        plan.to_close.append(task)

    return plan


def sweep(*, source, store: OpenTaskStore, sink: EventSink, user_id: str,
          label: str = DEFAULT_LABEL, lookback_days: int = DEFAULT_LOOKBACK_DAYS,
          limit: int = DEFAULT_LIMIT, now: int | None = None) -> SweepPlan:
    """Ask Gmail what is labelled, compare with what is recorded, emit the difference.

    `source` is a `finder.CandidateSource` — the same `N8nGmailQuery` the invoice's gates use,
    reused rather than reimplemented, so there is one Gmail client in the tier and one place
    where mailparser's awkward `from` object gets flattened.
    """
    now = now or int(datetime.now(UTC).timestamp())
    horizon = now - lookback_days * 86400

    found = source.candidates({"label": label, "limit": limit}, horizon)
    labelled = {c["upstream_id"]: c for _, c in found if c.get("upstream_id")}
    open_tasks = store.open_tasks(user_id, label)

    plan = diff(labelled, open_tasks, stale_horizon=horizon)
    logger.info("sweep %s: %d labelled in gmail, %d open in aware -> %d to open, %d to close",
                label, len(labelled), len(open_tasks), len(plan.to_open), len(plan.to_close))

    # Opens are emitted with the MAIL's time; a repair sweep therefore backdates the task to
    # when the mail arrived rather than to now.
    for candidate in plan.to_open:
        when = next((t for t, c in found if c["upstream_id"] == candidate["upstream_id"]), now)
        sink.emit(opened_body(candidate, user_id=user_id, label=label, when=when or now))

    for task in plan.to_close:
        sink.emit(closed_body(task, user_id=user_id, label=label, when=now,
                              closed_via=CLOSED_VIA_SWEEP))

    if plan.empty:
        logger.info("sweep %s: nothing to do", label)
    return plan
