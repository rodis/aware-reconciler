"""Sender purge: trash mail from listed senders once it has had its time.

The rule, as a desired state rather than a schedule (owner, 2026-11-03):

    cutoff = min(midnight of today in `tz`, now - min_age)
    trash mail from a listed sender received before the cutoff

So a mail stays for the rest of the day it arrived AND for at least `min_age` (12 hours by
default), and goes once both have passed. Arriving 08:00 -> gone just after midnight; arriving
23:50 -> gone just after 11:50 the next day.

**Why no schedule.** The first phrasing was "every day at 7 am", and Kopf has no cron. Stated as
a desired state the cutoff is a pure function of the clock, so the controller is a plain timer:
a missed pass is caught by the next one, a doubled pass finds nothing, and nothing has to
remember when it last ran.

**Every guard only ever KEEPS more mail.** Gmail's search does the selection (`from:` plus
`before:<epoch>`), and then two checks here refuse anything the search let through that the rule
does not cover:

- the sender must equal a listed address EXACTLY — Gmail's `from:` also matches display names
  and substrings;
- the mail's own time must be before the cutoff too — `before:` uses Gmail's received time, the
  `Date` header is the sender's, and the purge acts only when both agree it is old.

Like `tasks.sweep`, this takes its ports injected and returns a plan, so the controller only
decides when to run it and where to report.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, tzinfo
from typing import Protocol

logger = logging.getLogger("reconciler.purge")

DEFAULT_MIN_AGE = timedelta(hours=12)
# How far back the search reaches. The query relay requires a lower bound; ten years is
# "everything" for a newsletter sender while keeping the request well-formed.
SEARCH_HORIZON = timedelta(days=3650)


def cutoff(now: datetime, tz: tzinfo, min_age: timedelta = DEFAULT_MIN_AGE) -> datetime:
    """The newest arrival time that may be trashed at `now`.

    `now` must be timezone-aware. Midnight is taken in `tz` (Europe/Zurich), which DST cannot
    break: the switch happens at 02:00/03:00, so midnight always exists exactly once. The 12 hours
    are ELAPSED hours, not wall-clock ones.
    """
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    local = now.astimezone(tz)
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
    # In UTC, ALWAYS. Python subtracts a timedelta from an aware datetime in WALL-CLOCK time when
    # the zone stays the same, so `zurich_now - 12h` on a DST day is 11 or 13 real hours. Caught
    # by the DST tests (2026-11-03): the spring case would have trashed mail after 11 hours.
    return min(midnight, now.astimezone(UTC) - min_age)


class MailSource(Protocol):
    """`adapters.gmail.N8nGmailQuery` — the same client the sweep and the invoice use."""

    def candidates(self, signal: dict, since: int) -> list[tuple[int, dict]]:
        ...


class Trasher(Protocol):
    """`adapters.trash.N8nTrashRelay`."""

    def trash(self, *, message_id: str) -> None:
        ...


@dataclass
class PurgePlan:
    """What a pass found and did. Returned rather than performed, so it can be printed."""

    cutoff: datetime
    to_trash: list[dict] = field(default_factory=list)
    # Matched by Gmail's search but refused by a guard above — worth seeing, never acted on.
    refused: list[dict] = field(default_factory=list)
    # True when more matched than `max_per_run` allowed; the next pass continues.
    truncated: bool = False
    trashed: int = 0


def purge(*, source: MailSource, trasher: Trasher, senders: list[str], now: datetime,
          tz: tzinfo, min_age: timedelta = DEFAULT_MIN_AGE, max_per_run: int = 100,
          dry_run: bool = True) -> PurgePlan:
    """One pass: search per sender, guard, and — unless `dry_run` — trash up to `max_per_run`."""
    edge = cutoff(now, tz, min_age)
    edge_epoch = int(edge.timestamp())
    since = int((edge - SEARCH_HORIZON).timestamp())
    listed = {s.strip().lower() for s in senders if s.strip()}

    plan = PurgePlan(cutoff=edge)
    seen: set[str] = set()
    for sender in sorted(listed):
        # One more than the cap, so "exactly the cap" and "more than the cap" can be told apart.
        found = source.candidates(
            {"q": f"from:{sender} before:{edge_epoch}", "limit": max_per_run + 1}, since)
        for when, mail in found:
            uid = mail.get("upstream_id")
            if not uid or uid in seen:
                continue
            seen.add(uid)
            exact = (mail.get("from") or "").strip().lower() in listed
            # `when` is 0 when the Date header is missing or unparseable — treated as old,
            # because Gmail's own `before:` has already vouched for it.
            old = when < edge_epoch
            (plan.to_trash if exact and old else plan.refused).append(mail)

    if len(plan.to_trash) > max_per_run:
        plan.to_trash = plan.to_trash[:max_per_run]
        plan.truncated = True

    logger.info("purge before %s: %d to trash, %d refused%s", edge.isoformat(),
                len(plan.to_trash), len(plan.refused), " (truncated)" if plan.truncated else "")
    if dry_run:
        return plan

    # Trash is idempotent, so a failure part-way leaves nothing to unwind: the next pass finds
    # whatever is left and trashes it. Raise rather than skip, so a broken relay is a Failed
    # status, not a pass that quietly trashed half and reported success.
    for mail in plan.to_trash:
        trasher.trash(message_id=mail["upstream_id"])
        plan.trashed += 1
    return plan
