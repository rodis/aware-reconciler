"""When a scheduled process owes a cycle — as a desired state, not a cron trigger.

A definition's `opens: [{via: schedule, cron: "0 9 1 * *"}]` used to mean "Prefect fires this at
09:00 on the 1st". Prefect is gone and Kopf has no cron, so the same intent is stated as a fact
that should hold at every moment (owner, 2026-11-03):

    once this month's firing time has passed, a cycle exists for the previous month's period

**Idempotent by period, not by key.** A pass checks whether ANY cycle — open, complete or voided —
already carries that worked period, and only opens one if none does. So a missed pass opens late
instead of never, a repeated pass opens nothing, and a voided cycle is never silently re-opened: a
re-run after a void is a deliberate, manual act (ADR 0012).

**Only the most recent month.** The rule looks at the month that just ended and nothing older, so
deploying it can never back-fill a history of invoices.

Only the cron shape the invoice uses is supported — `M H D * *`, a fixed day of every month — and
anything else is a configuration error rather than a guess. A second process on another cadence
extends this function; it does not get a general cron parser by default.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, tzinfo


class UnsupportedSchedule(ValueError):
    """A schedule this module will not interpret."""


def monthly_firing(cron: str) -> tuple[int, int, int]:
    """`"M H D * *"` -> (day, hour, minute). Anything else raises."""
    fields = cron.split()
    if len(fields) != 5 or fields[3:] != ["*", "*"] or not all(f.isdigit() for f in fields[:3]):
        raise UnsupportedSchedule(f"only 'M H D * *' monthly crons are supported, got {cron!r}")
    minute, hour, day = (int(f) for f in fields[:3])
    if not (0 <= minute < 60 and 0 <= hour < 24 and 1 <= day <= 28):
        raise UnsupportedSchedule(f"out of range (day must be 1-28 to exist every month): {cron!r}")
    return day, hour, minute


def previous_month(today: date) -> dict[str, str]:
    """The calendar month before `today`, as a worked period (same as `app.previous_month`)."""
    end = today.replace(day=1) - timedelta(days=1)
    return {"start": end.replace(day=1).isoformat(), "end": end.isoformat()}


def due_period(cron: str, now: datetime, tz: tzinfo) -> dict[str, str] | None:
    """The worked period a cycle is owed for at `now`, or None before this month's firing time.

    `now` must be timezone-aware; the firing time is wall-clock in `tz` (09:00 Zurich stays 09:00
    across DST, as Prefect's schedule did).
    """
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    day, hour, minute = monthly_firing(cron)
    local = now.astimezone(tz)
    firing = local.replace(day=day, hour=hour, minute=minute, second=0, microsecond=0)
    if local < firing:
        return None
    return previous_month(local.date())


def scheduled_cron(opens) -> str | None:
    """The definition's scheduled opener (`definition.opens`, `Opens` models), if it has one."""
    crons = [o.cron for o in opens or [] if o.via == "schedule" and o.cron]
    if len(crons) > 1:
        raise UnsupportedSchedule(f"more than one scheduled opener: {crons}")
    return crons[0] if crons else None


def has_cycle_for(cycles, period: dict[str, str]) -> bool:
    """Whether any recorded cycle — whatever its state — already covers `period`."""
    return any((c.context or {}).get("worked_period") == period for c in cycles)
