"""`reconciler.schedule` — when a scheduled process owes a cycle, with no store and no cluster."""

from datetime import UTC, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from reconciler import schedule

ZURICH = ZoneInfo("Europe/Zurich")
CRON = "0 9 1 * *"                       # the invoice: 09:00 on the 1st


def at(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=ZURICH)


@pytest.mark.parametrize("now,expected", [
    ("2026-11-01 08:59", None),                                               # not yet
    ("2026-11-01 09:00", {"start": "2026-10-01", "end": "2026-10-31"}),       # exactly due
    ("2026-11-03 14:00", {"start": "2026-10-01", "end": "2026-10-31"}),       # still owed later
    ("2026-11-30 23:59", {"start": "2026-10-01", "end": "2026-10-31"}),
    ("2026-12-01 09:30", {"start": "2026-11-01", "end": "2026-11-30"}),       # the next month
    ("2027-01-01 10:00", {"start": "2026-12-01", "end": "2026-12-31"}),       # across the year
    ("2026-03-01 09:00", {"start": "2026-02-01", "end": "2026-02-28"}),       # short February
])
def test_the_owed_period_is_the_month_that_just_ended(now, expected):
    assert schedule.due_period(CRON, at(now), ZURICH) == expected


def test_the_firing_time_is_zurich_wall_clock_in_both_dst_halves():
    """09:00 Zurich is 08:00 UTC in winter and 07:00 UTC in summer; the opener follows the wall
    clock, as Prefect's Europe/Zurich schedule did."""
    assert schedule.due_period(CRON, datetime(2026, 11, 1, 7, 59, tzinfo=UTC), ZURICH) is None
    assert schedule.due_period(CRON, datetime(2026, 11, 1, 8, 0, tzinfo=UTC), ZURICH)
    assert schedule.due_period(CRON, datetime(2026, 7, 1, 6, 59, tzinfo=UTC), ZURICH) is None
    assert schedule.due_period(CRON, datetime(2026, 7, 1, 7, 0, tzinfo=UTC), ZURICH)


@pytest.mark.parametrize("cron", ["0 9 * * *", "0 9 1 1 *", "*/5 * * * *", "0 9 31 * *", "x"])
def test_anything_but_a_fixed_monthly_day_is_refused(cron):
    """Day 31 is refused too: it does not exist every month, so it would skip some."""
    with pytest.raises(schedule.UnsupportedSchedule):
        schedule.monthly_firing(cron)


def test_a_naive_now_is_refused():
    with pytest.raises(ValueError):
        schedule.due_period(CRON, datetime(2026, 11, 1, 9, 0), ZURICH)


def cycle(period=None, key="k"):
    return SimpleNamespace(key=key, context={"worked_period": period} if period else {})


OCTOBER = {"start": "2026-10-01", "end": "2026-10-31"}


def test_any_cycle_for_the_period_counts_whatever_its_state():
    """Open, complete or VOIDED: a re-run after a void is a deliberate manual act, never an
    automatic re-open."""
    assert schedule.has_cycle_for([cycle(OCTOBER)], OCTOBER)
    assert not schedule.has_cycle_for([cycle({"start": "2026-09-01", "end": "2026-09-30"})], OCTOBER)


def test_a_bonus_cycle_with_no_worked_period_does_not_satisfy_the_month():
    assert not schedule.has_cycle_for([cycle(None)], OCTOBER)


def test_the_scheduled_cron_is_read_from_the_definition():
    from reconciler.app import DEFAULT_PROCESSES_DIR, definition_named

    definition = definition_named("dreamhost_invoice", DEFAULT_PROCESSES_DIR)
    assert schedule.scheduled_cron(definition.opens) == CRON
