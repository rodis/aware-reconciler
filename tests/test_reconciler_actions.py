"""Tests for the stage actions (ADR 0012).

Both money bugs in the prior art lived here — the LLM weekday count and the decimal parse —
so these are the assertions that say the rebuild is actually better, not merely different.
"""

from datetime import date

import pytest

from reconciler.actions import (
    ActionContext,
    Services,
    build_action,
    register_action,
    registered_actions,
)
from reconciler.actions.lines import business_days
from reconciler.core import Cycle, Milestone


def _cycle(**context):
    return Cycle(key="dh_invoice_2026_004", process="dreamhost_invoice",
                 user_id="u", opened_at=1000, context=context)


def _ctx(cycle=None, milestones=None, config=None, services=None):
    return ActionContext(
        cycle=cycle or _cycle(),
        milestones=milestones or {},
        config=config or {},
        services=services or Services(),
    )


class FakeExtras:
    def __init__(self, rows):
        self.rows = rows

    def lines_for(self, cycle):
        return self.rows


# --- the registry ------------------------------------------------------------------------

def test_the_builtin_actions_are_registered():
    assert {"lines.worked_days", "lines.manual", "compute.total"} <= set(registered_actions())


def test_an_unknown_action_names_what_is_available():
    with pytest.raises(KeyError, match="registered:"):
        build_action("lines.invented")


def test_registering_a_duplicate_is_refused():
    with pytest.raises(ValueError, match="already registered"):
        register_action("compute.total")(lambda ctx: {})


# --- business_days: arithmetic, not an LLM -----------------------------------------------

@pytest.mark.parametrize("year,month,day,expected", [
    (2026, 4, 30, 22),    # April 2026 — starts Wednesday
    (2026, 3, 31, 22),    # March 2026 — starts Sunday
    (2026, 2, 28, 20),    # February 2026 — starts Sunday
    (2026, 8, 31, 21),    # August 2026 — starts Saturday
])
def test_weekday_counts_for_whole_months(year, month, day, expected):
    assert business_days(date(year, month, 1), date(year, month, day)) == expected


def test_a_single_weekend_day_counts_nothing():
    assert business_days(date(2026, 4, 4), date(2026, 4, 4)) == 0


def test_a_single_weekday_counts_one():
    assert business_days(date(2026, 4, 6), date(2026, 4, 6)) == 1


def test_an_inverted_range_is_zero_not_negative():
    assert business_days(date(2026, 4, 10), date(2026, 4, 1)) == 0


# --- lines.worked_days --------------------------------------------------------------------

def test_a_bonus_cycle_produces_no_worked_days_line():
    """The Christmas bonus: no worked period, so no line — and nothing branches on it."""
    result = build_action("lines.worked_days")(_ctx(config={"day_rate": 800}))
    assert result == {"lines": []}


def test_a_monthly_cycle_produces_one_priced_line():
    cycle = _cycle(worked_period={"start": "2026-04-01", "end": "2026-04-30"})
    result = build_action("lines.worked_days")(
        _ctx(cycle=cycle, config={"day_rate": 800, "hours_per_day": 8})
    )

    assert result["days"] == 22
    assert result["hours"] == 176
    (line,) = result["lines"]
    assert line["amount"] == "17600"          # 22 x 800, exact
    assert line["kind"] == "worked_days"
    assert "April 2026" in line["description"]


def test_the_day_rate_is_required_never_defaulted():
    # A silently-defaulted rate would produce a plausible, wrong invoice.
    cycle = _cycle(worked_period={"start": "2026-04-01", "end": "2026-04-30"})
    with pytest.raises(KeyError):
        build_action("lines.worked_days")(_ctx(cycle=cycle))


# --- lines.manual -------------------------------------------------------------------------

def test_no_extras_source_means_no_lines():
    assert build_action("lines.manual")(_ctx()) == {"lines": []}


def test_an_empty_extras_source_means_no_lines():
    ctx = _ctx(services=Services(extras=FakeExtras([])))
    assert build_action("lines.manual")(ctx) == {"lines": []}


def test_the_live_coursera_row_becomes_a_line():
    rows = [{"description": "Coursera Annual sbscription", "amount": 239.4}]
    ctx = _ctx(services=Services(extras=FakeExtras(rows)))

    (line,) = build_action("lines.manual")(ctx)["lines"]

    assert line == {"description": "Coursera Annual sbscription",
                    "amount": "239.4", "kind": "manual"}


def test_two_rows_with_the_same_description_both_survive():
    """The prior art wrote `invoice_amount_extra_<description>` as a Redis key, so two rows
    described "travel" silently overwrote each other — and the prefix-sum never noticed."""
    rows = [{"description": "travel", "amount": "40.00"},
            {"description": "travel", "amount": "60.00"}]
    ctx = _ctx(services=Services(extras=FakeExtras(rows)))

    lines = build_action("lines.manual")(ctx)["lines"]

    assert [line["amount"] for line in lines] == ["40.00", "60.00"]


def test_a_description_containing_a_dot_needs_no_special_handling():
    # `dotNotation: false` was a workaround for descriptions becoming nested object keys.
    rows = [{"description": "AWS bill (eu-west-1.prod)", "amount": "12.00"}]
    ctx = _ctx(services=Services(extras=FakeExtras(rows)))
    (line,) = build_action("lines.manual")(ctx)["lines"]
    assert line["description"] == "AWS bill (eu-west-1.prod)"


# --- compute.total ------------------------------------------------------------------------

def _milestone(stage, lines):
    return Milestone(stage=stage, timestamp=1, payload={"lines": lines})


def test_the_total_sums_lines_from_every_producer():
    milestones = {
        "computed_lines": _milestone("computed_lines", [
            {"description": "Consulting", "amount": "18400", "kind": "worked_days"}]),
        "manual_lines": _milestone("manual_lines", [
            {"description": "Coursera", "amount": "239.4", "kind": "manual"},
            {"description": "Travel", "amount": "340.00", "kind": "manual"}]),
    }

    result = build_action("compute.total")(_ctx(milestones=milestones))

    assert result["total"] == "18979.40"
    assert result["line_count"] == 3
    assert result["currency"] == "EUR"


def test_a_bonus_invoice_totals_its_single_line():
    milestones = {
        "computed_lines": _milestone("computed_lines", []),
        "manual_lines": _milestone("manual_lines", [
            {"description": "Christmas bonus", "amount": "2000", "kind": "manual"}]),
    }
    result = build_action("compute.total")(_ctx(milestones=milestones))
    assert result["total"] == "2000.00"
    assert result["line_count"] == 1


def test_an_invoice_with_no_lines_totals_zero():
    result = build_action("compute.total")(_ctx(milestones={}))
    assert result["total"] == "0.00"
    assert result["line_count"] == 0


def test_grouped_amounts_are_not_truncated_by_the_total():
    """End-to-end guard on the €1,234.50 → €1.23 bug."""
    milestones = {"manual_lines": _milestone("manual_lines", [
        {"description": "Conference", "amount": "1,234.50", "kind": "manual"}])}

    result = build_action("compute.total")(_ctx(milestones=milestones))

    assert result["total"] == "1234.50"


def test_the_total_echoes_its_lines_without_inviting_double_counting():
    """`summed_lines`, not `lines` — the total is itself a milestone, and a second
    aggregator scanning for `lines` would count everything twice."""
    milestones = {"manual_lines": _milestone("manual_lines", [
        {"description": "A", "amount": "10.00", "kind": "manual"}])}

    result = build_action("compute.total")(_ctx(milestones=milestones))

    assert "lines" not in result
    assert len(result["summed_lines"]) == 1

    # Re-summing a set of milestones that already contains a total must not change it.
    milestones["total_computed"] = Milestone("total_computed", 2, result)
    again = build_action("compute.total")(_ctx(milestones=milestones))
    assert again["total"] == "10.00"
