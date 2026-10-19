"""Line-producing actions (ADR 0012).

Every producer here may contribute **zero** lines, and nothing downstream branches on that.
A regular month yields one computed line and zero-to-two manual ones; a Christmas bonus
yields no computed line and one manual one. Keeping absence ordinary is what stops the
definition language from growing conditionals (trip-wire 7).
"""

import logging
from datetime import date, timedelta

from reconciler.actions import ActionContext, register_action
from reconciler.money import Line, parse_amount

logger = logging.getLogger("reconciler.actions.lines")


def business_days(start: date, end: date) -> int:
    """Weekdays between `start` and `end`, both inclusive.

    The prior art asked Gemini 2.5 Flash *"Excluding Saturdays and Sundays, how many days
    there are in the month of April 2026"* and multiplied the answer by the rate. That is
    arithmetic, and delegating it bought non-determinism, latency and a bill in exchange for
    an answer that could simply be wrong — while being the only reason the stage existed as
    a workflow rather than a line of code.

    Note this counts weekdays, not *worked* days: public holidays and leave are not modelled,
    exactly as they were not before. They belong in a manual line, where a human states them.
    """
    if end < start:
        return 0
    weeks, remainder = divmod((end - start).days + 1, 7)
    count = weeks * 5
    for offset in range(remainder):
        if (start + timedelta(days=weeks * 7 + offset)).weekday() < 5:
            count += 1
    return count


@register_action("lines.worked_days")
def worked_days(ctx: ActionContext) -> dict:
    """One line for the period worked — or none at all.

    The period comes from the cycle, not from the clock: a scheduled open stamps the month
    it covers, and a manual open (a bonus) simply does not, which is precisely how a bonus
    invoice ends up with no worked-days line without anything branching. The reference period
    is a property of *this line*, not of the invoice — which is why a bonus invoice, having
    no such line, needs no period at all.
    """
    period = ctx.cycle.context.get("worked_period")
    if not period:
        logger.info("cycle %s carries no worked period; contributing no lines", ctx.cycle.key)
        return {"lines": []}

    start = date.fromisoformat(period["start"])
    end = date.fromisoformat(period["end"])
    days = business_days(start, end)

    hours_per_day = int(ctx.config.get("hours_per_day", 8))
    day_rate = parse_amount(ctx.config["day_rate"])          # required — never a default
    hours = days * hours_per_day
    amount = day_rate * days

    line = Line(
        description=ctx.config.get(
            "description_template", "Consulting, {start:%B %Y} — {days} days ({hours}h)"
        ).format(start=start, end=end, days=days, hours=hours),
        amount=amount,
        kind="worked_days",
    )
    return {
        "lines": [line.as_payload()],
        "period": {"start": start.isoformat(), "end": end.isoformat()},
        "days": days,
        "hours": hours,
    }


@register_action("lines.manual")
def manual(ctx: ActionContext) -> dict:
    """Expense and bonus lines a human entered for this cycle.

    Runs *after* approval on purpose: the approval wait is the window in which a human is
    looking at the numbers, whereas the prior art read the table seconds after the run
    started — so it had to be filled in before you had seen the draft.

    A missing source, or an empty one, yields no lines. That graceful absence is the one
    thing the prior art got right and is preserved deliberately: most months have nothing to
    add, and a process that blocks waiting to be told "nothing this month" is worse than
    useless.
    """
    source = ctx.services.extras
    if source is None:
        logger.info("no extras source configured; contributing no lines")
        return {"lines": []}

    lines = []
    for raw in source.lines_for(ctx.cycle):
        # Descriptions stay descriptions. The prior art wrote them into Redis keys
        # (`invoice_amount_extra_<description>`), so two rows described "travel" silently
        # overwrote each other — and the total, summing by key prefix, never noticed.
        lines.append(
            Line(description=str(raw["description"]),
                 amount=parse_amount(raw["amount"]),
                 kind="manual").as_payload()
        )
    logger.info("cycle %s contributed %d manual line(s)", ctx.cycle.key, len(lines))
    return {"lines": lines}
