"""Aggregating actions (ADR 0012)."""

import logging
from decimal import ROUND_HALF_UP, Decimal

from reconciler.actions import ActionContext, register_action
from reconciler.money import lines_from

logger = logging.getLogger("reconciler.actions.compute")

_CENTS = Decimal("0.01")


@register_action("compute.total")
def total(ctx: ActionContext) -> dict:
    """Sum every line every producer contributed.

    Deliberately generic: it collects lines from *all* recorded milestones rather than from
    named stages, so adding a third line producer needs no change here. That is the same
    reason the prior art summed by an `invoice_amount*` key prefix — but where a prefix scan
    silently merges two lines that happen to share a description, reading a list cannot.
    """
    lines = [line for milestone in ctx.milestones.values()
             for line in lines_from(milestone.payload)]

    amount = sum((line.amount for line in lines), Decimal(0))
    rounded = amount.quantize(_CENTS, rounding=ROUND_HALF_UP)
    logger.info("cycle %s totals %s over %d line(s)", ctx.cycle.key, rounded, len(lines))

    return {
        "total": str(rounded),
        "currency": ctx.config.get("currency", "EUR"),
        "line_count": len(lines),
        # Echo the lines the total was computed from, so the milestone is self-contained
        # evidence rather than a number whose provenance must be reassembled by a reader.
        # Under `summed_lines`, NOT `lines`: this payload is itself a milestone, and a second
        # aggregator scanning for `lines` would count every line twice.
        "summed_lines": [line.as_payload() for line in lines],
    }
