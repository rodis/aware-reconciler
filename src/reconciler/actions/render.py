"""Rendering the invoice PDF (ADR 0012).

The payload the template expects is built here and unit-tested; the HTTP call lives in
`reconciler.adapters.craftmypdf`. Same split as the approval mail, for the same reason — the
exact JSON a cycle would send can be inspected without sending anything.

CraftMyPDF is called **directly**, not through n8n. Its n8n node is a community plugin, and
the cluster has no persistence, so the plugin is lost on every pod restart. Calling the REST
API needs only an API key — no OAuth, no token refresh — so nothing here needs n8n's
credential store the way Gmail and SMTP do. It also keeps PDF rendering off n8n's egress path,
which is the currently-unreliable one (backlog #70).
"""

import logging
from datetime import UTC, date, datetime
from decimal import Decimal

from reconciler.actions import ActionContext, register_action
from reconciler.money import lines_from, parse_amount

logger = logging.getLogger("reconciler.actions.render")

# "Thursday, 16 July 2026". The template prints this verbatim.
DEFAULT_DATE_FORMAT = "%A, %-d %B %Y"
DEFAULT_MONTH_FORMAT = "%B %Y"

# The services are an export of services and exempt from Swiss VAT — the template says so in
# prose. The line is still required as a zero, so it is appended HERE rather than carried as a
# process line: a zero-value line would inflate line_count, show a spurious VAT row in the
# approval mail, and imply the process models tax. It does not.
VAT_ITEM_NAME = "VAT"

# The computed line prints as a bare "Amount". Its full description ("Consulting, July 2026 —
# 23 days") would only repeat what the template already states in prose, and the hours and rate
# travel as their own fields. Manual lines keep their own descriptions, which is the whole
# point of them.
DEFAULT_WORKED_ITEM_NAME = "Amount"

# Money crosses the wire as a FORMATTED STRING, not a number. JSON numbers drop trailing
# zeros, so 18400.00 printed as "18400", 239.40 as "239.4" and 0.00 as "0" — three different
# shapes in one column of an invoice a client reads. Formatting here also means the Decimal
# never becomes a float at all, so the exactness kept all the way from `parse_amount` survives
# to the page.
MONEY_FORMAT = "{:,.2f}"


def money_str(amount: Decimal, fmt: str = MONEY_FORMAT) -> str:
    return fmt.format(amount)


def _cycle_year(ctx: ActionContext) -> int:
    period = ctx.cycle.context.get("worked_period")
    if period:
        return date.fromisoformat(period["start"]).year
    return datetime.fromtimestamp(ctx.cycle.opened_at, UTC).year


def _invoice_date(ctx: ActionContext) -> date:
    """The date printed on the invoice.

    The last day of the worked period, not the day it happens to be rendered: a monthly
    services invoice is dated to the work, and a cycle re-run weeks later must not print a
    different date than the one it replaced. A cycle with no worked period (a bonus) has no
    such day, so it falls back to when the cycle was opened.
    """
    period = ctx.cycle.context.get("worked_period")
    if period:
        return date.fromisoformat(period["end"])
    return datetime.fromtimestamp(ctx.cycle.opened_at, UTC).date()


def build_invoice_data(ctx: ActionContext) -> dict:
    """The `data` object sent to the template.

    `items` is dynamic: the computed line first, then every manual line (bonus, expense), then
    VAT as a mandatory zero. Adding a third kind of line producer needs no change here.
    """
    config = ctx.config
    currency = config.get("currency", "USD")
    number = f"{int(ctx.cycle.context['invoice_number']):02d}-{_cycle_year(ctx)}"

    lines, worked_hours = [], 0
    for milestone in ctx.milestones.values():
        # Skip the aggregate: compute.total echoes its inputs under `summed_lines`, but a
        # milestone that also carried `lines` would otherwise be counted twice.
        lines.extend(lines_from(milestone.payload))
        if "hours" in milestone.payload:
            worked_hours = milestone.payload["hours"]

    worked_name = config.get("worked_item_name", DEFAULT_WORKED_ITEM_NAME)
    fmt = config.get("money_format", MONEY_FORMAT)
    items = [{"name": worked_name if line.kind == "worked_days" else line.description,
              "currency": currency,
              "value": money_str(line.amount, fmt)}
             for line in lines]
    items.append({"name": VAT_ITEM_NAME, "currency": currency,
                  "value": money_str(Decimal(0), fmt)})

    grand_total = sum((line.amount for line in lines), Decimal(0))
    invoice_date = _invoice_date(ctx)
    period = ctx.cycle.context.get("worked_period")
    month_of = date.fromisoformat(period["start"]) if period else invoice_date

    rate = config.get("rate_per_hour")
    if rate is None and config.get("day_rate") and config.get("hours_per_day"):
        rate = parse_amount(config["day_rate"]) / int(config["hours_per_day"])

    return {
        "invoice_number": number,
        "invoice_date": invoice_date.strftime(config.get("date_format", DEFAULT_DATE_FORMAT)),
        "month": month_of.strftime(config.get("month_format", DEFAULT_MONTH_FORMAT)),
        # A count, not money — stays a number.
        "worked_hours": worked_hours,
        "rate_per_hour": money_str(Decimal(str(rate)), fmt) if rate is not None else None,
        "items": items,
        "grand_total": money_str(grand_total, fmt),
    }


@register_action("craftmypdf.render")
def render(ctx: ActionContext) -> dict:
    """Render the invoice and record where the PDF landed."""
    renderer = ctx.services.pdf
    if renderer is None:
        raise RuntimeError("craftmypdf.render needs a pdf renderer; none configured.")

    template_id = ctx.config["template_id"]      # required — never guess which template
    data = build_invoice_data(ctx)
    result = renderer.render(template_id=template_id, data=data)

    logger.info("cycle %s rendered invoice %s -> %s",
                ctx.cycle.key, data["invoice_number"], result.get("file"))
    # Keep the payload alongside the URL: a PDF whose numbers are disputed later is only
    # explicable if what was sent is recorded next to what came back.
    return {"invoice_number": data["invoice_number"], "sent": data, **result}
