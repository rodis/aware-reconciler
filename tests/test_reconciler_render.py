"""Tests for the invoice PDF payload (ADR 0012).

The template's field names and formats are a contract with a document a human signs and sends,
so they are pinned exactly. Getting `invoice_number` or `currency` wrong produces a plausible,
wrong invoice — the worst kind.
"""

import json

import pytest

from reconciler.actions import ActionContext, Services, build_action
from reconciler.actions.render import build_invoice_data
from reconciler.adapters.craftmypdf import CraftMyPdf
from reconciler.core import Cycle, Milestone

CONFIG = {"template_id": "tmpl_example", "currency": "USD", "rate_per_hour": 100,
          "worked_item_name": "Amount"}


def _cycle(**context):
    base = {"invoice_number": 8,
            "worked_period": {"start": "2026-07-01", "end": "2026-07-31"}}
    base.update(context)
    return Cycle(key="dh_invoice_2026_008", process="dreamhost_invoice", user_id="rods",
                 opened_at=1786883655, context=base)


def _ctx(cycle=None, lines=None, hours=184, services=None, config=None):
    payload = {"lines": lines if lines is not None else [
        {"description": "Consulting, July 2026", "amount": "18400", "kind": "worked_days"}]}
    if hours is not None:
        payload["hours"] = hours
    return ActionContext(cycle=cycle or _cycle(),
                         milestones={"computed_lines": Milestone("computed_lines", 1, payload)},
                         config=config or CONFIG,
                         services=services or Services())


# --- the fields the template prints ---------------------------------------------------------

def test_the_invoice_number_is_zero_padded_and_year_suffixed():
    """Sequence 8 prints as 08-2026. It LOOKS month-shaped, which is how the old design got
    confused — the sequence is per-year, and the format is presentation only."""
    assert build_invoice_data(_ctx())["invoice_number"] == "08-2026"


def test_the_invoice_is_dated_to_the_end_of_the_worked_period():
    """Not to the render day: a re-run weeks later must print the date it replaced."""
    assert build_invoice_data(_ctx())["invoice_date"] == "Friday, 31 July 2026"


def test_the_month_comes_from_the_period_not_the_render_day():
    assert build_invoice_data(_ctx())["month"] == "July 2026"


def test_hours_and_rate_travel_as_their_own_fields():
    data = build_invoice_data(_ctx())
    assert data["worked_hours"] == 184
    assert data["rate_per_hour"] == "100.00"


def test_the_rate_can_be_derived_from_the_day_rate():
    config = {**CONFIG}
    del config["rate_per_hour"]
    config.update({"day_rate": 800, "hours_per_day": 8})
    assert build_invoice_data(_ctx(config=config))["rate_per_hour"] == "100.00"


# --- items -----------------------------------------------------------------------------------

def test_the_computed_line_prints_as_a_bare_amount():
    # Its description would only repeat what the template already says in prose.
    items = build_invoice_data(_ctx())["items"]
    assert items[0] == {"name": "Amount", "currency": "USD", "value": "18,400.00"}


def test_manual_lines_keep_their_own_descriptions():
    items = build_invoice_data(_ctx(lines=[
        {"description": "Consulting, July 2026", "amount": "18400", "kind": "worked_days"},
        {"description": "Coursera Annual sbscription", "amount": "239.4", "kind": "manual"},
    ]))["items"]
    assert [i["name"] for i in items] == ["Amount", "Coursera Annual sbscription", "VAT"]


def test_vat_is_always_present_and_always_zero():
    """Exempt as an export of services, but the template requires the row."""
    for lines in ([], [{"description": "Bonus", "amount": "2000", "kind": "manual"}]):
        items = build_invoice_data(_ctx(lines=lines))["items"]
        assert items[-1] == {"name": "VAT", "currency": "USD", "value": "0.00"}


def test_vat_is_not_a_process_line_and_does_not_reach_the_total():
    """It is a rendering requirement, not a fact. A zero VAT *line* would inflate line_count
    and put a spurious row in the approval mail."""
    data = build_invoice_data(_ctx())
    assert data["grand_total"] == "18,400.00"
    assert data["items"][-1]["value"] == "0.00"     # VAT contributes nothing


def test_a_bonus_invoice_renders_with_no_worked_period():
    cycle = Cycle(key="dh_invoice_2026_009", process="dreamhost_invoice", user_id="rods",
                  opened_at=1786883655, context={"invoice_number": 9})
    data = build_invoice_data(_ctx(
        cycle=cycle, hours=None,
        lines=[{"description": "Christmas bonus", "amount": "2000", "kind": "manual"}]))

    assert data["invoice_number"] == "09-2026"
    assert data["worked_hours"] == 0
    assert [i["name"] for i in data["items"]] == ["Christmas bonus", "VAT"]
    assert data["invoice_date"].endswith("2026")     # falls back to the cycle open date


def test_money_crosses_the_wire_as_a_formatted_string():
    """JSON numbers drop trailing zeros, so 18400.00 printed as "18400", 239.40 as "239.4" and
    0.00 as "0" — three shapes in one column of a document a client reads. Formatting here also
    means the Decimal never becomes a float, so exactness survives to the page."""
    data = build_invoice_data(_ctx(lines=[
        {"description": "Conference", "amount": "1,234.50", "kind": "manual"},
        {"description": "Rounding", "amount": "0.1", "kind": "manual"}]))

    assert [i["value"] for i in data["items"]] == ["1,234.50", "0.10", "0.00"]
    assert data["grand_total"] == "1,234.60"
    assert json.dumps(data)          # must be serialisable — Decimal would not be


def test_hours_stay_a_number():
    # A count, not money. Formatting it would print "184.00" hours.
    assert build_invoice_data(_ctx())["worked_hours"] == 184


# --- the API adapter ---------------------------------------------------------------------------

class _Response:
    def __init__(self, payload):
        self._payload = json.dumps(payload).encode()

    def read(self):
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


LIVE_SHAPE = {"status": "success", "file": "https://s3/output.pdf?X-Amz-Expires=604800",
              "transaction_ref": "75e710a8", "total_pages": 1, "file_size": 69353,
              "template_id": "tmpl_example"}


def test_the_request_carries_a_seven_day_expiration(monkeypatch):
    """The API default is 300 SECONDS — a link dead before the approval mail is read."""
    import urllib.request
    seen = {}

    def fake(request, timeout=None):
        seen["body"] = json.loads(request.data)
        seen["headers"] = dict(request.headers)
        return _Response(LIVE_SHAPE)

    monkeypatch.setattr(urllib.request, "urlopen", fake)
    CraftMyPdf(api_key="k").render(template_id="t", data={"a": 1})

    assert seen["body"]["expiration"] == 10080          # minutes = 7 days
    assert seen["body"]["export_type"] == "json"
    assert seen["headers"]["X-api-key"] == "k"
    # Cloudflare 403s urllib's default UA with error code 1010 — see adapters.craftmypdf.
    assert "urllib" not in seen["headers"]["User-agent"]


def test_the_render_action_records_what_it_sent(monkeypatch):
    """The URL expires; the payload does not. A disputed invoice is only explicable if what
    was sent is stored beside what came back."""
    class FakePdf:
        def render(self, *, template_id, data):
            return LIVE_SHAPE

    result = build_action("craftmypdf.render")(_ctx(services=Services(pdf=FakePdf())))

    assert result["invoice_number"] == "08-2026"
    assert result["transaction_ref"] == "75e710a8"
    assert result["sent"]["grand_total"] == "18,400.00"


def test_a_missing_template_id_is_loud():
    class FakePdf:
        def render(self, **kw):
            raise AssertionError("must not be called")

    with pytest.raises(KeyError):
        build_action("craftmypdf.render")(
            _ctx(services=Services(pdf=FakePdf()), config={"currency": "USD"}))


def test_an_api_error_status_raises(monkeypatch):
    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen",
                        lambda r, timeout=None: _Response({"status": "error",
                                                           "message": "bad template"}))
    with pytest.raises(RuntimeError, match="refused"):
        CraftMyPdf(api_key="k").render(template_id="t", data={})


def test_an_http_error_is_never_retried(monkeypatch):
    """A response means the API answered and a render may already be billed. Only
    connect-level failures — where nothing arrived — are safe to repeat."""
    import urllib.error
    import urllib.request
    calls = {"n": 0}

    def refused(request, timeout=None):
        calls["n"] += 1
        raise urllib.error.HTTPError(request.full_url, 402, "Payment Required", {}, None)

    monkeypatch.setattr(urllib.request, "urlopen", refused)
    with pytest.raises(RuntimeError, match="402"):
        CraftMyPdf(api_key="k").render(template_id="t", data={})
    assert calls["n"] == 1


# --- the PDF must be reachable from the mail --------------------------------------------------

def test_the_ready_mail_carries_the_pdf_link():
    """The mail says "check the PDF" — so it has to say WHERE. Shipping that instruction with
    no link is worse than no instruction at all."""
    from reconciler.actions.notify import invoice_ready

    class Recorder:
        def __init__(self): self.sent = []
        def send(self, *, subject, html, text): self.sent.append((subject, html, text))

    rec = Recorder()
    ctx = ActionContext(
        cycle=_cycle(),
        milestones={
            "total_computed": Milestone("total_computed", 1, {"total": "18400.00"}),
            "invoice_generated": Milestone("invoice_generated", 2, {
                "invoice_number": "08-2026",
                "file": "https://s3.example/output.pdf?X-Amz-Expires=604800"}),
        },
        config={"currency": "USD"},
        services=Services(mailer=rec))

    result = invoice_ready(ctx)
    subject, html, text = rec.sent[0]

    assert subject == "Invoice 08-2026 — PDF ready to submit"
    assert "https://s3.example/output.pdf" in text
    assert "https://s3.example/output.pdf" in html
    assert "expires" in text.lower()          # the 7-day life is stated, not assumed
    assert result["pdf_url"].startswith("https://s3.example/")


def test_a_missing_pdf_link_is_visible_rather_than_silent():
    from reconciler.actions.notify import invoice_ready

    class Recorder:
        def __init__(self): self.sent = []
        def send(self, *, subject, html, text): self.sent.append((subject, html, text))

    rec = Recorder()
    invoice_ready(ActionContext(cycle=_cycle(),
                                milestones={"t": Milestone("t", 1, {"total": "1.00"})},
                                config={"currency": "USD"},
                                services=Services(mailer=rec)))
    _, html, text = rec.sent[0]
    assert "no PDF link" in text or "No PDF link" in html
