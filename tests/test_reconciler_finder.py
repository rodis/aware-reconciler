"""Tests for resolving `await` signals (ADR 0012).

Correlation is the whole risk here. A finder that matches too loosely lets one cycle's
approval close another's gate — and with a monthly invoice and an ad-hoc bonus able to be
open at once, that is not hypothetical.
"""

from reconciler.core import Cycle, Milestone
from reconciler.finder import NeonEvents, SignalFinder

APPROVED = "email_labeled_invoice_approved"


class FakeSignals:
    """Answers `signals(name, since)` from a scripted list of (ts, body)."""

    def __init__(self, events):
        self.events = events
        self.asked: list[tuple[str, int]] = []

    def signals(self, name, since):
        self.asked.append((name, since))
        return [(ts, body) for ts, body in self.events
                if body.get("name", name) == name and ts >= since]


def _finder(source):
    """A finder over recorded events — the matcher is shared with the Gmail source."""
    return SignalFinder(NeonEvents(source))


def _cycle(key="dh_invoice_2026_007"):
    return Cycle(key=key, process="dreamhost_invoice", user_id="rods", opened_at=1000)


def _requested(subject, ts=2000):
    return {"approval_requested": Milestone("approval_requested", ts, {"subject": subject})}


def _mail(ts, subject, **extra):
    return (ts, {"subject": subject, "label": "aware/invoice-approved", **extra})


# --- the happy path -----------------------------------------------------------------------

def test_a_labelled_mail_satisfies_the_gate():
    signals = FakeSignals([_mail(2100, "Invoice 7 — please check and approve")])
    finder = _finder(signals)

    found = finder.find(
        {"event": APPROVED, "correlate_on": "subject"},
        _cycle(), 2000, _requested("Invoice 7 — please check and approve"))

    assert found["matched_at"] == 2100
    assert found["evidence"]["label"] == "aware/invoice-approved"


def test_nothing_labelled_means_still_waiting():
    finder = _finder(FakeSignals([]))
    found = finder.find({"event": APPROVED, "correlate_on": "subject"},
                        _cycle(), 2000, _requested("Invoice 7"))
    assert found is None


# --- correlation keeps concurrent cycles apart ---------------------------------------------

def test_another_cycles_approval_does_not_close_this_gate():
    """A monthly invoice and a Christmas bonus can be open at once. The bonus being approved
    must not advance the monthly one."""
    signals = FakeSignals([_mail(2100, "Invoice 12 — please check and approve")])
    finder = _finder(signals)

    found = finder.find({"event": APPROVED, "correlate_on": "subject"},
                        _cycle(), 2000, _requested("Invoice 7 — please check and approve"))

    assert found is None


def test_the_right_mail_is_picked_out_of_several():
    signals = FakeSignals([
        _mail(2100, "Invoice 12 — please check and approve"),
        _mail(2200, "Invoice 7 — please check and approve"),
        _mail(2300, "Invoice 13 — please check and approve"),
    ])
    found = _finder(signals).find(
        {"event": APPROVED, "correlate_on": "subject"},
        _cycle(), 2000, _requested("Invoice 7 — please check and approve"))

    assert found["matched_at"] == 2200


def test_without_something_to_correlate_against_nothing_matches():
    """Fail closed. Matching on "any approval at all" is exactly the bug this guards."""
    signals = FakeSignals([_mail(2100, "Invoice 7")])
    found = _finder(signals).find(
        {"event": APPROVED, "correlate_on": "subject"}, _cycle(), 2000, {})
    assert found is None


def test_correlation_uses_the_most_recent_request():
    """A resent request must correlate against what it last said, not what it first said."""
    milestones = {
        "approval_requested": Milestone("approval_requested", 2000, {"subject": "Invoice 7 v1"}),
        "approval_resent": Milestone("approval_resent", 3000, {"subject": "Invoice 7 v2"}),
    }
    signals = FakeSignals([_mail(3100, "Invoice 7 v1"), _mail(3200, "Invoice 7 v2")])

    found = _finder(signals).find(
        {"event": APPROVED, "correlate_on": "subject"}, _cycle(), 3000, milestones)

    assert found["evidence"]["subject"] == "Invoice 7 v2"


# --- the time window ------------------------------------------------------------------------

def test_slack_lets_the_mail_we_sent_ourselves_match():
    """The connector's event time is the mail's `Date` header — roughly when the request went
    out, not when the label was applied. A strict `>= since` would never match it."""
    signals = FakeSignals([_mail(1995, "Invoice 7")])   # 5s BEFORE the milestone

    found = _finder(signals).find(
        {"event": APPROVED, "correlate_on": "subject", "slack_seconds": 900},
        _cycle(), 2000, _requested("Invoice 7"))

    assert found is not None
    assert signals.asked == [(APPROVED, 1100)]          # 2000 - 900


def test_slack_never_produces_a_negative_lower_bound():
    signals = FakeSignals([])
    _finder(signals).find(
        {"event": APPROVED, "slack_seconds": 900}, _cycle(), 100, {})
    assert signals.asked == [(APPROVED, 0)]


def test_evidence_older_than_the_window_is_not_matched():
    signals = FakeSignals([_mail(500, "Invoice 7")])
    found = _finder(signals).find(
        {"event": APPROVED, "correlate_on": "subject", "slack_seconds": 60},
        _cycle(), 2000, _requested("Invoice 7"))
    assert found is None


# --- plain field filters ---------------------------------------------------------------------

def test_where_filters_on_equality():
    signals = FakeSignals([
        _mail(2100, "s", from_domain="spam.example"),
        _mail(2200, "s", from_domain="dreamhost.com"),
    ])
    found = _finder(signals).find(
        {"event": APPROVED, "where": {"from_domain": "dreamhost.com"}}, _cycle(), 2000, {})
    assert found["matched_at"] == 2200


def test_where_and_correlation_both_apply():
    signals = FakeSignals([
        _mail(2100, "Invoice 7", from_domain="spam.example"),
        _mail(2200, "Invoice 9", from_domain="dreamhost.com"),
        _mail(2300, "Invoice 7", from_domain="dreamhost.com"),
    ])
    found = _finder(signals).find(
        {"event": APPROVED, "correlate_on": "subject",
         "where": {"from_domain": "dreamhost.com"}},
        _cycle(), 2000, _requested("Invoice 7"))
    assert found["matched_at"] == 2300


# --- event time is the evidence's, not the run's ------------------------------------------

def test_a_satisfied_await_is_stamped_with_the_evidence_time():
    """Load-bearing, not tidiness. DreamHost's payment mails arrive ~3h apart (submitted
    15:17, processed 18:12) — both between two runs of a daily reconciler. If the submitted
    milestone carried the RUN's clock, the processed stage would look from the next morning
    and never see the 18:12 mail, stalling in a way that looks like patient waiting."""
    from reconciler.adapters.gateway import event_time

    submitted_at = 1784301420          # 2026-07-15 15:17
    noticed_at = submitted_at + 60_000  # the run that noticed, next day

    found = _finder(FakeSignals([(submitted_at, {"subject": "Invoice 7"})])).find(
        {"event": APPROVED, "correlate_on": "subject"},
        _cycle(), submitted_at - 100, _requested("Invoice 7"))

    assert event_time(found, noticed_at) == submitted_at


def test_an_act_payload_is_stamped_with_now():
    from reconciler.adapters.gateway import event_time
    assert event_time({"lines": []}, 4242) == 4242
