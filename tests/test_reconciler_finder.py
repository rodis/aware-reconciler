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


# --- reply gates --------------------------------------------------------------------------

class FakeGmail:
    """A CandidateSource answering from a scripted mailbox, recording the query it was asked."""

    def __init__(self, mails):
        self.mails = mails
        self.asked = []

    def candidates(self, signal, since):
        self.asked.append(signal)
        return [(ts, body) for ts, body in self.mails if ts >= since]


SUBJECT = "Invoice 12 — please check and approve"
REPLY_SIGNAL = {"source": "gmail", "correlate_on": "subject", "reply": {"accept": ["yes"]}}


def _sent(ts, text, subject=f"Re: {SUBJECT}", thread="t1", upstream_id="r1"):
    return (ts, {"subject": subject, "snippet": text, "labels": ["SENT"],
                 "gmail_thread_id": thread, "upstream_id": upstream_id})


def _original(ts=2000, thread="t1", upstream_id="o1"):
    return (ts, {"subject": SUBJECT, "snippet": "Invoice 12 ...", "labels": ["INBOX"],
                 "gmail_thread_id": thread, "upstream_id": upstream_id})


def test_a_yes_reply_closes_the_gate_and_names_the_mail_it_answered():
    gmail = FakeGmail([_original(), _sent(2500, "Yes! On Sat, ... wrote: > Invoice 12")])
    found = SignalFinder(gmail).find(REPLY_SIGNAL, _cycle(), 2000, _requested(SUBJECT))
    assert found["answer"] == "yes"
    assert found["matched_at"] == 2500
    assert found["evidence"]["upstream_id"] == "r1"
    assert found["replied_to"] == "o1"
    assert f'subject:"{SUBJECT}"' in gmail.asked[0]["q"]


def test_a_no_or_a_question_leaves_the_gate_open():
    for text in ("No, the days are wrong", "I forgot the travel expense", ""):
        gmail = FakeGmail([_original(), _sent(2500, text)])
        assert SignalFinder(gmail).find(REPLY_SIGNAL, _cycle(), 2000, _requested(SUBJECT)) is None


def test_the_request_itself_never_counts_even_though_it_mentions_approve():
    """Only SENT mail is ours. The request arrives from the relay's sender."""
    gmail = FakeGmail([(2500, {"subject": SUBJECT, "snippet": "yes", "labels": ["INBOX"],
                               "gmail_thread_id": "t1", "upstream_id": "o1"})])
    assert SignalFinder(gmail).find(REPLY_SIGNAL, _cycle(), 2000, _requested(SUBJECT)) is None


def test_a_reply_to_another_invoice_does_not_close_this_gate():
    gmail = FakeGmail([_sent(2500, "yes", subject="Re: Invoice 13 — please check and approve")])
    assert SignalFinder(gmail).find(REPLY_SIGNAL, _cycle(), 2000, _requested(SUBJECT)) is None


def test_gate_two_s_reply_cannot_close_gate_one():
    gmail = FakeGmail([_sent(2500, "yes", subject="Re: Invoice 12-2026 — PDF ready to submit")])
    assert SignalFinder(gmail).find(REPLY_SIGNAL, _cycle(), 2000, _requested(SUBJECT)) is None


def test_stacked_reply_prefixes_are_stripped():
    gmail = FakeGmail([_sent(2500, "yes", subject=f"RE: Fwd: re:{SUBJECT}")])
    assert SignalFinder(gmail).find(REPLY_SIGNAL, _cycle(), 2000, _requested(SUBJECT))


def test_a_later_yes_after_a_no_closes_the_gate():
    gmail = FakeGmail([_original(), _sent(2500, "no wait", upstream_id="r1"),
                       _sent(2600, "yes", upstream_id="r2")])
    found = SignalFinder(gmail).find(REPLY_SIGNAL, _cycle(), 2000, _requested(SUBJECT))
    assert found["evidence"]["upstream_id"] == "r2"


def test_a_reply_gate_without_subject_correlation_is_a_definition_error():
    import pytest
    with pytest.raises(ValueError, match="correlate_on: subject"):
        SignalFinder(FakeGmail([])).find({"source": "gmail", "correlate_on": "invoice_number",
                                          "reply": {}}, _cycle(), 0,
                                         {"x": Milestone("x", 1, {"invoice_number": 12})})


def test_an_unquoted_yaml_yes_is_refused_not_read_as_true():
    import pytest
    with pytest.raises(ValueError, match="quoted"):
        SignalFinder(FakeGmail([])).find({**REPLY_SIGNAL, "reply": {"accept": [True]}},
                                         _cycle(), 0, _requested(SUBJECT))
