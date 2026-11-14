"""Tests for `gmail.archive` — archiving the mail a gate was satisfied by."""

import pathlib

import pytest

from reconciler.actions import ActionContext, Services, build_action
from reconciler.core import Cycle, Milestone
from reconciler.definition import load_definitions

PROCESSES_DIR = pathlib.Path(__file__).resolve().parents[1] / "processes"
LABEL = "aware/invoice-approved"


class FakeRelay:
    def __init__(self, fail_on=None):
        self.calls = []
        self.fail_on = fail_on

    def remove(self, *, message_id, label):
        if label == self.fail_on:
            raise RuntimeError("label relay rejected: HTTP 500")
        self.calls.append((message_id, label))


def _ctx(config, evidence, relay):
    cycle = Cycle(key="dh_invoice_2026_011", process="dreamhost_invoice",
                  user_id="u", opened_at=1000)
    milestones = {"data_approved": Milestone("data_approved", 2000,
                                             {"matched": "gmail", "evidence": evidence})}
    return ActionContext(cycle=cycle, milestones=milestones, config=config,
                         services=Services(labels=relay))


def test_archives_the_matched_mail_and_takes_the_label_off():
    relay = FakeRelay()
    out = build_action("gmail.archive")(_ctx(
        {"evidence_of": "data_approved", "remove_labels": [LABEL]},
        {"upstream_id": "18f2abc"}, relay))
    assert relay.calls == [("18f2abc", "INBOX"), ("18f2abc", LABEL)]
    assert out == {"evidence_of": "data_approved", "message_id": "18f2abc",
                   "removed": ["INBOX", LABEL]}


def test_inbox_only_when_no_extra_labels():
    relay = FakeRelay()
    build_action("gmail.archive")(_ctx({"evidence_of": "data_approved"},
                                       {"upstream_id": "m1"}, relay))
    assert relay.calls == [("m1", "INBOX")]


def test_evidence_without_a_message_id_is_recorded_not_raised():
    """A raise would end every pass at this cycle, for a mail nobody can identify."""
    relay = FakeRelay()
    out = build_action("gmail.archive")(_ctx({"evidence_of": "data_approved"}, {}, relay))
    assert relay.calls == []
    assert out["message_id"] is None


def test_a_relay_failure_raises_so_the_stage_is_retried():
    with pytest.raises(RuntimeError, match="HTTP 500"):
        build_action("gmail.archive")(_ctx(
            {"evidence_of": "data_approved", "remove_labels": [LABEL]},
            {"upstream_id": "m1"}, FakeRelay(fail_on=LABEL)))


def test_no_relay_is_a_loud_stop():
    with pytest.raises(RuntimeError, match="GMAIL_LABEL_URL"):
        build_action("gmail.archive")(_ctx({"evidence_of": "data_approved"},
                                           {"upstream_id": "m1"}, None))


def test_missing_evidence_of_is_a_definition_error():
    with pytest.raises(ValueError, match="evidence_of"):
        build_action("gmail.archive")(_ctx({}, {"upstream_id": "m1"}, FakeRelay()))


# --- the shipped definition -------------------------------------------------------------

def _invoice():
    return load_definitions(PROCESSES_DIR)[0]


def test_every_mail_the_invoice_waits_on_is_archived_after_its_stage():
    invoice = _invoice()
    awaits = {s.name for s in invoice.stages if s.kind == "await"}
    archived = {s.config["evidence_of"]: s for s in invoice.stages if s.action == "gmail.archive"}
    assert set(archived) == awaits
    # The approval mail goes when its gate closes; the PDF mail is held until the money lands.
    assert archived["data_approved"].after == ["data_approved"]
    assert archived["invoice_approved"].after == ["payment_processed"]


def test_our_labelled_mails_also_lose_the_approval_label():
    archived = {s.config["evidence_of"]: s.config.get("remove_labels", [])
                for s in _invoice().stages if s.action == "gmail.archive"}
    gate_label = {s.name: s.signal.get("label") for s in _invoice().stages if s.kind == "await"}
    for stage, removed in archived.items():
        assert removed == ([gate_label[stage]] if gate_label[stage] else [])


def test_housekeeping_comes_after_every_invoice_stage():
    """A pass ends at the first raise, so an archive failure must never precede real work."""
    kinds = ["archive" if s.action == "gmail.archive" else "work" for s in _invoice().stages]
    assert kinds == sorted(kinds, key=lambda k: k == "archive")


# --- through core + world, the path a real pass takes -------------------------------------

def test_each_archive_stage_archives_its_own_mail_in_a_real_pass():
    """Regression, 2026-11-14: `RealWorld` looked config up by ACTION, so all four
    `gmail.archive` stages ran with the first one's config and archived the approval mail
    four times. Calling the action directly (the tests above) could not see that."""
    from reconciler.adapters.gateway import DryRunMilestones
    from reconciler.core import reconcile
    from reconciler.world import RealWorld

    invoice = _invoice()
    relay = FakeRelay()
    world = RealWorld(invoice, sink=DryRunMilestones(invoice), services=Services(labels=relay))
    cycle = Cycle(key="dh_invoice_2026_011", process=invoice.name, user_id="u", opened_at=1)
    ids = {"data_approved": "approval", "invoice_approved": "pdf",
           "payment_submitted": "submitted", "payment_processed": "processed"}
    # Every invoice stage already recorded, as on the four complete cycles at deploy time.
    done = {s.name: Milestone(s.name, 10, {"evidence": {"upstream_id": ids.get(s.name)}})
            for s in invoice.stages if s.action != "gmail.archive"}

    reconcile(invoice, cycle, done, world)

    assert sorted(relay.calls) == sorted([
        ("approval", "INBOX"), ("approval", LABEL),
        ("submitted", "INBOX"),
        ("processed", "INBOX"),
        ("pdf", "INBOX"), ("pdf", LABEL),
    ])
