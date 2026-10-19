"""Tests for the `processes/*.yml` schema (ADR 0012).

The validation here is not decoration: a forward reference or a cycle in `after` would make
`core.reconcile`'s single linear scan invalid, and the symptom at run time is a process that
silently stalls — the worst failure mode for something that legitimately waits for weeks.
"""

import pathlib

import pytest
from pydantic import ValidationError

from reconciler.definition import ProcessDefinition, load_definitions

PROCESSES_DIR = pathlib.Path(__file__).resolve().parents[1] / "processes"


def _definition(**overrides):
    base = {
        "name": "p",
        "cycle_key": "p_{year}",
        "stages": [{"name": "a", "kind": "act", "action": "do.a"}],
    }
    base.update(overrides)
    return ProcessDefinition.model_validate(base)


# --- stage kinds are a strict binary ----------------------------------------------------

def test_act_requires_an_action():
    with pytest.raises(ValidationError, match="requires an `action`"):
        _definition(stages=[{"name": "a", "kind": "act"}])


def test_await_requires_a_signal():
    with pytest.raises(ValidationError, match="requires a `signal`"):
        _definition(stages=[{"name": "a", "kind": "await"}])


def test_act_may_not_carry_a_signal():
    with pytest.raises(ValidationError, match="must not carry a `signal`"):
        _definition(stages=[
            {"name": "a", "kind": "act", "action": "do.a", "signal": {"source": "gmail"}},
        ])


def test_a_third_kind_is_rejected():
    # Trip-wire 1: resisting a third stage kind is a live constraint, not an accident.
    with pytest.raises(ValidationError):
        _definition(stages=[{"name": "a", "kind": "poll", "action": "do.a"}])


def test_no_conditional_guard_field_exists():
    # Trip-wire 7: a `when:` guard is the symptom that some stage's absence isn't handled
    # gracefully. extra="forbid" makes adding one a deliberate schema change.
    with pytest.raises(ValidationError):
        _definition(stages=[
            {"name": "a", "kind": "act", "action": "do.a", "when": "something"},
        ])


# --- stages form an ordered DAG ---------------------------------------------------------

def test_forward_reference_is_rejected():
    with pytest.raises(ValidationError, match="not an earlier stage"):
        _definition(stages=[
            {"name": "a", "kind": "act", "action": "do.a", "after": ["b"]},
            {"name": "b", "kind": "act", "action": "do.b"},
        ])


def test_self_reference_is_rejected():
    with pytest.raises(ValidationError, match="not an earlier stage"):
        _definition(stages=[
            {"name": "a", "kind": "act", "action": "do.a", "after": ["a"]},
        ])


def test_duplicate_stage_names_are_rejected():
    with pytest.raises(ValidationError, match="duplicate stage"):
        _definition(stages=[
            {"name": "a", "kind": "act", "action": "do.a"},
            {"name": "a", "kind": "act", "action": "do.b"},
        ])


def test_reserved_milestone_names_are_rejected():
    # cycle_voided is terminal and cycle_opened is genesis; a stage owning either would let
    # a process define away its own void check.
    for reserved in ("cycle_opened", "cycle_voided"):
        with pytest.raises(ValidationError, match="reserved milestone name"):
            _definition(stages=[{"name": reserved, "kind": "act", "action": "do.a"}])


def test_a_process_needs_at_least_one_stage():
    with pytest.raises(ValidationError, match="at least one stage"):
        _definition(stages=[])


def test_a_diamond_is_valid():
    d = _definition(stages=[
        {"name": "a", "kind": "act", "action": "do.a"},
        {"name": "b", "kind": "act", "action": "do.b", "after": ["a"]},
        {"name": "c", "kind": "act", "action": "do.c", "after": ["a"]},
        {"name": "d", "kind": "act", "action": "do.d", "after": ["b", "c"]},
    ])
    assert [s.name for s in d.stages] == ["a", "b", "c", "d"]


# --- opens ------------------------------------------------------------------------------

def test_schedule_requires_a_cron():
    with pytest.raises(ValidationError, match="requires a `cron`"):
        _definition(opens=[{"via": "schedule"}])


def test_manual_must_not_carry_a_cron():
    with pytest.raises(ValidationError, match="must not carry a `cron`"):
        _definition(opens=[{"via": "manual", "cron": "0 9 1 * *"}])


def test_opens_is_not_keyed_on_the_yaml_boolean_trap():
    """YAML 1.1 resolves a bare `on` key to True, so `{on: manual}` parses as {True: ...}.

    Pinned because the failure is silent at the YAML layer and only surfaces as a missing
    required field — exactly the shape of bug that costs an afternoon.
    """
    import yaml
    assert yaml.safe_load("{on: manual}") == {True: "manual"}
    assert yaml.safe_load("{via: manual}") == {"via": "manual"}


# --- emitted event names are namespaced structurally ------------------------------------

def test_event_name_is_prefixed_with_the_process():
    # An unprefixed milestone could match a definition's input_event_names() and be routed
    # into an engine not expecting it — the one thing that turns a no-op into a bug.
    assert _definition().event_name("approved") == "p_approved"


# --- the real definition on disk --------------------------------------------------------

def test_the_shipped_invoice_definition_loads():
    definitions = load_definitions(PROCESSES_DIR)
    assert [d.name for d in definitions] == ["dreamhost_invoice"]


def test_the_invoice_collects_manual_lines_after_approval():
    """The ordering the prior art got backwards, pinned so it can't regress."""
    invoice = load_definitions(PROCESSES_DIR)[0]
    order = [s.name for s in invoice.stages]
    assert order.index("manual_lines") > order.index("data_approved")


def test_the_invoice_has_two_approval_gates():
    """Figures first, then the rendered PDF — they catch different mistakes."""
    invoice = load_definitions(PROCESSES_DIR)[0]
    order = [s.name for s in invoice.stages]
    assert order.index("data_approved") < order.index("invoice_generated")
    assert order.index("invoice_approved") > order.index("invoice_emailed")


def test_both_gates_share_one_label_and_are_told_apart_by_subject():
    """One Gmail label is enough because correlation is on subject, not on the label — so
    gate ①'s mail can never close gate ②."""
    invoice = load_definitions(PROCESSES_DIR)[0]
    gates = [s for s in invoice.stages if s.name in ("data_approved", "invoice_approved")]
    assert len({g.signal["label"] for g in gates}) == 1
    assert all(g.signal["correlate_on"] == "subject" for g in gates)


def test_no_await_waits_to_be_told():
    """Every loop belongs to the reconciler. A signal source that pushes (a polling connector
    depositing events) makes "nothing is watching" indistinguishable from "not yet"."""
    invoice = load_definitions(PROCESSES_DIR)[0]
    sources = {s.signal["source"] for s in invoice.stages if s.kind == "await"}
    assert sources <= {"gmail", "classify"}


def test_payment_stages_follow_the_observed_order():
    """Observed 2026-07-15: submitted 15:17, then processed 18:12. Backwards would stall the
    process forever, in a way indistinguishable from legitimately waiting."""
    invoice = load_definitions(PROCESSES_DIR)[0]
    order = [s.name for s in invoice.stages]
    assert order.index("payment_submitted") < order.index("payment_processed")


def test_the_invoice_opens_on_a_schedule_and_by_hand():
    # A bonus invoice has no cadence, so a schedule alone cannot be the way a cycle starts.
    invoice = load_definitions(PROCESSES_DIR)[0]
    assert {o.via for o in invoice.opens} == {"schedule", "manual"}


def test_invalid_definitions_are_skipped_not_fatal(tmp_path):
    (tmp_path / "good.yml").write_text(
        "name: good\ncycle_key: g\nstages: [{name: a, kind: act, action: do.a}]\n"
    )
    (tmp_path / "bad.yml").write_text("name: bad\nstages: []\n")
    assert [d.name for d in load_definitions(tmp_path)] == ["good"]


def test_disabled_definitions_are_skipped(tmp_path):
    (tmp_path / "off.yml").write_text(
        "name: off\nenabled: false\ncycle_key: o\n"
        "stages: [{name: a, kind: act, action: do.a}]\n"
    )
    assert load_definitions(tmp_path) == []
