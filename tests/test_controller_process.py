"""`Process` as a controller: the status projection, the write guard, and CRD agreement."""

import pathlib

import pytest
import yaml

from reconciler.app import ProcessPass
from reconciler.controllers import process
from reconciler.core import Cycle, Outcome, Status

ROOT = pathlib.Path(__file__).resolve().parents[1]
CRD = yaml.safe_load((ROOT / "deploy/crds/process.yml").read_text())
SCHEMA = CRD["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
OBJECT = yaml.safe_load((ROOT / "deploy/operator/process-dreamhost-invoice.yml").read_text())

OCTOBER = {"start": "2026-10-01", "end": "2026-10-31"}


def cyc(key, period=None):
    return Cycle(key=key, process="dreamhost_invoice", user_id="rods", opened_at=0,
                 context={"worked_period": period} if period else {})


def done(key):
    return (cyc(key), Outcome(Status.COMPLETE))


def result(results=(), opened=None):
    return ProcessPass(process="dreamhost_invoice", due_period=OCTOBER, opened=opened,
                       results=list(results))


def test_the_dry_run_preview_names_the_cycle_it_would_open():
    """What the owner checks before the swap: 'would open dh_invoice_2026_011 for October'."""
    status = process._status(result([done("dh_invoice_2026_010")],
                                    opened=cyc("dh_invoice_2026_011", OCTOBER)), dry_run=True)
    assert status["phase"] == "Opening"
    assert status["message"].startswith("would open dh_invoice_2026_011 for 2026-10-01..2026-10-31")
    assert "dry run" in status["message"]
    assert status["open"] == {"key": "dh_invoice_2026_011", "period": OCTOBER}
    assert status["complete"] == 1 and status["cycles"] == []


def test_a_waiting_cycle_says_what_it_waits_on():
    waiting = (cyc("dh_invoice_2026_011"),
               Outcome(Status.WAITING, waiting_on=["invoice_approved"]))
    status = process._status(result([waiting]), dry_run=False)
    assert status["phase"] == "Waiting"
    assert status["message"] == "waiting: dh_invoice_2026_011 on invoice_approved"
    assert status["cycles"] == [{"key": "dh_invoice_2026_011", "status": "waiting",
                                 "waitingOn": ["invoice_approved"]}]


def test_advancing_is_reported_with_its_stages():
    moved = (cyc("dh_invoice_2026_011"),
             Outcome(Status.WAITING, advanced=["total_computed", "invoice_generated"],
                     waiting_on=["invoice_approved"]))
    status = process._status(result([moved]), dry_run=False)
    assert status["phase"] == "Advanced"
    assert status["message"] == "advanced dh_invoice_2026_011 (total_computed, invoice_generated)"


def test_nothing_open_is_idle_not_silent():
    status = process._status(result([done("a"), done("b")]), dry_run=False)
    assert status["phase"] == "Idle" and status["message"] == "nothing open; 2 complete"


def test_every_phase_and_key_exists_in_the_crd():
    allowed = set(SCHEMA["properties"]["status"]["properties"]["phase"]["enum"])
    declared = set(SCHEMA["properties"]["status"]["properties"])
    cycle_keys = set(SCHEMA["properties"]["status"]["properties"]["cycles"]["items"]["properties"])
    samples = [
        process._status(result([done("a")], opened=cyc("b", OCTOBER)), dry_run=True),
        process._status(result([done("a")], opened=cyc("b", OCTOBER)), dry_run=False),
        process._status(result([(cyc("c"), Outcome(Status.WAITING, advanced=["x"],
                                                   waiting_on=["y"]))]), dry_run=False),
        process._status(result([(cyc("d"), None)]), dry_run=False),
        process._status(result([]), dry_run=False),
        process._failed(RuntimeError("x")),
    ]
    assert {s["phase"] for s in samples} <= allowed
    for s in samples:
        assert set(s) <= declared, f"written but not declared: {set(s) - declared}"
        for c in s.get("cycles", []):
            assert set(c) <= cycle_keys, f"cycle field not declared: {set(c) - cycle_keys}"


def _capture(monkeypatch) -> dict:
    seen = {}

    def fake(definition, *, tz, open_scheduled, options):
        seen.update(definition=definition, tz=tz, open=open_scheduled, dry_run=options.dry_run)
        return result()

    monkeypatch.setattr(process.app, "run_process", fake)
    return seen


def test_an_object_that_does_not_ask_to_act_cannot(monkeypatch):
    seen = _capture(monkeypatch)
    process.plan({"definition": "dreamhost_invoice"})
    assert seen == {"definition": "dreamhost_invoice", "tz": "Europe/Zurich", "open": True,
                    "dry_run": True}


@pytest.mark.parametrize("dry_run", [True, False])
def test_acting_follows_spec_dry_run(monkeypatch, dry_run):
    seen = _capture(monkeypatch)
    process.plan({"definition": "dreamhost_invoice", "dryRun": dry_run})
    assert seen["dry_run"] is dry_run


def test_the_production_object_is_pinned():
    """Writes went on 2026-11-03, after Argo's advance-cycles was suspended — never two
    advancers. Pinned so any change to what runs the invoice is a visible test change."""
    assert OBJECT["spec"] == {"definition": "dreamhost_invoice", "timezone": "Europe/Zurich",
                              "openScheduled": True, "dryRun": False}


def test_register_attaches_a_timer_that_matches_the_crd():
    kopf = pytest.importorskip("kopf", reason="needs the `operator` extra")

    registry = kopf.OperatorRegistry()
    previous = kopf.get_default_registry()
    kopf.set_default_registry(registry)
    try:
        process.register()
    finally:
        kopf.set_default_registry(previous)

    (handler,) = list(registry._spawning.get_all_handlers())
    assert handler.selector.group == CRD["spec"]["group"]
    assert handler.selector.any_name == CRD["spec"]["names"]["plural"]
    assert handler.id == "process"
