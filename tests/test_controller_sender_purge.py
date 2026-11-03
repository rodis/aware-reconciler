"""`SenderPurge` as a controller: the status projection, the write guard, and CRD agreement.

Same split as `test_controller_task_sweep.py`: the rule is tested in `test_purge.py` with no
driver at all; this file tests only what the controller adds.
"""

import contextlib
import pathlib
from datetime import UTC, datetime

import pytest
import yaml

from reconciler import purge
from reconciler.controllers import sender_purge

ROOT = pathlib.Path(__file__).resolve().parents[1]
CRD = yaml.safe_load((ROOT / "deploy/crds/senderpurge.yml").read_text())
SCHEMA = CRD["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
OBJECT = yaml.safe_load((ROOT / "deploy/operator/senderpurge-newsletters.yml").read_text())


def result(due=0, trashed=0, refused=0, truncated=False):
    return purge.PurgePlan(cutoff=datetime(2026, 11, 2, 23, 0, tzinfo=UTC),
                           to_trash=[{}] * due, refused=[{}] * refused,
                           truncated=truncated, trashed=trashed)


# --- the status projection ------------------------------------------------------------------

def test_nothing_due_is_clean_and_still_stamped():
    status = sender_purge._status(result())
    assert status["phase"] == "Clean" and status["lastPass"] and status["cutoff"]


def test_a_dry_run_says_what_it_would_trash():
    status = sender_purge._status(result(due=71), dry_run=True)
    assert status["phase"] == "Pending"
    assert status["message"].startswith("would trash 71") and "dry run" in status["message"]


def test_a_real_pass_says_what_it_trashed():
    status = sender_purge._status(result(due=3, trashed=3), dry_run=False)
    assert status["phase"] == "Purged" and status["message"] == "trashed 3"


def test_truncation_and_refusals_are_visible():
    message = sender_purge._status(result(due=100, truncated=True, refused=2))["message"]
    assert "more remain" in message and "refused 2" in message


def test_every_phase_and_status_key_exists_in_the_crd():
    """Unknown status keys are PRUNED on a status patch and the write still succeeds (measured
    2026-10-21, see the TaskSweep test) — so a key the CRD lacks vanishes silently."""
    allowed = set(SCHEMA["properties"]["status"]["properties"]["phase"]["enum"])
    declared = set(SCHEMA["properties"]["status"]["properties"])
    samples = [sender_purge._status(result()), sender_purge._status(result(due=1)),
               sender_purge._status(result(due=1, trashed=1), dry_run=False),
               sender_purge._failed(RuntimeError("x"))]
    assert {s["phase"] for s in samples} <= allowed
    for s in samples:
        assert set(s) <= declared, f"written but not declared: {set(s) - declared}"


# --- the guard that matters most ------------------------------------------------------------

def _capture(monkeypatch) -> dict:
    seen = {}

    def fake(*, senders, tz, min_age_hours, max_per_run, options):
        seen.update(senders=senders, tz=tz, min_age=min_age_hours, cap=max_per_run,
                    dry_run=options.dry_run)
        return result()

    monkeypatch.setattr(sender_purge.app, "purge_senders", fake)
    return seen


def test_an_object_that_does_not_ask_to_trash_cannot(monkeypatch):
    seen = _capture(monkeypatch)
    sender_purge.plan({"senders": ["a@b.c"]})
    assert seen == {"senders": ["a@b.c"], "tz": "Europe/Zurich", "min_age": 12, "cap": 100,
                    "dry_run": True}


@pytest.mark.parametrize("dry_run", [True, False])
def test_trashing_follows_spec_dry_run(monkeypatch, dry_run):
    seen = _capture(monkeypatch)
    sender_purge.plan({"senders": ["a@b.c"], "dryRun": dry_run})
    assert seen["dry_run"] is dry_run


def test_the_production_object_is_pinned():
    """Writes went on 2026-11-03, after the dry-run preview matched Gmail. Pinned so a change to
    who is purged, how soon, or whether it writes is always a visible test change."""
    assert OBJECT["spec"]["dryRun"] is False
    assert OBJECT["spec"]["senders"] == ["mftradingguide@class.it"]
    assert OBJECT["spec"]["minAgeHours"] == 12


def test_the_crd_rejects_a_sender_that_is_not_an_address():
    """A bare domain or an empty string would make Gmail's `from:` far too broad."""
    import re

    pattern = re.compile(SCHEMA["properties"]["spec"]["properties"]["senders"]["items"]["pattern"])
    assert pattern.match("mftradingguide@class.it")
    for bad in ["", "class.it", "@class.it", "a@", "a b@c.d", "a@b@c"]:
        assert not pattern.match(bad), bad


# --- the driver, when the operator extra is installed ---------------------------------------

def test_register_attaches_a_timer_that_matches_the_crd():
    kopf = pytest.importorskip("kopf", reason="needs the `operator` extra")

    registry = kopf.OperatorRegistry()
    with _swap_default_registry(kopf, registry):
        sender_purge.register()

    (handler,) = list(registry._spawning.get_all_handlers())
    assert handler.selector.group == CRD["spec"]["group"]
    assert handler.selector.version == CRD["spec"]["versions"][0]["name"]
    assert handler.selector.any_name == CRD["spec"]["names"]["plural"]
    assert handler.id == "purge"


@contextlib.contextmanager
def _swap_default_registry(kopf, registry):
    previous = kopf.get_default_registry()
    kopf.set_default_registry(registry)
    try:
        yield
    finally:
        kopf.set_default_registry(previous)
