"""SPIKE tests — `sweep-tasks` as a controller (see `measure-trip-wire-5` in the task vault).

These exist to prove one claim, which is the whole reason the spike is cheap: **the domain is
already testable without the driver.** Not one of them imports kopf, starts a cluster, or needs
a CRD — exactly as the Argo entrypoint's tests do not import Argo. If a controller is a thin
shell over `tasks.sweep`, then moving the driver costs the test suite nothing, and the 288
existing tests keep their value whichever way the decision goes.

The one thing genuinely worth testing in the controller itself is the STATUS PROJECTION, because
that is where the design rule lives: status is output, never input.
"""

import contextlib

import pytest

from reconciler.controllers import task_sweep


# --- the status projection ---------------------------------------------------------------

def test_agreement_is_in_sync_not_an_empty_status():
    """A sweep with nothing to do must say so positively.

    `phase: InSync` and a sentence, rather than absent fields — because "no drift" and "never
    ran" must not look identical in `kubectl get`. That is the same reasoning that made
    `waiting` its own state on the process board instead of collapsing into the stages behind
    it (ADR 0012), and the same failure the tier exists to avoid: silence reading as health.
    """
    status = task_sweep._status(0, 0)
    assert status["phase"] == "InSync"
    assert status["message"] == "the label and the event log agree"
    assert status["toOpen"] == 0 and status["toClose"] == 0
    assert status["lastSweep"], "an in-sync pass still has to stamp that it ran"


@pytest.mark.parametrize("to_open,to_close", [(1, 0), (0, 1), (3, 2)])
def test_any_difference_is_drift(to_open, to_close):
    status = task_sweep._status(to_open, to_close)
    assert status["phase"] == "Drifted"
    assert str(to_open) in status["message"] and str(to_close) in status["message"]


def test_the_phase_vocabulary_matches_the_crd_enum():
    """A status value the CRD will not accept is rejected by the API server at write time and
    the controller looks broken for a reason nothing in Python explains. Cheap to pin."""
    import pathlib

    import yaml

    crd = yaml.safe_load(
        (pathlib.Path(__file__).resolve().parents[1] / "deploy/crds/tasksweep.yml").read_text())
    schema = crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
    allowed = set(schema["properties"]["status"]["properties"]["phase"]["enum"])

    produced = {task_sweep._status(0, 0)["phase"], task_sweep._status(1, 1)["phase"], "Failed"}
    assert produced <= allowed, f"controller can emit a phase the CRD rejects: {produced - allowed}"


def test_every_status_key_the_controller_writes_exists_in_the_crd():
    """Unknown status keys are pruned, and that is operation-dependent — MEASURED on a real
    cluster (k8s 1.34, 2026-10-21) rather than assumed:

        apply  (spec)   unknown field -> REJECTED      (strict field validation, 1.25+)
        patch  (status) unknown field -> warned, PRUNED, and the write still SUCCEEDS

    kopf writes status by patching, so it lands in the second row: a key the CRD does not
    declare is dropped and the controller reports success. Nothing else would catch that, which
    is why this is pinned rather than trusted."""
    import pathlib

    import yaml

    crd = yaml.safe_load(
        (pathlib.Path(__file__).resolve().parents[1] / "deploy/crds/tasksweep.yml").read_text())
    schema = crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
    declared = set(schema["properties"]["status"]["properties"])

    written = set(task_sweep._status(1, 1)) | {"phase", "message"}
    assert written <= declared, f"written but not declared, so silently dropped: {written - declared}"


# --- the guard that matters most ----------------------------------------------------------

def test_the_spike_cannot_write(monkeypatch):
    """The Argo CronWorkflow is the SOLE producer of `email_task_closed` (doc/email-tasks.md).
    A second producer reading a store the first cannot see is the duplicate-close bug fixed on
    2026-10-18. The spike must observe only, and `dry_run` is what enforces it — so assert the
    option actually reaches the call rather than trusting the CRD default.
    """
    seen = {}

    def fake_sweep_tasks(*, label, user, lookback_days, options):
        seen.update(label=label, user=user, lookback=lookback_days, dry_run=options.dry_run)
        return type("P", (), {"to_open": [], "to_close": []})()

    monkeypatch.setattr(task_sweep.app, "sweep_tasks", fake_sweep_tasks)
    task_sweep.plan({"label": "aware/todo", "userId": "rods", "lookbackDays": 7})

    assert seen["dry_run"] is True, "the spike would write to production"
    assert seen == {"label": "aware/todo", "user": "rods", "lookback": 7, "dry_run": True}


def test_the_lookback_floor_is_declared_not_defaulted_silently():
    """`lookbackDays` has a `minimum` in the CRD, not just a default. Shortening it silently
    retires every task older than the horizon at once, because the sweep closes on ABSENCE of
    evidence — so the bound belongs where the API server enforces it, not in Python."""
    import pathlib

    import yaml

    crd = yaml.safe_load(
        (pathlib.Path(__file__).resolve().parents[1] / "deploy/crds/tasksweep.yml").read_text())
    spec = (crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
            ["properties"]["spec"]["properties"]["lookbackDays"])
    assert spec["minimum"] >= 1 and spec["default"] == 30


# --- the driver, exercised only when the extra is installed -------------------------------

def test_register_attaches_a_timer_that_matches_the_crd():
    """The one thing worth running kopf for: that the controller and the CRD agree.

    They are two files that must name the same resource, and nothing else checks it — a typo
    in either means the operator starts cleanly, watches a resource that does not exist, and
    reports nothing wrong. That is the silent-stall shape again, so it gets a test.

    Skipped without the `operator` extra, which is what keeps the CI contract intact: the
    suite must pass with no third-party packages beyond the dev ones.
    """
    import pathlib

    import yaml

    kopf = pytest.importorskip("kopf", reason="needs the `operator` extra")

    registry = kopf.OperatorRegistry()
    with kopf.get_default_registry_or(registry) if hasattr(kopf, "get_default_registry_or") \
            else _swap_default_registry(kopf, registry):
        task_sweep.register()

    handlers = list(registry._spawning.get_all_handlers())
    assert len(handlers) == 1, "expected exactly one timer"
    (handler,) = handlers

    crd = yaml.safe_load(
        (pathlib.Path(__file__).resolve().parents[1] / "deploy/crds/tasksweep.yml").read_text())
    assert handler.selector.group == crd["spec"]["group"]
    assert handler.selector.version == crd["spec"]["versions"][0]["name"]
    assert handler.selector.any_name == crd["spec"]["names"]["plural"]
    assert handler.id == "sweep", "a <locals> id would tie stored progress to the Python shape"


@contextlib.contextmanager
def _swap_default_registry(kopf, registry):
    """kopf has no public "register into this registry" API across versions; swap and restore."""
    previous = kopf.get_default_registry()
    kopf.set_default_registry(registry)
    try:
        yield
    finally:
        kopf.set_default_registry(previous)
