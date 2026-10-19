"""The process graph contract — `processes/*.yml` -> `contracts/processes.json` (ADR 0012).

Since the 2026-10-19 repo split this file is rendered and committed HERE, and the dashboard
(still in `inference`) keeps its own copy that its CI diffs against this one. These tests are the
producer half: they assert the projection still says what the board relies on it saying. The
consumer half — that the dashboard image actually COPYs the file into both build stages — moved
to `inference` with the Dockerfile it reads, because a test cannot assert about a tree that is
not in its repo.

The dashboard renders the process tier from a *generated projection* of the definitions rather
than by importing them, because the dashboard image is built with `dashboard/` as its Docker
context and cannot reach `src/reconciler` at all. That buys tier independence (the dashboard
needs neither pydantic nor pyyaml nor any idea what a `signal` block means) and costs a file
that can go stale, which is what these tests are for.

CI re-runs the generator and diffs, so staleness is caught there too. What is tested here is
the part a diff cannot see: that the projection still says what the board relies on it saying.
"""

import json
import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
GENERATOR = ROOT / "scripts" / "emit_process_graph.py"
CONTRACT = ROOT / "contracts" / "processes.json"

sys.path.insert(0, str(ROOT / "src"))
from reconciler.definition import GENESIS_STAGE, load_definitions  # noqa: E402


@pytest.fixture(scope="module")
def contract() -> dict:
    return json.loads(CONTRACT.read_text())


def test_the_committed_contract_matches_the_definitions():
    """The same check CI runs, kept here so `pytest` alone catches it.

    Run in a subprocess against a temp copy rather than by calling `main()`, so the test
    cannot itself rewrite the file it is checking — a generator test that repairs the drift it
    is meant to detect passes forever.
    """
    result = subprocess.run(
        [sys.executable, str(GENERATOR)], cwd=ROOT, capture_output=True, text=True,
        env={"PYTHONPATH": str(ROOT / "src"), "PATH": "/usr/bin:/bin"},
    )
    assert result.returncode == 0, result.stderr
    # The generator rewrote the file; if it differed, git would show it. Compare against a
    # fresh projection of the definitions instead, which is the same assertion without
    # needing git.
    fresh = json.loads(CONTRACT.read_text())
    names = {p["name"] for p in fresh["processes"]}
    assert names == {d.name for d in load_definitions(ROOT / "processes")}


def test_every_stage_carries_what_the_board_renders(contract):
    """The board's `StageDef` is a hard contract: a missing key renders as `undefined` in the
    browser rather than failing, so it has to be asserted somewhere."""
    required = {"name", "label", "kind", "after", "detail", "event"}
    for process in contract["processes"]:
        for stage in process["stages"]:
            assert required <= set(stage), f"{process['name']}.{stage['name']} missing keys"
            assert stage["kind"] in {"genesis", "act", "await"}
            assert stage["label"], f"{stage['name']} has no label"


def test_the_genesis_milestone_is_first_and_is_not_a_stage(contract):
    """`cycle_opened` is a recorded milestone but NOT a stage — the reconciler is handed cycles,
    it never creates them. The projection prepends it so the board can render it as given, and
    the board's frontier walk depends on it being index 0 (every real first stage lists it in
    `after`)."""
    for process in contract["processes"]:
        assert process["stages"][0]["name"] == GENESIS_STAGE
        assert process["stages"][0]["kind"] == "genesis"
        definition = next(d for d in load_definitions(ROOT / "processes")
                          if d.name == process["name"])
        assert GENESIS_STAGE not in {s.name for s in definition.stages}


def test_dependencies_all_resolve(contract):
    """`after` drives both the stepper's ordering and the frontier rule. A dangling name would
    make a stage permanently un-`waiting` — the process would look finished while sitting
    still, which is the exact silent stall the tier exists to avoid."""
    for process in contract["processes"]:
        seen: set[str] = set()
        for stage in process["stages"]:
            for dep in stage["after"]:
                assert dep in seen, (
                    f"{process['name']}.{stage['name']} depends on {dep!r}, "
                    f"which is not an earlier stage in the projection")
            seen.add(stage["name"])


def test_the_first_real_stage_depends_on_the_genesis_fact(contract):
    """An empty `after` in the YAML means "as soon as the cycle exists". The projection fills
    that in rather than leaving it empty, because the board treats an empty `after` as
    "ready immediately" — which for a stage in the MIDDLE of a process would light up the
    wrong frontier."""
    for process in contract["processes"]:
        for stage in process["stages"][1:]:
            assert stage["after"], f"{process['name']}.{stage['name']} has no dependency"


def test_the_milestone_event_names_are_process_prefixed(contract):
    """The prefix is structural, not conventional (ADR 0012): an unprefixed milestone could
    collide with an inference definition's `input_event_names()` and be routed into an engine
    that never expected it. The dashboard also relies on it — `view.ts::processOf` strips
    `<process>_` to title a milestone, and matches the separator so one process name cannot
    shadow another's."""
    for process in contract["processes"]:
        for stage in process["stages"]:
            assert stage["event"] == f"{process['name']}_{stage['name']}"
        assert process["void_event"].startswith(process["name"] + "_")
