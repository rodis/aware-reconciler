"""Emit the process graph — the contract that carries `processes/*.yml` to the dashboard.

**This repo OWNS the contract; the dashboard consumes a copy.** Since the 2026-10-19 split the
dashboard lives in `inference` and the definitions live here, so the file crosses a *repo*
boundary as well as a build one. The arrangement: this script renders `contracts/processes.json`
here and it is committed here, and `inference` keeps its own committed `dashboard/processes.json`
plus a CI step that fetches this file and fails on a diff. Producer renders, consumer verifies.

That is deliberately the cheap option, taken because exactly one process definition exists and
it changes rarely — see the `process-graph-contract` task, which revisits it when the dashboard
becomes its own repo and neither side is the natural home any more.

ADR 0012 promises the visualization comes free: "the definition is the graph, the events are
the state". The graph half has to cross a build boundary to collect on that, and this is it.

**Why a generated file and not an import.** The dashboard image is built with `dashboard/` as
its Docker context (`publish-images.yml` declares it explicitly), so `src/reconciler/` is not
merely un-installed there — it is *outside the build context* and cannot be COPYed. The two
ways out were to widen the context to the repo root, or to project the definitions into a data
file the dashboard reads like it already reads `logical_levels.json`. This is the second,
because it keeps the tiers uncoupled: the dashboard consumes a *contract*, not a package, so it
needs neither pydantic nor pyyaml nor any knowledge of what a `signal` block means. It is also
the move this repo already makes at its other language boundary — `emit_event_schema.py` ->
`contracts/inferred_event.schema.json` -> `npm run gen:types`.

The cost is a file that can go stale — now in two repos rather than one. This repo's CI re-runs
this script and fails on a diff; `inference`'s CI diffs its copy against this one. Do not
hand-edit either output.

Run from the repo root:  uv run python scripts/emit_process_graph.py
Re-run whenever a `processes/*.yml` changes, and commit the result.
"""

import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from reconciler.definition import (  # noqa: E402
    GENESIS_STAGE,
    VOID_STAGE,
    ProcessDefinition,
    load_definitions,
)

OUT = ROOT / "contracts" / "processes.json"


def _label(stage_name: str) -> str:
    """`total_computed` -> "Total computed".

    Generic on purpose. The alternative — a lookup table of the eleven invoice stages — would
    put per-process knowledge in shipped code and leave process #2 unlabelled, which is the
    same mistake as hardcoding an event name in framework code. A stage name is already
    written for humans; it only needs presenting.
    """
    return stage_name.replace("_", " ").capitalize()


def _detail(stage: object) -> str:
    """The one-line "how" under a stage's name.

    Read out of the stage rather than described: an `act` says which action runs, an `await`
    says what it is watching and where. That keeps this projection honest as the definition
    grows — a new signal source shows up here without this file being touched.
    """
    if stage.kind == "act":
        return stage.action or ""
    signal = stage.signal or {}
    source = signal.get("source", "?")
    if "label" in signal:
        return f"{source}: {signal['label']}"
    if "from" in signal:
        return f"{source}: {signal['from']}"
    return source


def graph_of(definition: ProcessDefinition) -> dict:
    return {
        "name": definition.name,
        "label": _label(definition.name),
        "cycle_key": definition.cycle_key,
        # What makes a cycle exist. Rendered as provenance, and it is also the answer to
        # "why is there no invoice this month?" — a process with no schedule never self-opens.
        "opens": [
            {"via": o.via, "cron": o.cron} for o in definition.opens
        ],
        # `cycle_opened` is not a stage (the reconciler is handed cycles, it never creates
        # them) but it IS a recorded milestone and the first thing a reader looks for, so the
        # projection prepends it. Marked `genesis` so the UI can render it as given rather
        # than as something that was done.
        "stages": [
            {
                "name": GENESIS_STAGE,
                "label": _label(GENESIS_STAGE),
                "kind": "genesis",
                "after": [],
                "detail": "opened",
                "event": definition.event_name(GENESIS_STAGE),
            },
            *(
                {
                    "name": s.name,
                    "label": _label(s.name),
                    "kind": s.kind,
                    # An empty `after` on the first real stage means "follows the genesis
                    # fact", which is what the reconciler's linear scan already assumes.
                    "after": s.after or [GENESIS_STAGE],
                    "detail": _detail(s),
                    "event": definition.event_name(s.name),
                }
                for s in definition.stages
            ),
        ],
        # Terminal, and deliberately not in `stages`: a voided cycle is corrected by re-running
        # under a new key, never by amending, so this is an end state rather than a step.
        "void_event": definition.event_name(VOID_STAGE),
    }


def main() -> None:
    definitions = load_definitions(ROOT / "processes")
    payload = {"processes": [graph_of(d) for d in definitions]}
    OUT.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"wrote {OUT} ({len(payload['processes'])} process(es), "
          f"{sum(len(p['stages']) for p in payload['processes'])} stages)")


if __name__ == "__main__":
    main()
