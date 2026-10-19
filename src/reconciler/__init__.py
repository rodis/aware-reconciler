"""The process tier (ADR 0012) — long-running processes advanced by reconciliation.

A sibling of `inference`, deliberately **not** a subpackage of it: the boundary this tier
is built on is that *the reconciler acts and Aware observes*, and a package whose whole
purpose is calling out (email, PDF services, an LLM) does not belong inside the one that
must never do any of that.

The split inside mirrors `inference.runtime`:

- [`definition`](definition.py) — the `processes/*.yml` schema.
- [`core`](core.py) — **pure**: definitions + recorded milestones -> what to do next.
- `actions/` — the `act` implementations; every side effect lives here.
- `flow.py` — the Prefect entry point (the composition root).

**INVARIANT: `core` and `definition` import nothing but stdlib + pydantic/pyyaml**, so the
tier stays testable under CI's deliberately bare install (`pytest ruff pydantic pyyaml` plus
`pip install -e . --no-deps`) and so swapping the runner touches `flow.py` alone.
"""
