# aware-reconciler

A **process tier**: long-running, multi-step processes described as data and advanced by a
reconciler that is a *pure function of the milestones recorded so far*.

It is the scheduling and reconciliation half of [Aware](https://github.com/rodis/inference), split
out into its own repo. The other half — the inference engine that derives high-level events from
sensor data — stays there.

## The idea

A process is a YAML file. Each stage is either something the tier **does** (`act`) or something it
**waits for** (`await`):

```yaml
stages:
  - name: computed_lines
    kind: act
    action: lines.worked_days

  - name: invoice_approved
    kind: await
    after: [approval_requested]
    signal:
      source: classify
      from: accounting@example.com
      classify: >-
        Does this email approve the invoice?
```

`after` is a list, so a process is a DAG rather than a chain.

**The reconciler never remembers where it got to.** On every run it reads the milestones already
recorded, works out the frontier, and acts on it. That is the whole design, and most of what this
repo is worth reading for:

- **No resume tokens, no correlation IDs, no suspended-run state.** The saga problem is avoided
  rather than solved.
- **Every run is safe to repeat.** Re-running advances nothing that has already advanced, so a
  missed schedule is a latency problem, never a correctness one.
- **A missed `await` is self-healing.** If the fact it waits for shows up late, the next run sees it.

The one act that is *not* idempotent is opening a cycle — a second open mints a second cycle — so
opening is deliberately kept on a separate runner, and no schedule in this repo can do it.

## What it runs on

Argo Workflows `CronWorkflow`s on Kubernetes, invoking a plain CLI:

```
reconciler reconcile --process <name> [--dry-run]
reconciler sweep-tasks [--dry-run]
```

`--dry-run` writes nothing and sends nothing, and prints exactly what would have happened. It is
the intended way to read a process's current state.

State lives in MySQL (`migrations/*.sql`, applied in order — three tables, no framework). A Postgres
adapter is also present; the two are interchangeable behind one `Store` seam, selected by
`RECONCILER_STORE`.

## Observation is separate from action

The tier records what it does by POSTing milestones at an ingest gateway, the same way any other
producer does. It does not write to the timeline database and nothing downstream has to know it
exists. *The reconciler acts; Aware observes.*

That is what keeps this repo independent: the only coupling left to the rest of Aware is
`contracts/processes.json`, a generated projection of the definitions that the dashboard renders.
Producer renders it here, consumer verifies its copy.

## Layout

| path | |
| :-- | :-- |
| `processes/` | the definitions — data, not code |
| `src/reconciler/core.py` | `Cycle`, `Milestone`, and the ports. Imports nothing |
| `src/reconciler/adapters/` | the outside world: MySQL, Postgres, Gmail, mail, LLM, PDF |
| `src/reconciler/actions/` | what an `act` stage can do |
| `migrations/` | numbered SQL, applied in order |
| `deploy/` | kustomize + the CronWorkflows |

```bash
uv sync --extra dev
uv run pytest
```

## A note on the example process

`processes/dreamhost_invoice.yml` is the real shape of a working process, with **placeholders where
the original carried commercial values** — the day rate and the template id in particular. It is
here to show what a non-trivial definition looks like, not to be run as-is: `lines.worked_days`
treats `day_rate` as required and never defaults it, so a wrong value produces a wrong invoice
rather than an error.

## Background

The design and the reasoning behind it are in ADR 0012 in the
[Aware repo](https://github.com/rodis/inference). Worth knowing before reading the code:

- **LLM classification lives here, not in the inference runtime.** Blocking I/O and
  non-determinism do not belong in a single-partition stream router.
- **Correction is re-run, never amend.** Voiding a cycle is terminal; the replacement takes a new
  key and inherits the number.
- **Every line-producing stage may produce zero lines**, or the definition language grows
  conditionals.
