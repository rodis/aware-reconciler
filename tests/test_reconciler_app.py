"""Composition and scheduling (ADR 0012).

`app` is where a run gets wired; `flow.py` and `run.py` are two thin skins over it. What is
worth pinning here is the small amount of *logic* that lives at that seam — which sequence
number a new cycle takes, and which period a scheduled open covers — plus the duplication the
design could not avoid: a cron appears in the process definition AND in the runner's manifest,
and nothing but a test keeps them honest.

Since 2026-09-03 there are TWO runners (ADR 0012's amendment): Argo Workflows on the cluster
carries the frequent schedules, Prefect Cloud runs each flow once a day as an independent
backstop. So the scheduling assertions below come in pairs — what each runner owns, and the
quota ceiling that decided the split.
"""

import pathlib
from datetime import date

import pytest
import yaml

from reconciler import app
from reconciler.core import Cycle
from reconciler.definition import load_definitions

ROOT = pathlib.Path(__file__).resolve().parents[1]
PROCESSES = ROOT / "processes"
PREFECT_YAML = ROOT / "prefect.yaml"
# The CronWorkflows ride in the Stakater chart's `extraObjects`, so this values file IS the
# schedule — see the file's own header for why the image tag has to come from there.
RECONCILER_VALUES = ROOT / "deploy" / "kustomize" / "base" / "reconciler" / "values.yml"


def _definition(name="dreamhost_invoice"):
    return next(d for d in load_definitions(PROCESSES) if d.name == name)


def _cycle(key):
    return Cycle(key=key, process="dreamhost_invoice", user_id="rods", opened_at=0, context={})


# --- the sequence a new cycle takes ------------------------------------------------------------

def test_the_sequence_is_read_back_out_of_the_key():
    """Generated and compared rather than parsed: the same `str.format` that minted the key
    reads it back, so a change to the `{seq:03d}` spec cannot desynchronise the two."""
    definition = _definition()
    assert app.sequence_of(definition, "dh_invoice_2026_008", 2026) == 8
    assert app.sequence_of(definition, "dh_invoice_2026_008", 2025) is None   # wrong year
    assert app.sequence_of(definition, "something_else", 2026) is None


def test_the_next_sequence_is_one_past_the_highest_used_this_year():
    definition = _definition()
    cycles = [_cycle("dh_invoice_2026_007"), _cycle("dh_invoice_2026_008")]
    assert app.next_sequence(definition, cycles, 2026) == 9


def test_a_year_with_no_cycles_starts_at_one():
    assert app.next_sequence(_definition(), [], 2027) == 1


def test_last_year_s_cycles_do_not_advance_this_year_s_sequence():
    """The sequence is PER YEAR (the invoice number is not the month — that mistake is what
    the old design's padding bug came from), so January must restart at 1."""
    definition = _definition()
    cycles = [_cycle("dh_invoice_2026_011"), _cycle("dh_invoice_2027_001")]
    assert app.next_sequence(definition, cycles, 2027) == 2
    assert app.next_sequence(definition, cycles, 2028) == 1


def test_a_gap_left_by_a_hand_opened_cycle_is_not_reused():
    """One past the HIGHEST, not the first free slot. A number already sent to a client must
    never be minted again, and a gap is cheaper than a collision."""
    definition = _definition()
    cycles = [_cycle("dh_invoice_2026_001"), _cycle("dh_invoice_2026_005")]
    assert app.next_sequence(definition, cycles, 2026) == 6


# --- the period a scheduled open covers --------------------------------------------------------

@pytest.mark.parametrize("today,expected", [
    (date(2026, 9, 1), {"start": "2026-08-01", "end": "2026-08-31"}),
    (date(2026, 3, 1), {"start": "2026-02-01", "end": "2026-02-28"}),   # short month
    (date(2028, 3, 1), {"start": "2028-02-01", "end": "2028-02-29"}),   # leap year
    (date(2027, 1, 1), {"start": "2026-12-01", "end": "2026-12-31"}),   # across the year
])
def test_a_cron_on_the_first_invoices_the_month_that_just_ended(today, expected):
    assert app.previous_month(today) == expected


# --- the duplication that needed a guard -------------------------------------------------------

def test_every_scheduled_opener_has_a_prefect_deployment_on_the_same_cron():
    """The definition says WHEN a cycle opens; Prefect is what makes it happen. Those are two
    files, and a cron edited in one and not the other fails silently — as a month with no
    invoice, noticed when someone wonders why they were not paid."""
    deployments = yaml.safe_load(PREFECT_YAML.read_text())["deployments"]
    scheduled_crons = {c["cron"] for d in deployments for c in (d.get("schedules") or [])}

    for definition in load_definitions(PROCESSES):
        for opener in definition.opens:
            if opener.via == "schedule":
                assert opener.cron in scheduled_crons, (
                    f"{definition.name} opens on {opener.cron!r} but no prefect.yaml "
                    f"deployment is scheduled for it")


def test_the_deployments_point_at_flows_that_exist():
    """An entrypoint typo is only discovered at the first scheduled run — which for a monthly
    opener is up to a month later."""
    import ast

    deployments = yaml.safe_load(PREFECT_YAML.read_text())["deployments"]
    root = PREFECT_YAML.parent

    for deployment in deployments:
        path, _, func = deployment["entrypoint"].partition(":")
        source = (root / path).read_text()
        # Parsed, not imported: prefect is an optional extra and CI does not install it.
        names = {n.name for n in ast.walk(ast.parse(source))
                 if isinstance(n, ast.FunctionDef)}
        assert func in names, f"{deployment['name']} points at missing {path}:{func}"


def test_walking_a_fresh_cycle_needs_no_database(monkeypatch):
    """The preview path, and the reason `open_and_advance_flow` uses it.

    A cycle opened one line ago has nothing recorded to read — and a milestone reaches Neon
    ASYNCHRONOUSLY (gateway -> Kafka -> Vector -> persister), so looking it back up races the
    write. `advance` did exactly that and failed with `no cycle 'dh_invoice_2026_010'`; under
    dry-run it could never work, since a dry open writes nothing to find.

    So this asserts the property that makes the race impossible: walking a just-made cycle
    touches no database at all. `NEON_DATABASE_URL` is removed here, and a DB read would raise
    `ConfigurationError`.
    """
    for var in ("NEON_DATABASE_URL", "GMAIL_QUERY_URL", "LLM_RELAY_URL",
                "MAIL_RELAY_URL", "VECTOR_BASE_URL", "CRAFTMYPDF_API_KEY"):
        monkeypatch.delenv(var, raising=False)

    cycle = Cycle(key="dh_invoice_2026_099", process="dreamhost_invoice", user_id="rods",
                  opened_at=1788000000,
                  context={"invoice_number": 99,
                           "worked_period": {"start": "2026-08-01", "end": "2026-08-31"}})

    outcome = app.walk_fresh("dreamhost_invoice", cycle,
                             options=app.RunOptions(dry_run=True, processes_dir=PROCESSES))

    # It gets through the pure + notify stages and then stops at the first gate, because no
    # finder is wired without GMAIL_QUERY_URL. `None` is that loud stop, not a silent skip.
    assert outcome is None


def test_no_deployed_entrypoint_takes_kwargs():
    """Prefect builds a deployment's parameter schema from the flow signature, and renders
    `**kwargs` as a property named `kwargs` that is REQUIRED. The deployment is then
    unrunnable: every attempt returns `'kwargs' is a required property` and no run is created.

    `open_and_advance_flow` shipped that way and was broken from creation until someone tried
    to use it — which is the worst possible discovery time for the deployment whose whole job
    is ad-hoc invoices. Parsed rather than imported, since CI has no prefect.
    """
    import ast

    deployments = yaml.safe_load(PREFECT_YAML.read_text())["deployments"]
    root = PREFECT_YAML.parent

    for deployment in deployments:
        path, _, func = deployment["entrypoint"].partition(":")
        tree = ast.parse((root / path).read_text())
        fn = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name == func)
        assert fn.args.kwarg is None, (
            f"{deployment['name']} -> {func} takes **{fn.args.kwarg.arg}; Prefect would make "
            f"it a required parameter and the deployment could never run")


def test_the_wiring_imports_with_no_third_party_packages_installed():
    """CI installs with `pip --no-deps`, so every module a test reaches must import against a
    bare interpreter. `app.py` pulls in every adapter, and `adapters/neon.py` had a
    module-level `import psycopg` — which collapsed the ENTIRE pytest collection in CI while
    passing locally, where psycopg is installed. Simulated here rather than discovered there.
    """
    import importlib
    import sys

    # Exactly what CI does NOT install: it has pytest, ruff, pydantic and pyyaml,
    # then `pip install -e . --no-deps`. Everything else must be imported lazily.
    blocked = {"psycopg", "pymysql", "dotenv", "prefect", "anthropic", "quixstreams"}
    saved = {k: v for k, v in sys.modules.items()
             if k.startswith("reconciler") or k.split(".")[0] in blocked}
    try:
        for name in list(sys.modules):
            if name.startswith("reconciler") or name.split(".")[0] in blocked:
                del sys.modules[name]
        for name in blocked:
            sys.modules[name] = None      # makes `import <name>` raise ImportError

        importlib.import_module("reconciler.app")
        importlib.import_module("reconciler.run")
    finally:
        for name in list(sys.modules):
            if name.startswith("reconciler") or name.split(".")[0] in blocked:
                del sys.modules[name]
        sys.modules.update(saved)


def _cron_workflows():
    """The CronWorkflows, read out of the chart values that carry them.

    Parsed as plain YAML rather than rendered with kustomize+helm: the point is the schedule,
    and a test that shells out to `kustomize build --enable-helm` would need network, a vendored
    chart and ~17k lines of render to assert two cron strings.
    """
    values = yaml.safe_load(RECONCILER_VALUES.read_text())
    return {o["metadata"]["name"]: o for o in values["extraObjects"]
            if o["kind"] == "CronWorkflow"}


def test_the_cron_workflows_invoke_real_reconciler_subcommands():
    """A CronWorkflow's args are a string list nothing type-checks, and a typo there is only
    discovered at the next firing — which for the invoice advance is an hour of silence that
    looks exactly like "no cycle needed anything".

    So: every template's first arg must be a subcommand `reconciler.run` actually registers.
    Read from the argparse tree rather than a hardcoded list, so renaming a subcommand breaks
    here instead of in the cluster.
    """
    from reconciler import run as run_module

    # `main` builds its parser inline, so the subcommands are not introspectable without
    # running it. The `cmd_*` functions are the same surface by construction — every
    # subcommand is registered with `set_defaults(func=cmd_...)` — and reading them needs no
    # parser and produces no help output.
    subcommands = {name.removeprefix("cmd_").replace("_", "-")
                   for name in dir(run_module) if name.startswith("cmd_")}
    assert subcommands, "no cmd_* functions found in reconciler.run"

    values = yaml.safe_load(RECONCILER_VALUES.read_text())
    templates = next(o for o in values["extraObjects"]
                     if o["kind"] == "WorkflowTemplate")["spec"]["templates"]
    assert templates, "the WorkflowTemplate declares no templates"

    for template in templates:
        args = template["container"]["args"]
        assert args[0] in subcommands, (
            f"WorkflowTemplate template {template['name']!r} runs {args[0]!r}, which is not a "
            f"reconciler.run subcommand ({sorted(subcommands)})")

    # And every CronWorkflow must point at a template that exists — `entrypoint` is a free
    # string, and a stale one makes the workflow fail at creation with nothing scheduled.
    names = {t["name"] for t in templates}
    for name, cron in _cron_workflows().items():
        entrypoint = cron["spec"]["workflowSpec"]["entrypoint"]
        assert entrypoint in names, (
            f"CronWorkflow {name!r} entrypoint {entrypoint!r} is not one of {sorted(names)}")


# Which reconciler subcommand each deployed Prefect flow ultimately performs. This is the
# correspondence between the two runners, and it is declared rather than inferred so that
# renaming a flow or adding one FAILS here instead of quietly escaping the collision check
# below.
FLOW_ACTS = {
    "open_cycle_flow": {"open"},
    "advance_flow": {"reconcile"},
    "open_and_advance_flow": {"open", "reconcile"},
    "sweep_tasks_flow": {"sweep-tasks"},
}


def _minutes(field: str) -> set[int]:
    """Which minutes-of-hour a cron's minute field can fire on: `*`, `*/n` and lists."""
    if field == "*":
        return set(range(60))
    fired: set[int] = set()
    for part in field.split(","):
        if part.startswith("*/"):
            fired |= set(range(0, 60, int(part[2:])))
        else:
            fired.add(int(part))
    return fired


def test_the_cron_workflows_use_the_plural_schedules_field():
    """`schedules` is a LIST in Argo Workflows v4; the singular `schedule:` was removed.

    Nothing else in this repo can catch that. We install the CRDs minified
    (`crds.full: false` in deploy/workflows/.../values.yml, because the full ones only install
    via a pre-install hook Job that is the wrong shape under Argo CD), and minified CRDs carry
    `x-kubernetes-preserve-unknown-fields` — so the API server accepts a field that no longer
    exists, `kubectl get cronworkflow` shows a healthy object with the schedule right there in
    the spec, and the ONLY signal is a controller log line: "cron workflow must have at least
    one schedule". The crons simply never fire.

    That shipped on 2026-09-03 and neither cron fired once. It cost a full deploy cycle to find
    and was invisible to every check that existed, which is the whole argument for this test:
    it is the validation the minified CRDs gave up.
    """
    values = yaml.safe_load(RECONCILER_VALUES.read_text())
    crons = [o for o in values["extraObjects"] if o["kind"] == "CronWorkflow"]
    assert crons, "no CronWorkflows found — has the schedule moved?"

    for cron in crons:
        spec = cron["spec"]
        name = cron["metadata"]["name"]
        assert "schedule" not in spec, (
            f"CronWorkflow {name!r} uses the singular `schedule:`, removed in Argo Workflows "
            f"v4. The API will accept it and the controller will never fire it. Use "
            f"`schedules:` with a list.")
        assert isinstance(spec.get("schedules"), list) and spec["schedules"], (
            f"CronWorkflow {name!r} must set `schedules:` to a non-empty list")
        for entry in spec["schedules"]:
            assert len(entry.split()) == 5, (
                f"CronWorkflow {name!r} schedule {entry!r} is not a 5-field cron expression")


def test_the_two_runners_never_do_the_same_job_in_the_same_minute():
    """Two runners performing the same act at the same instant is a duplicate-action bug.

    Both are pure functions of recorded milestones, which is what makes re-running safe
    *sequentially* — but a milestone reaches Neon ASYNCHRONOUSLY (gateway -> Kafka -> Vector ->
    persister), the very race `walk_fresh` exists to avoid. So two simultaneous advances both
    read "approval mail not sent" and both send it. The visible cost is two identical mails to
    a client, and neither runner would report a problem.

    `concurrencyPolicy: Forbid` guards this inside Argo and knows nothing about Prefect, so the
    separation has to live in the crons. It is asserted because the collision is invisible on
    inspection: Argo's hourly `17 * * * *` and a Prefect daily `17 6 * * *` look nothing alike
    and fire together every morning — which is exactly what they did when the schedules were
    first written.

    Compared PER JOB, not globally: the monthly `open` and the task sweep both touching minute
    0 is harmless, because they act on unrelated state. Only the same subcommand matters.
    """
    values = yaml.safe_load(RECONCILER_VALUES.read_text())
    templates = {tpl["name"]: tpl["container"]["args"][0]
                 for tpl in next(o for o in values["extraObjects"]
                                 if o["kind"] == "WorkflowTemplate")["spec"]["templates"]}

    argo: dict[str, list[tuple[str, str]]] = {}
    for name, cron in _cron_workflows().items():
        act = templates[cron["spec"]["workflowSpec"]["entrypoint"]]
        for schedule in cron["spec"]["schedules"]:
            argo.setdefault(act, []).append((name, schedule))

    for deployment in yaml.safe_load(PREFECT_YAML.read_text())["deployments"]:
        func = deployment["entrypoint"].partition(":")[2]
        assert func in FLOW_ACTS, (
            f"{deployment['name']} runs {func}, which FLOW_ACTS does not describe — say which "
            f"reconciler subcommand it performs so the collision check can cover it")

        for cron in deployment.get("schedules") or []:
            fires = _minutes(cron["cron"].split()[0])
            for act in FLOW_ACTS[func]:
                for argo_name, argo_cron in argo.get(act, []):
                    clash = fires & _minutes(argo_cron.split()[0])
                    assert not clash, (
                        f"prefect {deployment['name']!r} ({cron['cron']!r}) and argo "
                        f"{argo_name!r} ({argo_cron!r}) both run {act!r} at minute(s) "
                        f"{sorted(clash)} — two runners acting on the same cycle at once can "
                        f"duplicate an outbound action")


def test_prefect_is_a_daily_backstop_not_an_hourly_runner():
    """The quota regression this whole move exists to prevent.

    Prefect Managed bills a **60-second minimum per run** against a workspace limit of 30,000
    compute-seconds/month — 500 runs. An hourly deployment is 744 of them, 149% of quota,
    exhausting the workspace around the 17th and taking the backstop down with it. The failure
    is invisible from here: runs simply stop being created, and a tier whose whole job is to
    notice things quietly stops noticing.

    So no schedule in `prefect.yaml` may fire more than once a day. Asserted structurally
    rather than by listing crons, because the next deployment added would otherwise inherit
    the hourly habit.
    """
    deployments = yaml.safe_load(PREFECT_YAML.read_text())["deployments"]
    crons = [(d["name"], c["cron"]) for d in deployments
             for c in (d.get("schedules") or [])]
    assert crons, "prefect.yaml schedules nothing at all — is the backstop gone?"

    for name, cron in crons:
        minute, hour, *_ = cron.split()
        assert "*" not in minute and "/" not in minute, (
            f"{name} fires every minute-of-hour ({cron!r}) — that is >= 1440 runs/month "
            f"against a 500-run quota")
        assert "*" not in hour and "/" not in hour, (
            f"{name} fires hourly ({cron!r}) = 744 runs/month, 149% of the 500-run Prefect "
            f"quota. Frequent scheduling belongs on Argo Workflows; Prefect is the daily "
            f"backstop (ADR 0012 amendment)")


# --- the cutover: two entrypoints, two stores ---------------------------------------------------

def _template_env(template: dict) -> dict[str, str]:
    return {e["name"]: e["value"] for e in template["container"].get("env", [])}


def _templates() -> dict[str, dict]:
    values = yaml.safe_load(RECONCILER_VALUES.read_text())
    return {t["name"]: t for t in next(o for o in values["extraObjects"]
                                       if o["kind"] == "WorkflowTemplate")["spec"]["templates"]}


def test_both_reconciler_templates_declare_their_store():
    """`RECONCILER_STORE` has to be written out on every template, not inherited or defaulted.

    Two things make its absence dangerous rather than merely untidy. The code default is
    `neon` (`app.py::_store_choice`) and it is **silent**, so an omission reads exactly like a
    deliberate choice. And the templates share a YAML anchor: `<<: *reconciler` followed by its
    own `env:` REPLACES the merged list, so adding a variable to the anchor's env reaches one
    template and silently skips the other — which is precisely how the two templates came to
    read different stores in the first place.

    Being on the wrong store is not a visible failure either. The reconciler would read a
    store that is merely *behind*, find a stage's milestone missing, and redo the stage — for
    `invoice_emailed`, by sending the invoice again.
    """
    for name, template in _templates().items():
        env = _template_env(template)
        assert env.get("RECONCILER_STORE") in {"neon", "mysql"}, (
            f"WorkflowTemplate template {name!r} does not say which store it reads "
            f"(got {env.get('RECONCILER_STORE')!r}). The code default is `neon` and it is "
            f"silent; say it here so the manifest is the record of the cutover")
        assert env.get("MYSQL_SSL_CA"), (
            f"template {name!r} has no MYSQL_SSL_CA. There is no CA-less mode — PyMySQL treats "
            f"an empty `ssl` mapping as falsy and connects in PLAINTEXT — so a template that "
            f"flips to `mysql` without this would silently stop using TLS")


def test_a_sweep_cannot_outlive_the_gap_to_its_next_firing():
    """`activeDeadlineSeconds` must be shorter than the interval, or `Forbid` stops the clock.

    The two settings drifted apart when the sweep went from `*/15` to `*/5` and its 600-second
    deadline was left alone: a hung run would then still hold the slot when the next firing
    came due, `concurrencyPolicy: Forbid` would drop that one, and the one after, for as long
    as the hang lasted. The schedule would look healthy and fire nothing — the silent stall
    this tier exists to avoid, reintroduced by a number nobody changed.

    Only checked for `*/n` minute schedules; an hourly cron's gap is obvious and its deadline
    is set against the work, not the grid.
    """
    templates = _templates()
    for name, cron in _cron_workflows().items():
        spec = cron["spec"]
        deadline = spec["workflowSpec"].get("activeDeadlineSeconds")
        if deadline is None:
            continue
        for schedule in spec["schedules"]:
            minute = schedule.split()[0]
            if not minute.startswith("*/"):
                continue
            gap = int(minute[2:]) * 60
            assert deadline < gap, (
                f"CronWorkflow {name!r} fires every {gap}s but lets a run live {deadline}s — a "
                f"hung run holds the `Forbid` slot past the next firing and the schedule "
                f"silently stops")
            assert spec.get("startingDeadlineSeconds", 0) < gap, (
                f"CronWorkflow {name!r} would catch up a missed firing up to "
                f"{spec['startingDeadlineSeconds']}s late, which at a {gap}s interval collides "
                f"with the next one")
        assert spec["workflowSpec"]["entrypoint"] in templates
