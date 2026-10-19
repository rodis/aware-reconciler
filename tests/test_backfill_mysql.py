"""The Neon -> MySQL backfill transforms (`scripts/backfill_mysql.py`).

Loaded by path, like `test_dashboard_places.py`: a script is not a package on this path. Its
transforms are pure, which is the point — the migration's whole safety claim is that MySQL is
a *projection* of the event log and can be rebuilt from it, and that claim is only as good as
these three functions.

So what is pinned here is the ways a rebuild could be quietly wrong rather than loudly broken:
a context that gained envelope keys, a reopened task buried at migration time, a stage name
derived from the wrong prefix. Each of those produces a database that looks fine and disagrees
with Neon.
"""

import importlib.util
import json
import pathlib

import pytest

_PATH = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "backfill_mysql.py"
_spec = importlib.util.spec_from_file_location("backfill_mysql", _PATH)
backfill = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(backfill)

NOW = 1788000000
DAY = 86400


def process_event(name, ts, *, cycle="dh_invoice_2026_004", process="dreamhost_invoice",
                  user="rods", **body):
    """One row as `_PROCESS_EVENTS_SQL` returns it: (name, user_id, ts, message)."""
    message = {"id": "u-1", "name": name, "user_id": user, "timestamp": ts,
               "cycle_key": cycle, "process": process, **body}
    return (name, user, ts, message)


def task_event(name, ts, uid="mail-1", *, user="rods", **body):
    return (name, user, ts, {"upstream_id": uid, "label": "aware/todo", **body})


# --- cycles -----------------------------------------------------------------------------------

def test_only_the_genesis_event_makes_a_cycle():
    """A cycle exists because `cycle_opened` was recorded — nothing else creates one. If later
    milestones also minted cycles, every cycle would appear eleven times with the wrong
    `opened_at`."""
    rows = backfill.cycle_rows([
        process_event("dreamhost_invoice_cycle_opened", NOW, invoice_number=4),
        process_event("dreamhost_invoice_total_computed", NOW + 60, total=1234),
    ])
    assert len(rows) == 1
    assert rows[0][0] == "dh_invoice_2026_004"
    assert rows[0][3] == NOW


def test_context_keeps_exactly_what_the_postgres_reader_would_have_built():
    """The envelope keys must come OUT and the context keys must stay IN.

    This equality is what makes the replay diff meaningful: if the backfilled `context`
    differs from what `NeonMilestones.cycles` builds in memory, the comparison that validates
    the whole migration is comparing two different things and can pass for the wrong reason.
    """
    rows = backfill.cycle_rows([
        process_event("dreamhost_invoice_cycle_opened", NOW,
                      invoice_number=4, worked_period={"start": "2026-08-01"}),
    ])
    assert json.loads(rows[0][4]) == {"invoice_number": 4,
                                      "worked_period": {"start": "2026-08-01"}}


def test_a_context_field_called_name_is_not_mistaken_for_the_envelope():
    """`name` is an envelope key, so a context field that happens to share the name would be
    stripped. Pinned as a KNOWN limitation rather than a passing test pretending otherwise —
    if a future process opens with a `name` in its context, this is where it breaks and this
    comment is the explanation."""
    rows = backfill.cycle_rows([process_event("dreamhost_invoice_cycle_opened", NOW, foo=1)])
    assert "name" not in json.loads(rows[0][4])


def test_a_genesis_emitted_twice_collapses_to_one_cycle():
    """Ingest is at-most-once with no de-duplication, so a retry that got through twice is a
    real shape. Two rows would fail the primary key mid-backfill, which is a bad way to find
    out."""
    rows = backfill.cycle_rows([
        process_event("dreamhost_invoice_cycle_opened", NOW, invoice_number=4),
        process_event("dreamhost_invoice_cycle_opened", NOW + 5, invoice_number=4),
    ])
    assert len(rows) == 1


# --- milestones -------------------------------------------------------------------------------

def test_the_stage_is_the_name_minus_its_process_prefix():
    rows = backfill.milestone_rows([
        process_event("dreamhost_invoice_data_approved", NOW, matched_at=NOW - 10),
    ])
    assert rows[0][1] == "data_approved"


def test_the_genesis_is_a_milestone_too():
    """A stage may depend on nothing and still need somewhere to look back to — the Postgres
    reader includes it under `cycle_opened` for the same reason."""
    rows = backfill.milestone_rows([
        process_event("dreamhost_invoice_cycle_opened", NOW, invoice_number=4)])
    assert rows[0][1] == "cycle_opened"


def test_a_milestone_that_cannot_be_placed_on_a_stage_raises():
    """Silently dropping it would make the cycle look like it stalled at the stage before,
    which is indistinguishable from legitimately waiting — the one failure this tier refuses
    to fake."""
    bad = ("totally_different_name", "rods", NOW,
           {"cycle_key": "k", "process": "dreamhost_invoice"})
    with pytest.raises(ValueError, match="cannot derive a stage name"):
        backfill.milestone_rows([bad])


def test_a_re_emitted_milestone_keeps_the_later_one():
    rows = backfill.milestone_rows([
        process_event("dreamhost_invoice_total_computed", NOW, total=1),
        process_event("dreamhost_invoice_total_computed", NOW + 60, total=2),
    ])
    assert len(rows) == 1
    assert json.loads(rows[0][3])["total"] == 2


# --- tasks ------------------------------------------------------------------------------------

def test_an_open_with_no_close_is_open():
    rows = backfill.task_rows([task_event("email_labeled_todo", NOW, subject="Renew insurance")])
    assert rows[0][0] == "mail-1"
    assert rows[0][3] == "Renew insurance"
    assert rows[0][7] is None          # closed_at


def test_a_close_after_the_open_closes_it():
    rows = backfill.task_rows([
        task_event("email_labeled_todo", NOW),
        task_event("email_task_closed", NOW + DAY, closed_via="dashboard"),
    ])
    assert rows[0][7] == NOW + DAY
    assert rows[0][8] == "dashboard"


def test_a_relabelled_mail_is_reopened_not_buried():
    """THE rule this collapse exists to preserve. Re-labelling a mail finished last month is a
    legitimate way to reopen it, so a close OLDER than the newest open is spent.

    Getting this backwards buries every reopened task at the instant of migration, with
    nothing to notice: the board simply shows fewer tasks than Gmail does, and the sweep keeps
    agreeing because its store says closed.
    """
    rows = backfill.task_rows([
        task_event("email_labeled_todo", NOW - 30 * DAY),
        task_event("email_task_closed", NOW - 29 * DAY, closed_via="sweep"),
        task_event("email_labeled_todo", NOW, subject="Renew insurance"),   # relabelled today
    ])
    assert rows[0][6] == NOW           # opened_at is the NEW open
    assert rows[0][7] is None, "a close older than the newest open is spent"


def test_a_close_with_no_open_is_skipped_rather_than_invented(capsys):
    """Inventing the row would mean guessing `opened_at`, and a task whose age is fiction is
    worse on the board than one that is absent."""
    rows = backfill.task_rows([task_event("email_task_closed", NOW, closed_via="sweep")])
    assert rows == []
    assert "no matching open" in capsys.readouterr().out


def test_the_newest_open_supplies_the_prose():
    """Sender and subject come from the latest open, not the first — a resent thread can
    change them, and the board should show what is in the mailbox now."""
    rows = backfill.task_rows([
        task_event("email_labeled_todo", NOW - DAY, subject="old", from_name="A"),
        task_event("email_labeled_todo", NOW, subject="new", from_name="B"),
    ])
    assert (rows[0][3], rows[0][4]) == ("new", "B")


def test_tasks_are_independent_of_each_other():
    rows = {r[0]: r for r in backfill.task_rows([
        task_event("email_labeled_todo", NOW, uid="mail-1"),
        task_event("email_labeled_todo", NOW, uid="mail-2"),
        task_event("email_task_closed", NOW + DAY, uid="mail-2", closed_via="sweep"),
    ])}
    assert rows["mail-1"][7] is None
    assert rows["mail-2"][7] == NOW + DAY
