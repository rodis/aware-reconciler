"""The email todo sweep (reconciler.tasks).

The sweep is the only thing that can close a task from outside the dashboard, and it decides
that by *absence* — a task is done because Gmail no longer reports it. Deciding anything from
absence is dangerous, so most of what is pinned here is the guard rails around that inference
rather than the happy path.

No network and no database: `diff` is pure, and `sweep` takes its Gmail client and its store as
arguments.
"""

import pytest

from reconciler.tasks import (
    CLOSED_EVENT,
    CLOSED_VIA_SWEEP,
    OPENED_EVENT,
    OpenTask,
    closed_body,
    diff,
    opened_body,
    sweep,
)

DAY = 86400
NOW = 1788000000                      # a fixed "today" so ages are arithmetic, not wall-clock


def candidate(uid, subject="Renew car insurance", name="AXA", addr="insurance@example.com"):
    """One normalised Gmail item, in `adapters.gmail.normalise`'s shape."""
    return {"upstream_id": uid, "subject": subject, "from": addr, "from_name": name,
            "from_domain": addr.split("@")[-1], "gmail_thread_id": "t" + uid,
            "snippet": "…"}


def open_task(uid, opened_at=NOW - 3 * DAY, subject="Renew car insurance"):
    return OpenTask(upstream_id=uid, subject=subject, from_name="AXA",
                    from_address="insurance@example.com", opened_at=opened_at)


# --- the diff, which is the whole decision -----------------------------------------------------

def test_a_labelled_mail_we_have_no_open_for_is_opened():
    """The repair direction. It is not only for first runs: the n8n Gmail Trigger is documented
    to drop messages, so a label applied while the connector hiccupped would otherwise never
    become a task at all — and nothing would ever say so."""
    plan = diff({"m1": candidate("m1")}, {})
    assert [c["upstream_id"] for c in plan.to_open] == ["m1"]
    assert plan.to_close == []


def test_an_open_task_no_longer_labelled_is_closed():
    """The direction that cannot come from a trigger at all — Gmail reports a label being ADDED
    and never one being removed, so unlabelling on your phone is invisible to a connector."""
    plan = diff({}, {"m1": open_task("m1")})
    assert [t.upstream_id for t in plan.to_close] == ["m1"]
    assert plan.to_open == []


def test_agreement_produces_nothing():
    """The normal hour. `empty` is what keeps a quiet sweep from looking like a failed one."""
    plan = diff({"m1": candidate("m1")}, {"m1": open_task("m1")})
    assert plan.empty


def test_both_directions_in_one_sweep():
    plan = diff({"m1": candidate("m1"), "m2": candidate("m2", "Lease addendum")},
                {"m2": open_task("m2"), "m3": open_task("m3", subject="Service reminder")})
    assert [c["upstream_id"] for c in plan.to_open] == ["m1"]
    assert [t.upstream_id for t in plan.to_close] == ["m3"]


# --- the guard that stops the sweep destroying long-lived tasks --------------------------------

def test_a_task_older_than_the_search_horizon_is_never_closed():
    """THE failure this function must not have.

    The Gmail search is bounded (`--lookback`), so a mail older than the window is simply not in
    the answer — which is indistinguishable from "not labelled any more". Closing on that basis
    would retire every long-lived task the moment someone shortened the lookback, silently and
    all at once. An insurance renewal or a passport appointment can legitimately sit for months.
    """
    horizon = NOW - 30 * DAY
    ancient = {"m1": open_task("m1", opened_at=NOW - 200 * DAY)}
    assert diff({}, ancient, stale_horizon=horizon).to_close == []


def test_a_task_inside_the_horizon_is_still_closed():
    """The guard must not be so broad that it stops the sweep working."""
    horizon = NOW - 30 * DAY
    recent = {"m1": open_task("m1", opened_at=NOW - 3 * DAY)}
    assert [t.upstream_id for t in diff({}, recent, stale_horizon=horizon).to_close] == ["m1"]


def test_a_task_with_no_recorded_open_time_is_closed():
    """`opened_at` of 0 means we could not read one. Treating that as "before the horizon" would
    make such a task permanently uncloseable, which is worse than closing it — the label really
    is gone, and the dashboard tick can always reopen by re-labelling."""
    plan = diff({}, {"m1": open_task("m1", opened_at=0)}, stale_horizon=NOW - 30 * DAY)
    assert [t.upstream_id for t in plan.to_close] == ["m1"]


# --- the wire bodies ---------------------------------------------------------------------------

def test_an_open_carries_the_same_fields_the_connector_emits():
    """Two producers, one event name. If they disagreed on shape, a consumer would be reading
    one of them wrong — and which one it got would depend on a race."""
    body = opened_body(candidate("m1"), user_id="rods", label="aware/todo", when=NOW)
    assert body["event_name"] == OPENED_EVENT
    assert body["user_id"] == "rods"
    # exactly the parking connector's contract fields
    for field in ("upstream_id", "gmail_thread_id", "from", "from_name", "from_domain",
                  "subject", "snippet", "label", "timestamp"):
        assert field in body, f"{field} missing — the connector emits it"
    assert "closed_via" not in body


def test_an_open_is_stamped_with_the_mail_s_time_not_the_sweep_s():
    """A task's age is how long the MAIL has been sitting. Stamping discovery time would reset
    every age to zero the first time a repair sweep re-opened something, and the board's whole
    job is showing what is rotting."""
    body = opened_body(candidate("m1"), user_id="rods", label="aware/todo", when=NOW - 12 * DAY)
    assert body["timestamp"] == NOW - 12 * DAY


def test_a_close_carries_the_subject_so_the_timeline_row_reads():
    """`upstream_id` identifies it, but the close lands on the Day timeline as its own row and
    "Email task closed" with no subject is unreadable without a join."""
    body = closed_body(open_task("m1"), user_id="rods", label="aware/todo",
                       when=NOW, closed_via=CLOSED_VIA_SWEEP)
    assert body["event_name"] == CLOSED_EVENT
    assert body["subject"] == "Renew car insurance"
    assert body["closed_via"] == CLOSED_VIA_SWEEP
    assert body["timestamp"] == NOW           # when YOU finished it, not when the mail arrived
    assert body["open_seconds"] == 3 * DAY


def test_a_close_with_no_known_open_time_reports_no_duration():
    """None rather than a fabricated 0 — "open for no time at all" is a claim, and we do not
    have the evidence for it."""
    assert closed_body(open_task("m1", opened_at=0), user_id="rods", label="aware/todo",
                       when=NOW, closed_via=CLOSED_VIA_SWEEP)["open_seconds"] is None


# --- the sweep end to end, with fakes ----------------------------------------------------------

class FakeGmail:
    def __init__(self, items):
        self.items = items
        self.asked = []

    def candidates(self, signal, since):
        self.asked.append((signal, since))
        return self.items


class FakeStore:
    def __init__(self, tasks):
        self.tasks = tasks

    def open_tasks(self, user_id, label):
        return self.tasks


class FakeSink:
    def __init__(self):
        self.emitted = []

    def emit(self, payload):
        self.emitted.append(payload)


def test_the_sweep_emits_one_event_per_difference():
    gmail = FakeGmail([(NOW - 2 * DAY, candidate("m1"))])
    sink = FakeSink()
    plan = sweep(source=gmail, store=FakeStore({"m2": open_task("m2")}), sink=sink,
                 user_id="rods", now=NOW)

    assert len(plan.to_open) == 1 and len(plan.to_close) == 1
    names = [e["event_name"] for e in sink.emitted]
    assert names == [OPENED_EVENT, CLOSED_EVENT]
    # the open is backdated to the mail, the close is stamped now
    assert sink.emitted[0]["timestamp"] == NOW - 2 * DAY
    assert sink.emitted[1]["timestamp"] == NOW


def test_the_sweep_asks_gmail_for_the_label_and_bounds_the_window():
    gmail = FakeGmail([])
    sweep(source=gmail, store=FakeStore({}), sink=FakeSink(), user_id="rods",
          label="aware/todo", lookback_days=30, now=NOW)
    signal, since = gmail.asked[0]
    assert signal["label"] == "aware/todo"
    assert since == NOW - 30 * DAY


def test_a_quiet_sweep_emits_nothing():
    sink = FakeSink()
    plan = sweep(source=FakeGmail([(NOW, candidate("m1"))]),
                 store=FakeStore({"m1": open_task("m1")}), sink=sink, user_id="rods", now=NOW)
    assert plan.empty and sink.emitted == []


def test_the_horizon_guard_is_applied_by_the_sweep_not_just_by_diff():
    """`sweep` derives the horizon from `lookback_days` and must pass it down; forgetting to is
    the mistake that turns a shortened lookback into a mass close."""
    sink = FakeSink()
    plan = sweep(source=FakeGmail([]),
                 store=FakeStore({"old": open_task("old", opened_at=NOW - 200 * DAY)}),
                 sink=sink, user_id="rods", lookback_days=30, now=NOW)
    assert plan.to_close == [] and sink.emitted == []


@pytest.mark.parametrize("label", ["aware/todo", "aware/reading"])
def test_the_label_rides_on_every_event(label):
    """The board and the sweep both filter on it, so a second label is a second list rather than
    a mixed one — and that costs no code, only a parameter."""
    sink = FakeSink()
    sweep(source=FakeGmail([(NOW, candidate("m1"))]), store=FakeStore({"m2": open_task("m2")}),
          sink=sink, user_id="rods", label=label, now=NOW)
    assert {e["label"] for e in sink.emitted} == {label}
