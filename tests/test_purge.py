"""`reconciler.purge` — the cutoff rule and the guards, with no Gmail and no Kubernetes."""

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from reconciler import purge

ZURICH = ZoneInfo("Europe/Zurich")


def at(text: str) -> datetime:
    """A Zurich wall-clock time as an aware datetime."""
    return datetime.fromisoformat(text).replace(tzinfo=ZURICH)


# --- the cutoff: min(midnight, now - 12h) ---------------------------------------------------

@pytest.mark.parametrize("now,expected", [
    # Afternoon: midnight is the earlier bound, so yesterday's mail goes and today's stays.
    ("2026-11-03 14:00", "2026-11-03 00:00"),
    # Just after midnight: now - 12h (12:05 yesterday) is earlier, so yesterday evening's mail
    # still has its 12 hours.
    ("2026-11-04 00:05", "2026-11-03 12:05"),
    # Exactly noon: both bounds coincide.
    ("2026-11-03 12:00", "2026-11-03 00:00"),
])
def test_the_cutoff_is_the_earlier_of_midnight_and_twelve_hours_ago(now, expected):
    assert purge.cutoff(at(now), ZURICH) == at(expected)


@pytest.mark.parametrize("arrived,kept_until", [
    ("2026-11-03 08:00", "2026-11-04 00:00"),   # the day ends first
    ("2026-11-03 23:50", "2026-11-04 11:50"),   # the 12 hours end first
])
def test_the_owners_two_examples(arrived, kept_until):
    """The table in the task, as an assertion: a mail is due exactly when the cutoff passes it."""
    arrival, deadline = at(arrived), at(kept_until)
    assert purge.cutoff(deadline - timedelta(minutes=1), ZURICH) <= arrival
    assert purge.cutoff(deadline + timedelta(minutes=1), ZURICH) > arrival


@pytest.mark.parametrize("now,expected", [
    # Spring forward (2026-03-29, 02:00 -> 03:00): the day is 23 hours long, midnight still
    # exists once, and now - 12h is plain elapsed time.
    ("2026-03-29 14:00", "2026-03-29 00:00"),
    ("2026-03-29 10:00", "2026-03-28 21:00"),
    # Fall back (2026-10-25, 03:00 -> 02:00): 25 hours long.
    ("2026-10-25 14:00", "2026-10-25 00:00"),
    ("2026-10-25 10:00", "2026-10-24 23:00"),
])
def test_dst_days_keep_both_bounds(now, expected):
    got = purge.cutoff(at(now), ZURICH)
    assert got == at(expected)
    elapsed = at(now).astimezone(UTC) - got.astimezone(UTC)
    assert elapsed >= timedelta(hours=12), "the 12-hour floor must hold on DST days too"


def test_a_naive_now_is_refused():
    with pytest.raises(ValueError):
        purge.cutoff(datetime(2026, 11, 3, 14, 0), ZURICH)


@pytest.mark.parametrize("day", ["2026-11-03", "2026-03-29", "2026-10-25"])
def test_the_floor_holds_for_every_minute_of_a_day(day):
    """The one property the owner asked for, checked exhaustively rather than by example, on an
    ordinary day and both DST days. Elapsed time is measured in UTC: subtracting two Zurich
    datetimes would compare wall clocks, which is the bug this test exists to catch."""
    start = at(f"{day} 00:00").astimezone(UTC)
    for minute in range(25 * 60):
        now = start + timedelta(minutes=minute)
        assert now - purge.cutoff(now, ZURICH).astimezone(UTC) >= timedelta(hours=12)
        assert now - purge.cutoff(now.astimezone(ZURICH), ZURICH).astimezone(UTC) >= timedelta(hours=12)


# --- the guards ------------------------------------------------------------------------------

class FakeSource:
    def __init__(self, mails: list[tuple[int, dict]]):
        self.mails, self.asked = mails, []

    def candidates(self, signal, since):
        self.asked.append(signal)
        return list(self.mails)


class FakeTrasher:
    def __init__(self, fail_on: str | None = None):
        self.trashed, self.fail_on = [], fail_on

    def trash(self, *, message_id):
        if message_id == self.fail_on:
            raise RuntimeError("relay down")
        self.trashed.append(message_id)


NOW = at("2026-11-03 14:00")          # cutoff: 2026-11-03 00:00 Zurich
OLD = int(at("2026-11-02 09:00").timestamp())
YOUNG = int(at("2026-11-03 09:00").timestamp())


def mail(uid: str, sender: str = "mftradingguide@class.it") -> dict:
    return {"upstream_id": uid, "from": sender, "subject": f"subject {uid}"}


def run(mails, *, senders=("mftradingguide@class.it",), dry_run=True, trasher=None, **kw):
    source, trasher = FakeSource(mails), trasher or FakeTrasher()
    plan = purge.purge(source=source, trasher=trasher, senders=list(senders), now=NOW,
                       tz=ZURICH, dry_run=dry_run, **kw)
    return plan, source, trasher


def test_the_search_carries_the_cutoff_as_epoch_seconds():
    """Gmail's date operators do not follow Europe/Zurich; epoch seconds are exact."""
    _, source, _ = run([])
    (signal,) = source.asked
    assert signal["q"] == ("in:inbox from:mftradingguide@class.it "
                           f"before:{int(at('2026-11-03 00:00').timestamp())}")


def test_the_search_never_leaves_the_inbox():
    """Owner, 2026-11-03: mail that has left the inbox was handled by hand, so it is never
    touched. Asserted on every query, for every sender."""
    _, source, _ = run([], senders=["a@b.c", "d@e.f"])
    assert all(s["q"].startswith("in:inbox ") for s in source.asked)


def test_only_an_exact_sender_is_trashed():
    """Gmail's `from:` also matches display names and look-alike addresses."""
    plan, _, _ = run([(OLD, mail("a")),
                      (OLD, mail("b", "MFTradingGuide@Class.IT")),        # case differs: same address
                      (OLD, mail("c", "not-mftradingguide@class.it.evil")),
                      (OLD, mail("d", "someone@else.com"))])
    assert [m["upstream_id"] for m in plan.to_trash] == ["a", "b"]
    assert [m["upstream_id"] for m in plan.refused] == ["c", "d"]


def test_a_mail_the_date_header_calls_young_is_kept():
    """`before:` uses Gmail's received time; both must agree the mail is old."""
    plan, _, _ = run([(OLD, mail("old")), (YOUNG, mail("young"))])
    assert [m["upstream_id"] for m in plan.to_trash] == ["old"]
    assert [m["upstream_id"] for m in plan.refused] == ["young"]


def test_dry_run_trashes_nothing():
    plan, _, trasher = run([(OLD, mail("a"))], dry_run=True)
    assert len(plan.to_trash) == 1 and plan.trashed == 0 and trasher.trashed == []


def test_a_real_pass_trashes_what_it_planned():
    plan, _, trasher = run([(OLD, mail("a")), (OLD, mail("b"))], dry_run=False)
    assert trasher.trashed == ["a", "b"] and plan.trashed == 2


def test_max_per_run_bounds_a_pass_and_says_so():
    plan, source, trasher = run([(OLD, mail(str(i))) for i in range(5)], dry_run=False,
                                max_per_run=3)
    assert trasher.trashed == ["0", "1", "2"] and plan.truncated
    assert source.asked[0]["limit"] == 4, "one more than the cap, to detect 'more remain'"


def test_a_relay_failure_raises_after_what_it_already_did():
    """Trash is idempotent, so a partial pass needs no unwinding — but it must not report
    success: the controller turns the raise into a Failed status."""
    with pytest.raises(RuntimeError):
        run([(OLD, mail("a")), (OLD, mail("b"))], dry_run=False,
            trasher=FakeTrasher(fail_on="b"))


def test_one_message_found_by_two_senders_counts_once():
    plan, _, _ = run([(OLD, mail("a"))], senders=["mftradingguide@class.it", "x@y.z"])
    assert [m["upstream_id"] for m in plan.to_trash] == ["a"]


def test_blank_senders_are_ignored_not_searched():
    """`from:` with nothing after it would match every mail in the box."""
    _, source, _ = run([], senders=["mftradingguide@class.it", "  ", ""])
    assert [s["q"].split()[1] for s in source.asked] == ["from:mftradingguide@class.it"]


def test_now_in_utc_gives_the_same_cutoff():
    assert purge.cutoff(NOW.astimezone(UTC), ZURICH) == at("2026-11-03 00:00")
