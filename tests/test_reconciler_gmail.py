"""Tests for the Gmail query adapter (ADR 0012).

This mapping used to live in an n8n Set node. It is here so it can be tested: a previous
version of the parking connector stringified mailparser's `from` object and shipped
"[objectobject]" as from_domain to 15 rows, which is exactly the class of bug a unit test
catches and a GUI expression does not.
"""

import json

import pytest

from reconciler.adapters.gmail import (
    N8nGmailQuery,
    build_query,
    message_time,
    normalise,
)

RAW = {
    "id": "18f2a",
    "threadId": "18f00",
    "subject": "Invoice 8 — please check and approve",
    "date": "2026-08-16T09:15:00Z",
    "labelIds": ["INBOX", "Label_7"],
    "from": {"value": [{"address": "Bot@Example.COM", "name": "Invoice Bot"}]},
    "text": "Invoice 8\n\n  Consulting: EUR 18,400.00   ",
}


# --- query building ----------------------------------------------------------------------

def test_a_label_becomes_gmail_search_syntax():
    assert build_query({"label": "aware/invoice-approved"}) == "label:aware/invoice-approved"


def test_a_sender_filter_is_added():
    assert build_query({"from": "accounting@example.com"}) == "from:accounting@example.com"


def test_label_and_sender_combine():
    q = build_query({"label": "aware/x", "from": "a@b.com"})
    assert q == "label:aware/x from:a@b.com"


def test_an_empty_signal_yields_an_empty_query():
    assert build_query({}) == ""


# --- normalising the raw Gmail item --------------------------------------------------------

def test_the_sender_object_is_unwrapped_not_stringified():
    """The [objectobject] bug, pinned."""
    out = normalise(RAW)
    assert out["from"] == "Bot@Example.COM"
    assert out["from_name"] == "Invoice Bot"
    assert out["from_domain"] == "example.com"      # lowercased for comparison


def test_a_missing_sender_degrades_to_empty_strings():
    out = normalise({"id": "x"})
    assert out["from"] == "" and out["from_name"] == "" and out["from_domain"] == ""


def test_a_malformed_sender_does_not_raise():
    for bad in ({"from": None}, {"from": {}}, {"from": {"value": []}},
                {"from": {"value": [None]}}):
        assert normalise({"id": "x", **bad})["from"] == ""


def test_the_subject_is_carried_through_for_correlation():
    # correlate_on: subject is what ties evidence to a cycle, so this field is load-bearing.
    assert normalise(RAW)["subject"] == "Invoice 8 — please check and approve"


def test_the_snippet_is_whitespace_collapsed_and_bounded():
    out = normalise(RAW)
    assert out["snippet"] == "Invoice 8 Consulting: EUR 18,400.00"
    assert len(normalise({"id": "x", "text": "a" * 5000})["snippet"]) == 1000


# --- event time ----------------------------------------------------------------------------

def test_the_date_header_becomes_epoch_seconds():
    assert message_time(RAW) == 1786871700   # 2026-08-16T09:15:00Z


def test_an_unparseable_or_missing_date_is_zero_not_a_crash():
    assert message_time({}) == 0
    assert message_time({"date": "not a date"}) == 0


# --- the query call --------------------------------------------------------------------------

class _Response:
    def __init__(self, payload):
        self._payload = json.dumps(payload).encode()

    def read(self):
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_the_query_posts_search_terms_and_returns_sorted_candidates(monkeypatch):
    import urllib.request

    seen = {}

    def fake_urlopen(request, timeout=None):
        seen["headers"] = dict(request.headers)
        seen["body"] = json.loads(request.data)
        return _Response([RAW, {**RAW, "id": "older", "date": "2026-08-15T09:00:00Z"}])

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    found = N8nGmailQuery(url="https://n8n.example/webhook/q", token="s3cret").candidates(
        {"label": "aware/invoice-approved"}, 1786900000)

    assert seen["headers"]["X-relay-token"] == "s3cret"
    assert seen["body"]["q"] == "label:aware/invoice-approved"
    assert seen["body"]["received_after"].startswith("2026-08-1")
    assert [ts for ts, _ in found] == sorted(ts for ts, _ in found)   # oldest first
    assert found[-1][1]["upstream_id"] == "18f2a"


def test_an_empty_result_is_a_normal_answer(monkeypatch):
    """Most days nothing has been labelled. That must not look like an error."""
    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", lambda r, timeout=None: _Response([]))

    assert N8nGmailQuery(url="https://x/y", token="t").candidates({"label": "l"}, 0) == []


def test_items_without_an_id_are_dropped(monkeypatch):
    """alwaysOutputData makes the node emit one empty item when nothing matched."""
    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen",
                        lambda r, timeout=None: _Response([{}]))

    assert N8nGmailQuery(url="https://x/y", token="t").candidates({"label": "l"}, 0) == []


def test_an_unreachable_query_raises_rather_than_reporting_nothing_found(monkeypatch):
    """The entire reason this replaced a polling connector: "n8n is down" must not be
    indistinguishable from "you have not labelled it yet"."""
    import urllib.error
    import urllib.request

    def boom(request, timeout=None):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", boom)
    monkeypatch.setattr("reconciler.adapters.gmail.time.sleep", lambda s: None)

    with pytest.raises(RuntimeError, match="unreachable"):
        N8nGmailQuery(url="https://x/y", token="t").candidates({"label": "l"}, 0)


def test_a_rejected_query_raises(monkeypatch):
    import urllib.error
    import urllib.request

    def boom(request, timeout=None):
        raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", {}, None)

    monkeypatch.setattr(urllib.request, "urlopen", boom)

    with pytest.raises(RuntimeError, match="403"):
        N8nGmailQuery(url="https://x/y", token="bad").candidates({"label": "l"}, 0)


def test_an_unreachable_query_is_retried_before_raising(monkeypatch):
    import urllib.error
    import urllib.request
    monkeypatch.setattr("reconciler.adapters.gmail.time.sleep", lambda s: None)

    def boom(request, timeout=None):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", boom)
    with pytest.raises(RuntimeError, match="unreachable"):
        N8nGmailQuery(url="https://x/y", token="t").candidates({"label": "l"}, 0)


# --- retrying transport failures (but never waits) -------------------------------------------

def test_a_transient_failure_is_retried(monkeypatch):
    """Retrying a TRANSPORT error is not the wait-as-retry mistake: nothing was learned, so
    asking again is correct. Retrying an unmet *await* would be the mistake."""
    import urllib.error
    import urllib.request

    calls = {"n": 0}

    def flaky(request, timeout=None):
        calls["n"] += 1
        if calls["n"] < 3:
            raise urllib.error.URLError("timeout exceeded when trying to connect")
        return _Response([RAW])

    monkeypatch.setattr(urllib.request, "urlopen", flaky)
    monkeypatch.setattr("reconciler.adapters.gmail.time.sleep", lambda s: None)

    found = N8nGmailQuery(url="https://x/y", token="t").candidates({"label": "l"}, 0)

    assert calls["n"] == 3
    assert len(found) == 1


def test_retries_are_bounded_and_then_it_raises(monkeypatch):
    import urllib.error
    import urllib.request

    calls = {"n": 0}

    def always_fail(request, timeout=None):
        calls["n"] += 1
        raise urllib.error.URLError("nope")

    monkeypatch.setattr(urllib.request, "urlopen", always_fail)
    monkeypatch.setattr("reconciler.adapters.gmail.time.sleep", lambda s: None)

    with pytest.raises(RuntimeError, match="unreachable"):
        N8nGmailQuery(url="https://x/y", token="t", attempts=3).candidates({"label": "l"}, 0)
    assert calls["n"] == 3


def test_an_auth_failure_is_not_retried(monkeypatch):
    """A bad token will never come good; retrying only delays the real message."""
    import urllib.error
    import urllib.request

    calls = {"n": 0}

    def forbidden(request, timeout=None):
        calls["n"] += 1
        raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", {}, None)

    monkeypatch.setattr(urllib.request, "urlopen", forbidden)

    with pytest.raises(RuntimeError, match="403"):
        N8nGmailQuery(url="https://x/y", token="bad").candidates({"label": "l"}, 0)
    assert calls["n"] == 1
