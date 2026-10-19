"""Asking n8n a Gmail question, synchronously (ADR 0012).

The reconciler's inbound edge. It builds a Gmail search, posts it to the query workflow, and
normalises the raw response — so n8n stays a credential-holding answering machine with no
loop, no cursor and no opinion.

**All mapping happens here, not in n8n**, and that is deliberate: extracting an address from
mailparser's `from` object is exactly the sort of thing that shipped `[objectobject]` to 15
rows once already, and here it is unit-tested.
"""

import json
import logging
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime

logger = logging.getLogger("reconciler.adapters.gmail")

DEFAULT_LIMIT = 25
DEFAULT_ATTEMPTS = 3


def build_query(signal: dict) -> str:
    """Gmail search syntax from a signal's declarative fields.

    Only `label` and `from` today — the two the invoice needs. `q` passes a raw query through
    for anything else, but note that a signal reaching for raw `q` is a hint that the grammar
    is missing a field, not that it needs an escape hatch.
    """
    parts = []
    if label := signal.get("label"):
        parts.append(f"label:{label}")
    if sender := signal.get("from"):
        parts.append(f"from:{sender}")
    if raw := signal.get("q"):
        parts.append(raw)
    return " ".join(parts)


def normalise(message: dict) -> dict:
    """One raw Gmail item into the flat shape signals are matched against.

    Field names match what the retired connector emitted, so `correlate_on: subject` and
    `where: {from: ...}` mean the same thing they did before the inbound edge changed.
    """
    sender = (((message.get("from") or {}).get("value") or [{}]) or [{}])[0] or {}
    address = sender.get("address") or ""
    return {
        "upstream_id": message.get("id"),
        "gmail_thread_id": message.get("threadId"),
        "subject": message.get("subject") or "",
        "from": address,
        "from_name": sender.get("name") or "",
        "from_domain": address.split("@")[-1].lower() if "@" in address else "",
        "labels": message.get("labelIds") or [],
        # A bounded prefix, never the whole body: it is evidence of what was approved, not an
        # archive. The same ~1 MiB ingress reasoning as the connector's snippet cap.
        "snippet": " ".join(str(message.get("text") or message.get("snippet") or "").split())[:1000],
    }


def message_time(message: dict) -> int:
    """Event time from the message's own `Date` header, in epoch seconds.

    The sender sets that header, so a skewed clock skews this — acceptable here because the
    mail whose time matters most is one we sent ourselves, and `correlate_on` does the real
    identification work regardless.
    """
    raw = message.get("date")
    if not raw:
        return 0
    try:
        return int(datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp())
    except ValueError:
        logger.warning("unparseable Date %r; treating as epoch 0", raw)
        return 0


class N8nGmailQuery:
    """Candidates come from a live Gmail search, asked at decision time.

    Failure raises once retries are exhausted. That is the whole point of moving off a polling
    connector: an unreachable n8n must not look like "nothing has been labelled yet", because
    the two are indistinguishable to everything downstream and one of them stalls the process
    forever.

    Retries, unlike an `await`. This is the distinction the whole tier turns on: a stage that
    has not happened yet must NOT be retried — that is the wait-as-retry mistake the prior art
    made. A transport error is different in kind: nothing was learned, so asking again is
    correct rather than a stall in disguise. Observed 2026-08-16, this instance's egress to
    Google times out at ~20s on roughly two calls in three and answers in ~1.1s otherwise.

    The mail relay deliberately does NOT retry — see `adapters.mail`.
    """

    def __init__(self, *, url: str, token: str, header: str = "X-Relay-Token",
                 timeout: float = 30.0, limit: int = DEFAULT_LIMIT,
                 attempts: int = DEFAULT_ATTEMPTS):
        self._url = url
        self._token = token
        self._header = header
        self._timeout = timeout
        self._limit = limit
        self._attempts = attempts

    def candidates(self, signal: dict, since: int) -> list[tuple[int, dict]]:
        body = {
            "q": build_query(signal),
            "received_after": datetime.fromtimestamp(since, UTC).isoformat(),
            "limit": signal.get("limit", self._limit),
        }
        request = urllib.request.Request(
            self._url,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", self._header: self._token},
            method="POST",
        )
        payload = self._ask(request)

        messages = payload if isinstance(payload, list) else payload.get("data", [])
        # An empty search is a normal answer — most days nothing has been labelled — so the
        # workflow sets alwaysOutputData and can return a single empty item.
        found = [(message_time(m), normalise(m)) for m in messages if m.get("id")]
        found.sort(key=lambda pair: pair[0])
        logger.info("gmail query %r since %s -> %d candidate(s)",
                    body["q"], body["received_after"], len(found))
        return found

    def _ask(self, request):
        """POST, retrying transport failures. A search is read-only, so a repeat is free."""
        last = None
        for attempt in range(1, self._attempts + 1):
            try:
                with urllib.request.urlopen(request, timeout=self._timeout) as response:
                    return json.loads(response.read() or b"[]")
            except urllib.error.HTTPError as e:
                if e.code in (401, 403):
                    # A bad token will never come good; retrying only delays the real message.
                    raise RuntimeError(f"gmail query rejected: HTTP {e.code} {e.reason}") from e
                last = RuntimeError(f"gmail query failed: HTTP {e.code} {e.reason}")
            except urllib.error.URLError as e:
                last = RuntimeError(f"gmail query unreachable: {e}")
            if attempt < self._attempts:
                logger.warning("gmail query attempt %d/%d failed (%s); retrying",
                               attempt, self._attempts, last)
                time.sleep(2 ** (attempt - 1))
        raise last
