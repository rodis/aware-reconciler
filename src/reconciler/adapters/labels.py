"""Removing a Gmail label, through n8n.

The outbound counterpart to `adapters.gmail`. That one asks a question; this one changes
something — and the difference decides every design choice below.

**Why n8n at all:** the Gmail OAuth credential lives in n8n's store and stays there. That is the
user's standing rule (credentials in n8n, never in the repo, not even gitignored), and it is
what keeps this tier holding exactly one secret of its own.

**Why this is still ADR 0008-legal:** the relay authenticates and transmits. It does not decide
that a task is done — a person did, by ticking a box or by unlabelling the mail themselves. The
relay is a wire with a credential on it, the same as `mail-relay` and `gmail-query`.

**No retries.** Unlike a Gmail *search*, this is a write, and the failure modes are asymmetric
in the way that matters: a label already removed is harmless to remove again (Gmail's
modify API is idempotent on labels), but a caller that retries through a timeout cannot tell
"it did not happen" from "it happened and the reply was lost" — and the second case, retried,
races the event we emit on success. So a failure is raised and the task simply stays open, which
is the truthful outcome: the label is still on the mail, so the list still showing it is right.
"""

import json
import logging
import urllib.error
import urllib.request

logger = logging.getLogger("reconciler.adapters.labels")


class N8nLabelRelay:
    """Removes one label from one message."""

    def __init__(self, *, url: str, token: str, header: str = "X-Relay-Token",
                 timeout: float = 30.0):
        self._url = url
        self._token = token
        self._header = header
        self._timeout = timeout

    def remove(self, *, message_id: str, label: str) -> None:
        body = {"message_id": message_id, "label": label, "action": "remove"}
        request = urllib.request.Request(
            self._url,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", self._header: self._token},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                status = response.status
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"label relay rejected: HTTP {e.code} {e.reason}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"label relay unreachable: {e}") from e
        logger.info("removed %s from %s (HTTP %s)", label, message_id, status)
