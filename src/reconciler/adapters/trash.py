"""Moving a Gmail message to the Bin, through n8n (`Relay: gmail trash`, QqDX8ib4qXWFgysC).

The second outbound relay, next to `adapters.labels`, and the same reasoning applies: the Gmail
credential stays in n8n, and the relay authenticates and transmits while the decision of WHAT to
trash is made by `reconciler.purge`.

**Trash, never delete.** The relay calls `messages/{id}/trash`: 30 days in Gmail's Bin, and only
the `gmail.modify` scope. Nothing here can delete permanently.

**No retries here.** Unlike `labels`, a repeat would be harmless — trashing a trashed message
succeeds and changes nothing — but there is no need: the purge is a pure function of what is in
the mailbox, so the next pass retries anything this one failed on.
"""

import json
import logging
import urllib.error
import urllib.request

logger = logging.getLogger("reconciler.adapters.trash")


class N8nTrashRelay:
    """Trashes one message by id."""

    def __init__(self, *, url: str, token: str, header: str = "X-Relay-Token",
                 timeout: float = 30.0):
        self._url = url
        self._token = token
        self._header = header
        self._timeout = timeout

    def trash(self, *, message_id: str) -> None:
        request = urllib.request.Request(
            self._url,
            data=json.dumps({"message_id": message_id}).encode(),
            headers={"Content-Type": "application/json", self._header: self._token},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                status = response.status
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"trash relay rejected {message_id}: HTTP {e.code} {e.reason}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"trash relay unreachable: {e}") from e
        logger.info("trashed %s (HTTP %s)", message_id, status)
