"""Calling CraftMyPDF's REST API (ADR 0012).

Direct, not through n8n. The CraftMyPDF n8n node is a **community plugin**, and the cluster
has no persistence, so it is lost whenever an n8n pod restarts. The REST API needs only an API
key — no OAuth, no refresh — so unlike Gmail and SMTP there is nothing here that n8n's
credential store is needed for. It also keeps rendering off n8n's egress path, which is the
currently-unreliable one (backlog #70).

Contract, confirmed against the live API:

    POST https://api.craftmypdf.com/v1/create
    X-API-KEY: <key>
    {"template_id": "...", "export_type": "json", "expiration": <minutes>, "data": {...}}

    -> {"status": "success", "file": "<presigned S3 url>", "transaction_ref": "...",
        "total_pages": 1, "file_size": 69353, "template_id": "..."}

**`file` is ephemeral and the default is brutal: 300 seconds.** `expiration` is in MINUTES and
maps straight onto the S3 presign, so 10080 buys the 7-day maximum (verified: 60 -> 3600s,
1440 -> 86400s, 10080 -> 604800s). Even then the link dies eventually, so the durable record of
a rendered invoice is the `transaction_ref` plus the payload that produced it — which is why
the render action stores `sent` alongside. A PDF needed after the link expires is re-rendered
from that, byte-identical inputs, rather than recovered from a URL.

Rendering is **not** idempotent — each call consumes a credit and mints a new transaction — so
this retries only where a repeat cannot double-charge. See `_ask`.
"""

import json
import logging
import time
import urllib.error
import urllib.request

logger = logging.getLogger("reconciler.adapters.craftmypdf")

DEFAULT_ENDPOINT = "https://api.craftmypdf.com/v1/create"
DEFAULT_ATTEMPTS = 2
# Minutes. 10080 = 7 days = the S3 presign ceiling; the API default of 5 MINUTES would hand
# the approval mail a link that is dead before it is read.
DEFAULT_EXPIRATION_MINUTES = 10080

# CraftMyPDF sits behind Cloudflare, which BANS urllib's default `Python-urllib/3.x`
# User-Agent outright: the render comes back `HTTP 403 ... error code: 1010`, which is
# Cloudflare's "banned by browser signature", not an auth or quota failure. Identifying
# ourselves properly is the fix. The same call made with curl succeeds, which is exactly what
# makes this confusing to diagnose from a shell probe.
USER_AGENT = "aware-reconciler/1.0 (+https://github.com/rodis/inference)"


class CraftMyPdf:
    """Renders a template to a PDF and returns whatever the API reports about it."""

    def __init__(self, *, api_key: str, endpoint: str = DEFAULT_ENDPOINT,
                 export_type: str = "json", timeout: float = 60.0,
                 attempts: int = DEFAULT_ATTEMPTS,
                 expiration_minutes: int = DEFAULT_EXPIRATION_MINUTES):
        self._api_key = api_key
        self._endpoint = endpoint
        self._export_type = export_type
        self._timeout = timeout
        self._attempts = attempts
        self._expiration = expiration_minutes

    def render(self, *, template_id: str, data: dict) -> dict:
        body = {"template_id": template_id, "export_type": self._export_type,
                "expiration": self._expiration, "data": data}
        request = urllib.request.Request(
            self._endpoint,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "X-API-KEY": self._api_key,
                     "User-Agent": USER_AGENT},
            method="POST",
        )
        payload = self._ask(request)

        if isinstance(payload, dict) and payload.get("status") not in (None, "success"):
            raise RuntimeError(f"craftmypdf refused the render: {payload}")
        return payload if isinstance(payload, dict) else {"response": payload}

    def _ask(self, request):
        """POST, retrying only failures that cannot have rendered anything.

        A connect-level failure means the request never arrived, so repeating it is free. A
        response-level failure (any HTTP status) means the API *did* answer, and a render may
        already have been billed — so those are not retried. This is the same
        can-a-repeat-do-harm test the mail relay applies, and it lands in the same place for
        the same reason.
        """
        last = None
        for attempt in range(1, self._attempts + 1):
            try:
                with urllib.request.urlopen(request, timeout=self._timeout) as response:
                    return json.loads(response.read() or b"{}")
            except urllib.error.HTTPError as e:
                detail = (e.read() or b"")[:300].decode(errors="replace")
                raise RuntimeError(
                    f"craftmypdf rejected the render: HTTP {e.code} {e.reason} {detail}"
                ) from e
            except urllib.error.URLError as e:
                last = RuntimeError(f"craftmypdf unreachable: {e}")
            if attempt < self._attempts:
                logger.warning("craftmypdf attempt %d/%d failed (%s); retrying",
                               attempt, self._attempts, last)
                time.sleep(2 ** (attempt - 1))
        raise last
