"""Posting one raw event at the ingest gateway.

`adapters.gateway` does this for *process milestones* — it knows about definitions, cycles and
`EVIDENCE_TIME_KEY`, and its wire body is shaped by all three. The task sweep has none of those:
it emits a plain raw signal, the same way an iOS Shortcut does. So rather than generalise the
module the invoice depends on, this is the generic half on its own — one POST, no opinion about
what the payload means.

The contract is `shape_sensor.yml`'s and nothing more:

    POST /sensors/<app>   {"payload": {"event_name": ..., "user_id": ..., ...}}

Two things about that contract are easy to get wrong and expensive to discover later:

- **`event_name` is renamed to `name`, and `user_id` is required.** An event without one is
  dropped with a Vector *error log*, not a 4xx — the HTTP source has already answered 200. So a
  204/200 here does not prove the event landed, and the only real confirmation is the row
  appearing in Neon a few seconds later.
- **`app` is free.** `route_by_app.yml` routes everything that is not `overland` to the standard
  adapter, so a new producer needs no Vector change, no transform and no deploy — which is why
  this whole feature adds no infrastructure.
"""

import json
import logging
import urllib.error
import urllib.request

logger = logging.getLogger("reconciler.adapters.ingest")


class GatewayEvents:
    """Emits raw events to `/sensors/<app>`.

    **Does not retry, deliberately.** Ingest is at-most-once and acks on receipt, so a repeat
    cannot be de-duplicated downstream: `message.id` is a fresh uuid per POST and the Postgres
    sink has no `ON CONFLICT`. A duplicated close would put two `email_task_closed` rows on the
    timeline for one action. The sweep is the recovery path instead — it re-derives the same
    diff on its next run, so a dropped event costs an hour, while a doubled one is permanent.
    """

    def __init__(self, base_url: str, app: str, timeout: float = 10.0):
        self._url = f"{base_url.rstrip('/')}/sensors/{app}"
        self._timeout = timeout

    def emit(self, payload: dict) -> None:
        request = urllib.request.Request(
            self._url,
            data=json.dumps({"payload": payload}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                status = response.status
        except urllib.error.URLError as e:
            raise RuntimeError(f"could not emit {payload.get('event_name')}: {e}") from e
        logger.info("emitted %s (HTTP %s)", payload.get("event_name"), status)


class DryRunEvents:
    """Emits nothing; keeps what it was asked to emit.

    The sweep's `--dry-run`, and the reason it exists is that the first run of a sweep over a
    real mailbox is exactly when you want to see the diff before it becomes history — a wrong
    label or an off-by-one lookback would otherwise close every open task at once.
    """

    def __init__(self):
        self.emitted: list[dict] = []

    def emit(self, payload: dict) -> None:
        self.emitted.append(payload)
        logger.info("[dry-run] would emit %s (%s)",
                    payload.get("event_name"), payload.get("subject", "")[:60])
