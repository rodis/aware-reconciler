"""Fetching the reconciler's own configuration from Doppler (ADR 0012).

**Why the runner holds one credential instead of eight.** Every other secret in this project
already lives in Doppler and reaches the cluster through the Doppler operator; the reconciler
runs on Prefect's infrastructure instead, which the operator cannot reach. Copying eight values
into Prefect would make Prefect a second source of truth for them — and the CA incident of
2026-09-03 is what that costs: one credential existed in two places, one copy went stale, and
the entire ingest pipeline dropped events for 37 hours while looking healthy.

So Prefect holds a single read-only Doppler service token, and the eight values are read at run
time from the one place they are authored.

**Scope is the whole design, not a detail.** The token is scoped to a config that is the ROOT of
its own `reconciler` environment, so it inherits nothing: it resolves this process's eight values
and cannot see the rest of the project — notably `avnadmin`'s Kafka private key (full ACL on the
event bus), the BMW refresh token and the dashboard password. A token scoped to `prd` would read
all of those, which is why a branch config was rejected: branch configs inherit their root.

Deliberately NOT runner-specific. Only *obtaining* the token is Prefect's business (`flow.py`);
fetching with it is plain HTTP, so moving to GitHub Actions or a cron box would reuse this
unchanged. Plain `urllib`, so the tier still adds no dependency.

    GET https://api.doppler.com/v3/configs/config/secrets/download?format=json
    Authorization: Basic base64("<service-token>:")     -> {"NAME": "value", ...}

Project and config are implied by the token, so there is nothing here to keep in sync with
Doppler — a token for a different config needs no code change.
"""

import base64
import json
import logging
import re
import time
import urllib.error
import urllib.request

logger = logging.getLogger("reconciler.adapters.doppler")

DEFAULT_ENDPOINT = "https://api.doppler.com/v3/configs/config/secrets/download?format=json"

# A read, so a repeat is free — the same test the Gmail query and the classifier apply, and it
# lands the same way. 120 secret reads/minute on the lowest plan against one read per hourly
# run leaves the budget irrelevant.
DEFAULT_ATTEMPTS = 3

# A Doppler reference that failed to resolve comes back as the LITERAL `${config.NAME}` rather
# than as an error — documented behaviour when the target is renamed or deleted. Left alone it
# would surface far downstream as psycopg trying to dial a host called `${prd.NEON_DATABASE_URL}`,
# which reads like a code bug rather than a secret-store mistake. Caught here instead.
_UNRESOLVED = re.compile(r"^\$\{[^}]*\}$")


class DopplerError(RuntimeError):
    """Doppler could not be read, or answered with something unusable."""


def fetch(token: str, *, endpoint: str = DEFAULT_ENDPOINT, timeout: float = 30.0,
          attempts: int = DEFAULT_ATTEMPTS) -> dict[str, str]:
    """Every secret in the token's config, as a flat name -> value mapping.

    Raises rather than returning a partial mapping. A half-populated environment fails later,
    at whichever stage first needs the value it is missing, which is a much worse place to
    discover a configuration problem than at startup.
    """
    request = urllib.request.Request(
        endpoint,
        headers={
            # Doppler takes the token as the basic-auth *username* with an empty password.
            "Authorization": "Basic " + base64.b64encode(f"{token}:".encode()).decode(),
            "Accept": "application/json",
        },
        method="GET",
    )
    payload = _ask(request, timeout=timeout, attempts=attempts)
    if not isinstance(payload, dict) or not payload:
        raise DopplerError(f"doppler returned no secrets: {str(payload)[:200]}")

    unresolved = sorted(k for k, v in payload.items() if _UNRESOLVED.match(str(v)))
    if unresolved:
        raise DopplerError(
            "doppler reference(s) did not resolve — the target secret was probably renamed "
            f"or deleted: {', '.join(unresolved)}")

    values = {k: str(v) for k, v in payload.items()}
    logger.info("read %d secret(s) from doppler config %s/%s", len(values),
                values.get("DOPPLER_PROJECT", "?"), values.get("DOPPLER_CONFIG", "?"))
    return values


def _ask(request, *, timeout: float, attempts: int):
    last = None
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                # A revoked, expired or wrong-config token will never come good. Retrying only
                # delays a message that names the actual problem.
                raise DopplerError(f"doppler rejected the token: HTTP {e.code} {e.reason}") from e
            last = DopplerError(f"doppler read failed: HTTP {e.code} {e.reason}")
        except urllib.error.URLError as e:
            last = DopplerError(f"doppler unreachable: {e}")
        if attempt < attempts:
            logger.warning("doppler attempt %d/%d failed (%s); retrying", attempt, attempts, last)
            time.sleep(2 ** (attempt - 1))
    raise last
