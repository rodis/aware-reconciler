"""Reading the reconciler's config from Doppler (ADR 0012).

No token and no network: the HTTP call is faked, so what is actually pinned here is the
contract — how the token is presented, and the three ways a read can be wrong in a way that
would otherwise surface far downstream.
"""

import base64
import json
import urllib.error
import urllib.request

import pytest

from reconciler.adapters import doppler

LIVE = {
    "NEON_DATABASE_URL": "postgresql://u:p@host/db",
    "MAIL_RELAY_TOKEN": "t" * 64,
    "DOPPLER_PROJECT": "kafka-aiven-credentials",
    "DOPPLER_CONFIG": "reconciler",
}


class _Body:
    def __init__(self, payload):
        self._raw = json.dumps(payload).encode()

    def read(self):
        return self._raw

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_the_token_is_the_basic_auth_username_with_an_empty_password(monkeypatch):
    """Doppler's scheme, and easy to get wrong as a Bearer token — which fails as a 401 that
    looks like a revoked token rather than a malformed request."""
    seen = {}

    def capture(request, timeout=None):
        seen["auth"] = request.headers["Authorization"]
        seen["url"] = request.full_url
        return _Body(LIVE)

    monkeypatch.setattr(urllib.request, "urlopen", capture)
    doppler.fetch("dp.st.abc")

    scheme, _, blob = seen["auth"].partition(" ")
    assert scheme == "Basic"
    assert base64.b64decode(blob).decode() == "dp.st.abc:"      # trailing colon = no password
    assert "format=json" in seen["url"]


def test_secrets_come_back_as_a_flat_mapping(monkeypatch):
    monkeypatch.setattr(urllib.request, "urlopen", lambda r, timeout=None: _Body(LIVE))
    assert doppler.fetch("t")["MAIL_RELAY_TOKEN"] == "t" * 64


def test_an_unresolved_reference_is_named_rather_than_passed_through(monkeypatch):
    """Doppler returns a broken reference as the LITERAL `${config.NAME}`, not an error. Passed
    through, it reaches psycopg as a hostname — a bug that reads like ours and isn't. The
    failure has to name the secret store instead."""
    broken = {**LIVE, "NEON_DATABASE_URL": "${prd.NEON_DATABASE_URL}"}
    monkeypatch.setattr(urllib.request, "urlopen", lambda r, timeout=None: _Body(broken))

    with pytest.raises(doppler.DopplerError, match="NEON_DATABASE_URL"):
        doppler.fetch("t")


def test_a_value_that_merely_contains_braces_is_not_mistaken_for_a_reference(monkeypatch):
    """A password may legitimately contain `${`. Only a value that is EXACTLY a reference
    counts — anchored, so a real secret is never rejected as broken config."""
    fine = {**LIVE, "MAIL_RELAY_TOKEN": "pa${ss}word-and-more"}
    monkeypatch.setattr(urllib.request, "urlopen", lambda r, timeout=None: _Body(fine))
    assert doppler.fetch("t")["MAIL_RELAY_TOKEN"] == "pa${ss}word-and-more"


def test_an_empty_config_raises_rather_than_returning_nothing(monkeypatch):
    """Returning `{}` would leave the environment untouched and the failure would land at
    whichever stage first needed a value — the worst place to learn the token is wrong."""
    monkeypatch.setattr(urllib.request, "urlopen", lambda r, timeout=None: _Body({}))
    with pytest.raises(doppler.DopplerError, match="no secrets"):
        doppler.fetch("t")


def test_a_rejected_token_is_not_retried(monkeypatch):
    """A revoked or wrong-config token will never come good; retrying only delays the message
    that names it. Same rule as the Gmail query and the LLM relay."""
    monkeypatch.setattr("time.sleep", lambda *_: None)
    calls = {"n": 0}

    def forbidden(request, timeout=None):
        calls["n"] += 1
        raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", {}, None)

    monkeypatch.setattr(urllib.request, "urlopen", forbidden)
    with pytest.raises(doppler.DopplerError, match="rejected the token"):
        doppler.fetch("t")
    assert calls["n"] == 1


def test_a_transient_failure_is_retried(monkeypatch):
    """Reading is idempotent, so a repeat is free — unlike the mail relay, where a repeat
    sends a second invoice."""
    monkeypatch.setattr("time.sleep", lambda *_: None)
    calls = {"n": 0}

    def flaky(request, timeout=None):
        calls["n"] += 1
        if calls["n"] < 3:
            raise urllib.error.HTTPError(request.full_url, 500, "Internal", {}, None)
        return _Body(LIVE)

    monkeypatch.setattr(urllib.request, "urlopen", flaky)
    assert doppler.fetch("t")["DOPPLER_CONFIG"] == "reconciler"
    assert calls["n"] == 3
