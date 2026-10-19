"""Asking Gemini a question through n8n (ADR 0012).

The transport half of `reconciler.classify`; the prompt, the verdict shape and the parsing live
there and are tested with no network and no key.

**Through n8n, unlike CraftMyPDF, because the credential already lives there.** The rule this
tier follows is the user's: a credential we would otherwise have to put in the repo belongs in
n8n's store instead. The Gemini API key is one — it predates this work, shared with the retired
invoice workflows — so routing through the relay means the reconciler holds exactly one secret
(the relay token we mint ourselves) rather than two.

**What n8n is NOT doing is the important half.** `connectors/n8n/llm-relay.workflow.ts` maps two
fields onto Gemini's request shape and returns the reply verbatim. The question is composed
here, from a `classify:` line in `processes/*.yml`; the verdict is parsed here. That split is
what keeps the relay inside ADR 0008's boundary — a relay may authenticate and transmit, it may
not compose, decide or interpret — and it is exactly the line the retired
`Workflow: Check Payment Was Submitted` crossed, with `invoice 03-2026` hardcoded in its prompt.

Response shape, confirmed live 2026-08-16 (`jsonOutput: true` does NOT pre-parse; it returns
Gemini's candidate with the JSON still a string):

    {"content": {"parts": [{"text": "{\\n \\"matches\\": true, ...}"}], "role": "model"},
     "finishReason": "STOP", "index": 0}
"""

import json
import logging
import time
import urllib.error
import urllib.request

from reconciler.classify import SYSTEM_PROMPT, Verdict, build_prompt, parse_verdict

logger = logging.getLogger("reconciler.adapters.llm")

# Flash is the right tier for one binary reading of a short email. The relay defaults to the
# same value; sending it explicitly keeps the choice in the repo, where it is reviewable.
DEFAULT_MODEL = "models/gemini-2.5-flash"

# Deliberately small, and it multiplies: the relay node retries twice on its own, so this is a
# ceiling of six Gemini calls per question. That ceiling is the point — the key is on a quota'd
# tier, and a generous retry budget spent against a *quota* error does not eventually succeed,
# it just exhausts the day's allowance faster. (Learned on 2026-08-16 by doing exactly that
# while testing.) A healthy call answers on the first attempt in ~1.2-1.8s.
#
# Sized after backlog #70 was diagnosed and fixed the same day: two of n8n's three workers were
# sitting `ready=False` logging `Database connection timed out`, so a job routed to one died in
# Bull *before any node ran* — which is why retrying inside the workflow could not help it, and
# only a fresh request could. That is transport flakiness, and three attempts covers it; it is
# not a licence to paper over a broken fleet.
DEFAULT_ATTEMPTS = 3


class N8nGeminiRelay:
    """Judges one email against one question, and returns a structured verdict.

    Retries anything that failed, including HTTP statuses — which is the opposite of the mail
    relay and of CraftMyPDF, and the difference is not inconsistency but the same test applied
    to a different act. Those two *do* something on the far side: a repeat sends a second
    invoice, or bills a second render. Asking a question is a read. Nothing on the far side
    changes, so a repeat is free and the only cost of not retrying is a stage that stalls
    because a proxy hiccupped.

    Exhausted retries raise. An unreachable classifier must never read as "no match" — that is
    the same silent-stall failure the polling connector was retired for.
    """

    def __init__(self, *, url: str, token: str, header: str = "X-Relay-Token",
                 model: str = DEFAULT_MODEL, timeout: float = 90.0,
                 attempts: int = DEFAULT_ATTEMPTS):
        self._url = url
        self._token = token
        self._header = header
        self._model = model
        self._timeout = timeout
        self._attempts = attempts

    def judge(self, *, question: str, message: dict) -> Verdict:
        body = {
            "system": SYSTEM_PROMPT,
            "prompt": build_prompt(question, message),
            "model": self._model,
        }
        request = urllib.request.Request(
            self._url,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", self._header: self._token},
            method="POST",
        )
        verdict = parse_verdict(_reply_text(self._ask(request)))
        logger.info("classified %r -> %s (%s)",
                    (message.get("subject") or "")[:80], verdict.matches, verdict.reason)
        return verdict

    def _ask(self, request):
        last = None
        for attempt in range(1, self._attempts + 1):
            try:
                with urllib.request.urlopen(request, timeout=self._timeout) as response:
                    return json.loads(response.read() or b"{}")
            except urllib.error.HTTPError as e:
                if e.code in (401, 403):
                    # A bad token will never come good; retrying only delays the real message.
                    raise RuntimeError(f"llm relay rejected: HTTP {e.code} {e.reason}") from e
                last = RuntimeError(f"llm relay failed: HTTP {e.code} {e.reason}")
            except urllib.error.URLError as e:
                last = RuntimeError(f"llm relay unreachable: {e}")
            if attempt < self._attempts:
                logger.warning("llm relay attempt %d/%d failed (%s); retrying",
                               attempt, self._attempts, last)
                time.sleep(2 ** (attempt - 1))
        raise last


def _reply_text(payload: dict) -> str:
    """Gemini's candidate down to the string the verdict is in.

    `finishReason` is checked first and on purpose: `MAX_TOKENS` truncates the reply
    mid-object, which surfaces as a JSON decode error and reads like the model misbehaving
    rather than the ceiling being too low. (The node's own default is 16 tokens — the relay
    raises it, and this is the guard for the day someone lowers it again.)
    """
    reason = payload.get("finishReason")
    if reason not in (None, "STOP"):
        raise RuntimeError(f"llm relay returned an incomplete answer (finishReason={reason})")

    parts = ((payload.get("content") or {}).get("parts")) or []
    text = "".join(p.get("text", "") for p in parts if isinstance(p, dict))
    if not text.strip():
        raise RuntimeError(f"llm relay returned no text: {str(payload)[:200]}")
    return text
