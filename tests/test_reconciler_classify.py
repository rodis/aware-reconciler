"""Telling a payment SENT from the same payment COMPLETED (ADR 0012).

The two mails below are the real ones, verbatim, from 2026-07-15 — same sender, same invoice,
2h55m apart. Nothing structural separates them, which is the entire reason this stage reads
rather than compares.

What is pinned here is the **split**: identity is decided deterministically and semantics are
decided by the classifier, in that order. A classifier that could also decide identity would be
one hallucination away from closing another invoice's stage.
"""

import os

import pytest

from reconciler.classify import Verdict, build_prompt, parse_verdict
from reconciler.core import Cycle, Milestone
from reconciler.finder import SignalFinder

# --- the real mail, exactly as it arrived ------------------------------------------------------

SUBMITTED = {
    "subject": "DreamHost submitted a payment to you",
    "from": "accounting@example.com",
    "snippet": "Dear Jordan Rivera, A payment to you was submitted by Wire Transfer. "
               "Payment message: Invoice 7-2026 Best regards, The DreamHost team.",
}

PROCESSED = {
    "subject": "[Tipalti payment processed successfully]",
    "from": "accounting@example.com",
    "snippet": "Dear Rivera Gmbh (Jordan Rivera), A USD 12,345.00 payment was sent to you "
               "today by Wire Transfer and covers the following: Amount Type Document number "
               "Document date USD 12,345.00 Invoice 7-2026 7/13/2026 Best regards, "
               "The DreamHost Team",
}

SUBMITTED_AT, PROCESSED_AT = 1784301420, 1784311920      # 15:17 and 18:12

SIGNAL = {"source": "classify", "from": "accounting@example.com",
          "mentions": "invoice_number", "classify": "Was the payment sent?"}


def _cycle():
    return Cycle(key="dh_invoice_2026_007", process="dreamhost_invoice", user_id="rods",
                 opened_at=1783900000, context={"invoice_number": 7})


def _milestones(number="07-2026"):
    return {"invoice_generated": Milestone("invoice_generated", 1784200000,
                                           {"invoice_number": number})}


class FakeSource:
    def __init__(self, *mails):
        self._mails = list(mails)

    def candidates(self, signal, since):
        return self._mails


class Judge:
    """Answers from a fixed verdict per subject, and records what it was shown."""

    def __init__(self, matches):
        self._matches = matches
        self.asked = []

    def judge(self, *, question, message):
        self.asked.append(message["subject"])
        return Verdict(matches=self._matches(message), reason="fixture")


# --- the prompt ---------------------------------------------------------------------------------

def test_the_prompt_carries_the_question_and_the_whole_mail():
    """Subject included on purpose: it is the more precise of the two here — 'processed
    successfully' appears there and nowhere in the body."""
    prompt = build_prompt("Was the payment sent?", PROCESSED)

    assert "Was the payment sent?" in prompt
    assert "[Tipalti payment processed successfully]" in prompt
    assert "accounting@example.com" in prompt
    assert "USD 12,345.00 payment was sent to you today" in prompt


def test_a_long_body_is_bounded():
    """A forwarded thread must not push the actual message out of the window."""
    prompt = build_prompt("q", {**SUBMITTED, "snippet": "x" * 50_000})
    assert len(prompt) < 6000


def test_a_verdict_is_read_structurally():
    verdict = parse_verdict('{"matches": true, "reason": "says it was submitted"}')
    assert verdict.matches is True
    assert verdict.reason == "says it was submitted"


def test_a_reply_without_a_verdict_raises_rather_than_reading_as_no():
    """A drifted response shape must not become a silent False — a stage that never fires
    looks exactly like a payment that never arrived."""
    with pytest.raises(ValueError, match="no verdict"):
        parse_verdict('{"answer": "yes"}')


# --- identity is decided before, and independently of, the reading -------------------------------

def test_the_classifier_is_never_asked_about_another_invoice_s_mail():
    """The deterministic guard runs FIRST. This is the load-bearing one: the reading decides
    which step, never whose invoice."""
    judge = Judge(lambda m: True)
    other = {**SUBMITTED, "snippet": "A payment to you was submitted. Payment message: Invoice 3-2026"}
    finder = SignalFinder(FakeSource((SUBMITTED_AT, other)), classifier=judge)

    assert finder.find(SIGNAL, _cycle(), 1784200000, _milestones()) is None
    assert judge.asked == []          # not shown to the model at all


def test_our_zero_padding_still_matches_their_bare_number():
    """We render 07-2026; DreamHost writes 7-2026. Neither is a substring of the other, so the
    unpadded form has to be searched for in its own right."""
    judge = Judge(lambda m: True)
    finder = SignalFinder(FakeSource((SUBMITTED_AT, SUBMITTED)), classifier=judge)

    assert finder.find(SIGNAL, _cycle(), 1784200000, _milestones("07-2026")) is not None


def test_nothing_matches_before_there_is_an_invoice_number_to_mention():
    judge = Judge(lambda m: True)
    finder = SignalFinder(FakeSource((SUBMITTED_AT, SUBMITTED)), classifier=judge)

    assert finder.find(SIGNAL, _cycle(), 1784200000, {}) is None
    assert judge.asked == []


# --- the reading ---------------------------------------------------------------------------------

def test_a_no_verdict_skips_the_candidate_rather_than_ending_the_search():
    """Both mails are plausible; the earlier one is rejected and the later one still found.
    A `continue` rather than a `return None` is what makes that possible."""
    judge = Judge(lambda m: "processed" in m["subject"])
    finder = SignalFinder(FakeSource((SUBMITTED_AT, SUBMITTED), (PROCESSED_AT, PROCESSED)),
                          classifier=judge)

    found = finder.find(SIGNAL, _cycle(), 1784200000, _milestones())

    assert found["matched_at"] == PROCESSED_AT
    assert found["evidence"]["subject"] == PROCESSED["subject"]
    assert judge.asked == [SUBMITTED["subject"], PROCESSED["subject"]]


def test_the_reading_is_recorded_beside_the_evidence():
    """An await that fired on a judgement should be able to say what it read."""
    finder = SignalFinder(FakeSource((SUBMITTED_AT, SUBMITTED)), classifier=Judge(lambda m: True))
    assert finder.find(SIGNAL, _cycle(), 1784200000, _milestones())["reading"] == "fixture"


def test_a_signal_with_no_question_never_reaches_the_classifier():
    """The Gmail label stages share this finder and must stay deterministic."""
    judge = Judge(lambda m: False)
    finder = SignalFinder(FakeSource((SUBMITTED_AT, SUBMITTED)), classifier=judge)
    signal = {k: v for k, v in SIGNAL.items() if k != "classify"}

    assert finder.find(signal, _cycle(), 1784200000, _milestones()) is not None
    assert judge.asked == []


def test_an_unwired_classifier_is_loud():
    """Silently skipping the question would be indistinguishable from still waiting — the one
    failure this tier must never fake."""
    finder = SignalFinder(FakeSource((SUBMITTED_AT, SUBMITTED)))
    with pytest.raises(RuntimeError, match="no classifier"):
        finder.find(SIGNAL, _cycle(), 1784200000, _milestones())


# --- the relay adapter ---------------------------------------------------------------------------

# The live shape, confirmed 2026-08-16: `jsonOutput: true` does NOT pre-parse — the verdict
# comes back as a string inside Gemini's candidate.
LIVE_SHAPE = {"content": {"parts": [{"text": '{"matches": true, "reason": "says submitted"}'}],
                          "role": "model"},
              "finishReason": "STOP", "index": 0}


def test_the_verdict_is_dug_out_of_geminis_candidate():
    from reconciler.adapters.llm import _reply_text
    assert parse_verdict(_reply_text(LIVE_SHAPE)).matches is True


def test_a_truncated_answer_is_named_rather_than_read_as_broken_json():
    """`maxOutputTokens` defaults to 16 on the n8n node. The relay raises it; this is the
    guard for the day someone lowers it, so the symptom names the ceiling instead of looking
    like the model emitting garbage."""
    from reconciler.adapters.llm import _reply_text
    truncated = {**LIVE_SHAPE, "finishReason": "MAX_TOKENS"}
    with pytest.raises(RuntimeError, match="incomplete"):
        _reply_text(truncated)


def test_an_empty_reply_raises_rather_than_becoming_a_no():
    from reconciler.adapters.llm import _reply_text
    with pytest.raises(RuntimeError, match="no text"):
        _reply_text({"content": {"parts": []}, "finishReason": "STOP"})


def test_the_relay_retries_a_failing_hop_but_not_a_bad_token(monkeypatch):
    """Asking a question is a READ — nothing on the far side changes, so a repeat is free and
    the only cost of not retrying is a stall. (The mail relay and CraftMyPDF deliberately do
    the opposite: a repeat there sends a second invoice or bills a second render.) Measured
    2026-08-16, this n8n instance's egress fails about one call in three — backlog #70."""
    import urllib.error
    import urllib.request

    from reconciler.adapters.llm import N8nGeminiRelay

    monkeypatch.setattr("time.sleep", lambda *_: None)
    calls = {"n": 0}

    def flaky(request, timeout=None):
        calls["n"] += 1
        if calls["n"] < 3:
            raise urllib.error.HTTPError(request.full_url, 500, "Internal", {}, None)
        return _Body(LIVE_SHAPE)

    monkeypatch.setattr(urllib.request, "urlopen", flaky)
    relay = N8nGeminiRelay(url="https://n8n.example/hook", token="t")
    assert relay.judge(question="q", message=SUBMITTED).matches is True
    assert calls["n"] == 3

    def forbidden(request, timeout=None):
        calls["n"] += 1
        raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", {}, None)

    calls["n"] = 0
    monkeypatch.setattr(urllib.request, "urlopen", forbidden)
    with pytest.raises(RuntimeError, match="403"):
        relay.judge(question="q", message=SUBMITTED)
    assert calls["n"] == 1          # a bad token will never come good


def test_the_request_carries_the_composed_prompt_and_the_token(monkeypatch):
    """n8n receives a finished question and returns a reply. It never sees an invoice, a
    cycle, or a stage — that boundary is what keeps the relay a wire."""
    import json as _json
    import urllib.request

    from reconciler.adapters.llm import N8nGeminiRelay

    seen = {}

    def capture(request, timeout=None):
        seen["body"] = _json.loads(request.data)
        seen["headers"] = dict(request.headers)
        return _Body(LIVE_SHAPE)

    monkeypatch.setattr(urllib.request, "urlopen", capture)
    N8nGeminiRelay(url="https://n8n.example/hook", token="tok").judge(
        question="Was it initiated?", message=SUBMITTED)

    assert "Was it initiated?" in seen["body"]["prompt"]
    assert SUBMITTED["subject"] in seen["body"]["prompt"]
    assert seen["body"]["system"].startswith("You classify one email")
    assert seen["headers"]["X-relay-token"] == "tok"

    # The boundary, asserted rather than described: the relay is handed a question and a
    # message, and nothing that would let it know which process, cycle or stage is asking —
    # so it cannot grow an opinion about any of them.
    assert set(seen["body"]) == {"system", "prompt", "model"}
    assert "dh_invoice" not in _json.dumps(seen["body"])
    assert "payment_submitted" not in _json.dumps(seen["body"])


class _Body:
    def __init__(self, payload):
        import json as _json
        self._raw = _json.dumps(payload).encode()

    def read(self):
        return self._raw

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


# --- the prompts against the real model (opt-in) -------------------------------------------------

def _questions():
    """The live prompts, read from the definition rather than copied.

    Copies rot: a prompt edited in the YAML and not here would leave this passing while
    production used untested wording — and the wording is the whole artefact under test.
    """
    import pathlib

    from reconciler.definition import load_definitions

    root = pathlib.Path(__file__).resolve().parents[1] / "processes"
    definition = next(d for d in load_definitions(root) if d.name == "dreamhost_invoice")
    return {s.name: s.signal["classify"] for s in definition.stages
            if s.kind == "await" and s.signal.get("classify")}


@pytest.mark.skipif(not os.environ.get("LLM_RELAY_URL"),
                    reason="needs LLM_RELAY_URL + MAIL_RELAY_TOKEN (the n8n Gemini relay)")
@pytest.mark.parametrize("stage,mail,expected", [
    ("payment_submitted", SUBMITTED, True),
    ("payment_submitted", PROCESSED, False),      # the one an earlier draft got wrong
    ("payment_processed", PROCESSED, True),
    ("payment_processed", SUBMITTED, False),
])
def test_the_real_prompts_separate_the_real_mails(stage, mail, expected):
    """The live prompts, on the mails they were written for, through the live relay.

    Skipped in CI, which has no relay. It is the only test here that exercises the thing
    actually being trusted — everything above tests the plumbing around it — so run it after
    any edit to either prompt.

    The `payment_submitted` x PROCESSED row is the one that earns its keep: a draft asking
    whether the payment had been "SENT or ISSUED" answered *true* here, because the completion
    notice says "A USD 12,345.00 payment was sent to you today". Nothing offline would have
    caught that, and in production it is invisible until the next stage never fires.
    """
    from reconciler.adapters.llm import N8nGeminiRelay

    relay = N8nGeminiRelay(url=os.environ["LLM_RELAY_URL"],
                           token=os.environ["MAIL_RELAY_TOKEN"],
                           header=os.environ.get("MAIL_RELAY_HEADER", "X-Relay-Token"))
    assert relay.judge(question=_questions()[stage], message=mail).matches is expected
