"""Reading a mail's meaning, when a label cannot carry it (ADR 0012).

Most `await`s here need no interpretation at all: applying a Gmail label **is** the decision,
and matching one is a string comparison. Two stages are different. Tipalti sends two mails
about the same payment — one when it is submitted, one when it has gone through — and nothing
structural separates them: same sender, same invoice, minutes-to-hours apart. Telling them
apart is a reading, and that is what this is for.

**The split this file exists to keep:** identity is deterministic, semantics are interpreted.
Whether a mail belongs to *this* cycle is decided by `finder.SignalFinder` — a substring test
against the invoice number, which cannot be talked out of its answer. The LLM is asked exactly
one thing: of the mail we already know is ours, which of the two things does it say? A
classifier that also decided identity could hallucinate a cycle boundary, and a wrong boundary
means one invoice's payment closes another invoice's stage.

**Uncertainty resolves to `false`, and the asymmetry is the point.** A missed match costs an
hour — the next reconcile asks again, and the mail has not gone anywhere. A wrong match records
a milestone, which is the one thing this tier treats as fact; the stage after it starts waiting
for a payment confirmation that was never announced. So the prompt says to abstain when unsure,
and abstaining is cheap by construction.
"""

import json
from dataclasses import dataclass
from typing import Protocol

# Long enough to be evidence of what the mail said, short enough that a forwarded thread with a
# quoted history does not push the actual message out of view. The mails these stages read say
# their piece in the first paragraph.
MAX_BODY_CHARS = 4000

SYSTEM_PROMPT = """\
You classify one email against one yes/no question. You are part of an automated finance \
process; your answer decides whether a step is recorded as having happened.

Rules:
- Answer ONLY the question asked. Do not judge whether the email is important, whether it \
belongs to a particular invoice, or whether anything else should follow from it.
- Judge what the email SAYS, not what you infer must be true. Related emails in this process \
describe adjacent steps that sound similar; a message about a neighbouring step is not a match.
- If the email is ambiguous, or you are less than confident, answer false. A false answer costs \
nothing — the same email will be re-examined shortly. A wrong true answer is recorded \
permanently and cannot be withdrawn.
- The subject line carries meaning here and is often more precise than the body.

Reply with the verdict object only: `matches` is your answer, `reason` is one short sentence \
quoting the words you decided on.\
"""

# `additionalProperties: false` + `required` are what structured outputs need to constrain the
# response exactly; without them the schema is advisory.
VERDICT_SCHEMA = {
    "type": "object",
    "properties": {
        "matches": {"type": "boolean"},
        "reason": {"type": "string"},
    },
    "required": ["matches", "reason"],
    "additionalProperties": False,
}


@dataclass(frozen=True)
class Verdict:
    matches: bool
    reason: str = ""


class Classifier(Protocol):
    """Answers one yes/no question about one message."""

    def judge(self, *, question: str, message: dict) -> Verdict: ...


def build_prompt(question: str, message: dict) -> str:
    """The user turn: the question, then the email, fenced and labelled.

    Pure, so the exact text a stage would send is inspectable — and diffable — without an API
    key. The fields are the ones `adapters.gmail.normalise` produces, so a change there shows
    up here as a test failure rather than as a quietly worse classification.
    """
    body = str(message.get("snippet") or "")[:MAX_BODY_CHARS]
    return (
        f"Question: {question.strip()}\n\n"
        "Email:\n"
        f"From: {message.get('from', '')}\n"
        f"Subject: {message.get('subject', '')}\n"
        "---\n"
        f"{body}\n"
        "---"
    )


def parse_verdict(text: str) -> Verdict:
    """The model's JSON into a `Verdict`.

    Kept out of the adapter so the parsing is testable without the SDK, and so a malformed
    reply raises here rather than turning into a silent `false` — a stage that never fires
    because the response shape drifted would look exactly like a payment that never arrived.
    """
    data = json.loads(text)
    if not isinstance(data, dict) or "matches" not in data:
        raise ValueError(f"classifier returned no verdict: {text[:200]!r}")
    return Verdict(matches=bool(data["matches"]), reason=str(data.get("reason", "")))
