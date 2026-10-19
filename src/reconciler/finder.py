"""Resolving `await` signals (ADR 0012).

An `await` asks "has the fact this stage waits for appeared yet?". Answering it has two parts,
split here on purpose:

- **fetching candidates** — a `CandidateSource`, the only thing that touches the outside world;
- **deciding whether one satisfies the stage** — `SignalFinder`, pure and fully tested.

Every loop lives on this side. n8n is asked a question and answers it; it never notices
anything, never polls, and never decides. That split exists because the alternative — a
polling connector pushing events — put the "has it happened yet?" loop in two places, and
made "n8n is down" indistinguishable from "not approved yet". A synchronous question fails
loudly; a missing push is silent.

For a human decision there is nothing to interpret: **applying a Gmail label IS the decision**.
Classification is reserved for genuine semantic work, like telling a *submitted* payment from
a merely *processed* one.

**Where a classifier sits is load-bearing.** It runs last, after every deterministic filter, so
it is asked only about mail already known to be from the right sender and about the right
invoice — and so a run with nothing to judge costs nothing. Identity is never its job: see
`reconciler.classify`.
"""

import logging
import re
from typing import Protocol

from reconciler.classify import Classifier
from reconciler.core import Cycle, Milestone

logger = logging.getLogger("reconciler.finder")

DEFAULT_SLACK_SECONDS = 900


class CandidateSource(Protocol):
    """Fetches things that *might* satisfy a signal. Interprets nothing."""

    def candidates(self, signal: dict, since: int) -> list[tuple[int, dict]]: ...


class SignalFinder:
    """Satisfies a stage when a candidate matches it.

    Signal grammar, common to every source:

        correlate_on:     a field that must match the SAME field on a predecessor milestone,
                          which is what ties evidence to *its* cycle rather than to whichever
                          cycle happens to be open
        mentions:         the same tie, for evidence we did NOT send: the milestone value must
                          appear somewhere in the message's text
        where:            plain field equality filters, e.g. {from: accounting@example.com}
        classify:         a yes/no question a `Classifier` answers about the message
        slack_seconds:    how far before `since` to look

    **Slack is not sloppiness.** For a mail we sent ourselves, the evidence's time is the
    message's own `Date` — i.e. roughly when the approval request went out, not when the label
    was applied. So it legitimately carries a timestamp at or just before the milestone that
    triggered it, and a strict `>= since` would never match. `correlate_on` is what keeps the
    widened window safe.

    **`mentions` is `correlate_on` for mail somebody else composed.** Our own approval request
    carries the invoice number in a field, so equality works; Tipalti's notifications carry it
    in a sentence, so the tie has to be a substring test. It is still deterministic, and it is
    still what makes the widened window safe — the classifier is never asked whose invoice a
    mail is about.

    Filters run cheapest-first and the classifier runs **last**, so a reconcile that finds no
    plausible mail spends nothing on interpretation.
    """

    def __init__(self, source: CandidateSource, classifier: Classifier | None = None):
        self._source = source
        self._classifier = classifier

    def find(self, signal: dict, cycle: Cycle, since: int,
             milestones: dict[str, Milestone] | None = None) -> dict | None:
        slack = int(signal.get("slack_seconds", DEFAULT_SLACK_SECONDS))
        where = signal.get("where", {})
        correlate_on = signal.get("correlate_on")
        mentions = signal.get("mentions")
        question = signal.get("classify")

        expected = None
        if correlate_on:
            expected = _expected_value(correlate_on, milestones or {})
            if expected is None:
                # Fail closed. Matching "any approval at all" would let one cycle's label
                # close another cycle's gate.
                logger.warning(
                    "cycle %s: nothing to correlate %r against yet; not matching",
                    cycle.key, correlate_on)
                return None

        wanted = None
        if mentions:
            value = _expected_value(mentions, milestones or {})
            if value is None:
                logger.warning(
                    "cycle %s: nothing for %r to be mentioned yet; not matching",
                    cycle.key, mentions)
                return None
            wanted = _mention_variants(value)

        if question and self._classifier is None:
            # Loud, for the same reason `world.NotYetImplemented` is: an await that silently
            # skipped its own question would be indistinguishable from one still waiting.
            raise RuntimeError(
                f"cycle {cycle.key}: signal asks a `classify` question but no classifier "
                f"is wired for source {signal.get('source', '?')!r}")

        for ts, body in self._source.candidates(signal, max(0, since - slack)):
            if any(body.get(field) != value for field, value in where.items()):
                continue
            if expected is not None and body.get(correlate_on) != expected:
                continue
            if wanted is not None and not _mentions(body, wanted):
                continue

            found = {"matched": signal.get("source", "signal"),
                     "matched_at": ts, "evidence": body}
            if question:
                # Errors propagate: an unreachable classifier must not read as "not yet".
                verdict = self._classifier.judge(question=question, message=body)
                if not verdict.matches:
                    logger.info("cycle %s: candidate at %s read as not-a-match (%s)",
                                cycle.key, ts, verdict.reason)
                    continue
                found["reading"] = verdict.reason

            logger.info("cycle %s: matched evidence at %s", cycle.key, ts)
            return found
        return None


def _mention_variants(value) -> list[str]:
    """The forms a milestone value may legitimately take in someone else's prose.

    Just one variation today, and it is a real one: we render the invoice number zero-padded
    (`08-2026`) and DreamHost's mail writes it bare (`7-2026`). Stripping the padding covers
    both directions at once, since the unpadded form is a substring of neither — it has to be
    searched for in its own right.
    """
    text = str(value).strip()
    unpadded = re.sub(r"(?<!\d)0+(\d)", r"\1", text)
    return [v.lower() for v in dict.fromkeys([text, unpadded]) if v]


def _mentions(body: dict, wanted: list[str]) -> bool:
    haystack = f"{body.get('subject', '')} {body.get('snippet', '')}".lower()
    return any(v in haystack for v in wanted)


def _expected_value(field: str, milestones: dict[str, Milestone]):
    """The value a signal must carry, taken from the most recent milestone that has it.

    Most recent, not first: a process that asks twice (a re-opened cycle, a resent mail) must
    correlate against the *latest* thing it said, or it would match its own stale request
    forever.
    """
    candidates = [m for m in milestones.values() if field in m.payload]
    if not candidates:
        return None
    return max(candidates, key=lambda m: m.timestamp).payload[field]


class NeonEvents:
    """Candidates are recorded raw events of one name (`signal.event`).

    **No current consumer.** The invoice's gates all read Gmail directly, so this exists for
    the obvious next case — a process awaiting an *Aware-derived* fact ("you got home"), which
    genuinely is something only Neon knows. Flagged rather than deleted, and worth deleting if
    it is still unused when the second process lands: ADR 0009 records `decaying_window` sitting
    registered with zero consumers for months, and that is the smell being watched for here.
    """

    def __init__(self, source):
        self._source = source

    def candidates(self, signal: dict, since: int) -> list[tuple[int, dict]]:
        return self._source.signals(signal["event"], since)
