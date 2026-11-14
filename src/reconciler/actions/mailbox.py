"""Mailbox housekeeping: archiving the mails a stage was satisfied by.

Every `await` in the invoice is closed by a mail — our own approval request and PDF mail
(labelled by a human), and DreamHost's two payment notices (read by the classifier). Once the
process has moved past one, the mail has done its job and only clutters the inbox. This action
archives it: Gmail has no "archive" call, archiving IS removing the `INBOX` label, so the same
label-remove relay the task board uses does it with no n8n change (`INBOX` is a system label
and appears in the relay's label listing like any other).

**Which mail is not a guess.** A satisfied `await` records the message it matched under
`evidence.upstream_id`; `config.evidence_of` names that stage. The action never searches.

**Why an `act` stage, not a side effect of the await.** A recorded milestone is what stops the
next pass doing it again, and it puts the archiving on the board where it can be seen. It also
keeps the timing a YAML decision: the invoice mail is archived as the LAST stage, not when its
own gate closes, because it is the one mail worth keeping in view until the money has landed.

**Removing the approval label too** (`config.remove_labels`) is what makes the archive stick:
a mail still labelled `aware/invoice-approved` keeps showing under that label, which is the
view a human checks to see what is still in flight.

Config:
    evidence_of:    the await stage whose matched mail to archive (required)
    remove_labels:  further labels to take off, after `INBOX` (optional)
"""

import logging

from reconciler.actions import ActionContext, register_action

logger = logging.getLogger("reconciler.actions.mailbox")

INBOX = "INBOX"


class ConsoleLabelRemover:
    """The dry-run relay: says what it would remove, removes nothing."""

    def remove(self, *, message_id: str, label: str) -> None:
        print(f"[dry-run] would remove {label} from {message_id}")


@register_action("gmail.archive")
def archive(ctx: ActionContext) -> dict:
    stage = ctx.config.get("evidence_of")
    if not stage:
        raise ValueError("gmail.archive needs `evidence_of`: the stage whose mail to archive")
    # `after` guarantees the stage is recorded; a KeyError here is a definition bug.
    evidence = ctx.milestones[stage].payload.get("evidence") or {}
    message_id = evidence.get("upstream_id")
    if not message_id:
        # Recorded, not raised. A raise would stop every later cycle in the same pass, every
        # pass, for a mail there is no way to identify — and there is nothing to retry.
        logger.warning("cycle %s: %s recorded no message id; nothing to archive",
                       ctx.cycle.key, stage)
        return {"evidence_of": stage, "message_id": None, "removed": []}

    relay = ctx.services.labels
    if relay is None:
        raise RuntimeError("gmail.archive needs a label relay; GMAIL_LABEL_URL is not set.")

    labels = [INBOX, *ctx.config.get("remove_labels", [])]
    # One call per label: the relay takes one. INBOX first, so a failure part-way leaves the
    # mail archived-but-labelled (still findable under the label) rather than the reverse.
    for label in labels:
        relay.remove(message_id=message_id, label=label)
    logger.info("cycle %s: archived the %s mail %s (removed %s)",
                ctx.cycle.key, stage, message_id, ", ".join(labels))
    return {"evidence_of": stage, "message_id": message_id, "removed": labels}
