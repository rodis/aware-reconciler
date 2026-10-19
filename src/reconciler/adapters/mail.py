"""Mail transports (ADR 0012).

Four implementations behind one port.

`N8nRelayMailer` is the default: it POSTs a fully-composed message to a tiny n8n workflow that
holds the SMTP credential, so **no personal credential ever has to live in the repo**, not even
gitignored. The reconciler still composes every byte and decides whether to send — n8n only
authenticates and transmits, which is the outbound mirror of ADR 0008's connector rule and is
why this does not reintroduce the "process state in n8n" that ADR 0012 rejected.

`ConsoleMailer` and `FileMailer` exist because the first thing you want from an approval mail
is to *read* it. `SmtpMailer` remains the escape hatch for local-only testing or if n8n is
down; it is stdlib, so keeping it costs nothing.
"""

import json
import logging
import smtplib
import sys
import urllib.error
import urllib.request
from email.message import EmailMessage

logger = logging.getLogger("reconciler.adapters.mail")


class ConsoleMailer:
    """Prints the mail instead of sending it."""

    def __init__(self, stream=None, show_html: bool = False):
        self._stream = stream or sys.stdout
        self._show_html = show_html

    def send(self, *, subject: str, html: str, text: str) -> None:
        print("=" * 72, file=self._stream)
        print(f"SUBJECT: {subject}", file=self._stream)
        print("=" * 72, file=self._stream)
        print(text, file=self._stream)
        if self._show_html:
            print("-" * 72 + "\nHTML:\n" + html, file=self._stream)


class FileMailer:
    """Writes the HTML body to a file — for opening the real rendering in a browser."""

    def __init__(self, path):
        self._path = path

    def send(self, *, subject: str, html: str, text: str) -> None:
        self._path.write_text(f"<!-- {subject} -->\n{html}")
        logger.info("wrote mail preview to %s", self._path)


class SmtpMailer:
    """Sends over SMTP with STARTTLS, or implicit TLS on port 465."""

    def __init__(self, *, host: str, port: int, username: str, password: str,
                 sender: str, recipient: str, timeout: float = 30.0):
        self._host, self._port = host, port
        self._username, self._password = username, password
        self._sender, self._recipient = sender, recipient
        self._timeout = timeout

    def send(self, *, subject: str, html: str, text: str) -> None:
        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = self._sender
        message["To"] = self._recipient
        # Plain text first, HTML as the alternative — so a reply quotes something readable,
        # and the await that classifies that reply has clean text to work with.
        message.set_content(text)
        message.add_alternative(html, subtype="html")

        if self._port == 465:
            server = smtplib.SMTP_SSL(self._host, self._port, timeout=self._timeout)
        else:
            server = smtplib.SMTP(self._host, self._port, timeout=self._timeout)
        with server:
            if self._port != 465:
                server.starttls()
            server.login(self._username, self._password)
            server.send_message(message)
        logger.info("sent %r to %s", subject, self._recipient)


DEFAULT_RELAY_HEADER = "X-Relay-Token"


class N8nRelayMailer:
    """Sends by POSTing a composed message to the n8n mail relay.

    The token is the only secret the reconciler holds, and deliberately so: we mint it, it is
    scoped to one webhook, it is revocable in seconds, and it carries no personal data —
    categorically unlike a mail account password.

    **Deliberately does NOT retry**, unlike the Gmail query. The relay answers only *after* its
    send node, so a timeout is ambiguous: the mail may well have gone out and only the response
    was lost. Retrying would send a second invoice mail. A read-only search can be repeated for
    free; an email cannot be un-sent, so this fails once and lets the next reconciler run
    decide — and since the milestone was never recorded, that run simply tries again.
    """

    def __init__(self, *, url: str, token: str, recipient: str,
                 sender: str | None = None, header: str = DEFAULT_RELAY_HEADER,
                 timeout: float = 30.0):
        self._url = url
        self._token = token
        self._recipient = recipient
        self._sender = sender
        self._header = header
        self._timeout = timeout

    def send(self, *, subject: str, html: str, text: str) -> None:
        body = {"to": self._recipient, "subject": subject, "html": html, "text": text}
        if self._sender:
            body["from"] = self._sender
        request = urllib.request.Request(
            self._url,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", self._header: self._token},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                status = response.status
        except urllib.error.HTTPError as e:
            # 401/403 means the token is wrong; anything else means SMTP refused it. Either
            # way this must raise: the relay responds only AFTER the send node, so a failure
            # here is a mail that did not go out, and swallowing it would stall the process
            # at a gate nobody is watching.
            raise RuntimeError(
                f"mail relay rejected {subject!r}: HTTP {e.code} {e.reason}"
            ) from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"mail relay unreachable: {e}") from e

        logger.info("relayed %r to %s (HTTP %s)", subject, self._recipient, status)
