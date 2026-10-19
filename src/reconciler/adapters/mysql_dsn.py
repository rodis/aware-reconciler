"""Turning a MySQL DSN into a connection — the plumbing, with no domain model attached.

Split out of `adapters.mysql` so that **tools can share the connection story without importing
the tier**. `scripts/mysql_migrate.py` creates three tables; it has no business pulling in
`reconciler.core` -> `reconciler.definition` -> pydantic and pyyaml to do it, and before this
split it did exactly that and died on `ModuleNotFoundError: yaml` in its own PEP 723 env.

Sharing it is not optional, though: a migration runner that connected more loosely than the
application would be the worst possible place to discover a TLS difference. So the plumbing
moves here rather than being duplicated, and this module imports nothing but the standard
library.
"""

import logging
import ssl
import urllib.parse

logger = logging.getLogger("reconciler.adapters.mysql")

DEFAULT_PORT = 3306


def connection_args(dsn: str, *, ssl_ca: str | None = None) -> dict:
    """Parse `mysql://user:pass@host:port/db` into PyMySQL's keyword arguments.

    `ssl_ca` is passed separately and **wins over** a `?ssl_ca=` in the DSN, because the two
    carry different kinds of fact. The DSN is a credential, authored in Doppler; the path to a
    mounted certificate is deploy-time config, authored in the manifest beside the volume that
    puts it there. Threading it through the secret would mean moving the mount became a secret
    edit — a console change with no diff and no revert, which is the shape this tier already
    decided against for `RECONCILER_STORE`. The query param stays supported so a DSN is still
    usable on its own, for a one-off run from a laptop.

    Extracted and public so it can be asserted directly — every decision that governs whether
    this connection is encrypted is made here, and a test should be able to read it without a
    database.

    **A CA is required, and there is no CA-less mode.** Not because MySQL demands TLS — it
    does not, as `caching_sha2_password` falls back to an RSA key exchange — but because the
    only concise way to express "TLS without a CA" in PyMySQL is `ssl={}`, and PyMySQL
    branches on truthiness, so an empty dict turns TLS *off*. A mode whose honest description
    is "silently plaintext" is worse than no mode, so this raises instead. `?ssl=disable`
    raises too, as policy: the link crosses the public internet.

    The resulting `ssl` argument is a built `SSLContext` rather than PyMySQL's
    `ssl_verify_cert` / `ssl_verify_identity` flags, because those were tried against the live
    server and could not express what it needs. See `tls_context`.
    """
    parsed = urllib.parse.urlsplit(dsn)
    if parsed.scheme not in {"mysql", "mysql+pymysql"}:
        raise ValueError(f"not a mysql DSN: scheme {parsed.scheme!r}")

    try:
        host, port = parsed.hostname, parsed.port
    except ValueError as e:
        # `urlsplit` raises here only when the authority was cut short, which in a DSN whose
        # one variable part is a generated password means an unencoded `/`, `?` or `#` in it:
        # each ends the authority early and leaves a scrap of the password where the port
        # should be. Its own message talks about a PORT — nowhere near the mistake — so it is
        # replaced rather than propagated. (`@` and `:` are safe unencoded, because the netloc
        # splits on the LAST `@` and a password may contain colons; encode anyway.)
        raise ValueError(
            "could not read host/port from the mysql DSN. This is almost always an unencoded "
            "'/', '?' or '#' in the password, which truncates the authority — percent-encode "
            "the password in MYSQL_DATABASE_URL."
        ) from e

    if not host:
        raise ValueError("mysql DSN has no host")

    database = parsed.path.lstrip("/")
    if not database:
        raise ValueError("mysql DSN names no database")

    query = urllib.parse.parse_qs(parsed.query)
    args: dict = {
        "host": host,
        "port": port or DEFAULT_PORT,
        "user": urllib.parse.unquote(parsed.username or ""),
        "password": urllib.parse.unquote(parsed.password or ""),
        "database": database,
        "charset": "utf8mb4",
        # Reads only, but an open transaction on a connection that is about to be closed is
        # noise in the server's process list and nothing else.
        "autocommit": True,
        "connect_timeout": int(query.get("connect_timeout", ["10"])[0]),
    }

    if query.get("ssl", ["require"])[0] == "disable":
        # Policy, and only policy — stated plainly because the tempting justification is
        # false. It was believed for a while that `caching_sha2_password` made TLS mandatory
        # for authentication; testing against the live server disproved it, since the handshake
        # falls back to an RSA public-key exchange and a plaintext connection logs in fine.
        # So nothing *forces* TLS here. It is refused because this link crosses the public
        # internet carrying invoice amounts, email subjects and sender addresses, and that is
        # sufficient on its own.
        raise ValueError(
            "?ssl=disable is not supported. The reconciler reaches mysql.example.com across "
            "the public internet carrying invoice amounts and email metadata, so plaintext is "
            "refused as policy. (MySQL would permit it — caching_sha2_password falls back to "
            "an RSA key exchange — which is exactly why the refusal has to be explicit.)")

    ca = ssl_ca or query.get("ssl_ca", [None])[0]
    if not ca:
        # NO DEGRADED MODE, because the obvious one is a silent downgrade to plaintext.
        # `ssl={}` reads as "TLS with nothing to verify against", and an earlier version of
        # this adapter passed exactly that — but PyMySQL branches on the argument's
        # TRUTHINESS, and an empty dict is falsy, so it disables TLS outright. Measured
        # against the live server: `SHOW STATUS LIKE 'Ssl_cipher'` came back empty. The
        # accompanying warning said "encrypted but unauthenticated", which was not a caveat
        # but a false statement about the wire.
        raise ValueError(
            "a CA certificate is required. Set MYSQL_SSL_CA to the mounted path (the "
            "manifest does this), or put ?ssl_ca=<path> in the DSN for a one-off run. There "
            "is deliberately no CA-less mode: the only concise way to express one in PyMySQL "
            "silently connects in plaintext.")
    args["ssl"] = tls_context(ca)
    return args


def tls_context(ca: str) -> ssl.SSLContext:
    """A context that pins `ca` and connects to *this* server — the one shape that works.

    PyMySQL passes an `SSLContext` through untouched, which is why the context is built here
    rather than described with `ssl_verify_cert` / `ssl_verify_identity`. Those were tried
    against the live server and all failed; the two deviations below are each load-bearing,
    and each would look like something to tidy away.
    """
    context = ssl.create_default_context(cafile=ca)

    # The server presents MySQL 8.4's auto-generated certificate, whose CN is
    # `MySQL_Server_8.4.8_Auto_Generated_Server_Certificate` rather than the hostname, so a
    # hostname check cannot pass. Verification of the chain is untouched by this —
    # `verify_mode` stays `CERT_REQUIRED` — so the peer is still authenticated, by the CA
    # rather than by its name. The same trade the estate already makes for vmauth.
    context.check_hostname = False

    # DO NOT REMOVE — without this every connection fails with CERTIFICATE_VERIFY_FAILED
    # ("Missing Authority Key Identifier"), which reads like a trust problem and is not one.
    #
    # Python 3.13 turns `VERIFY_X509_STRICT` on by default in `create_default_context`. MySQL
    # 8.4's auto-generated CA carries only `basicConstraints: CA:TRUE` and omits
    # `authorityKeyIdentifier`, which RFC 5280 strict validation requires — so the certificate
    # is rejected for a MISSING EXTENSION, not for anything about its signature or its chain.
    # Clearing this one flag drops the extension-presence audit and nothing else:
    # `verify_mode` is still `CERT_REQUIRED`, the signature is still checked against the
    # pinned CA, and `openssl verify -CAfile` agrees independently.
    #
    # Verified on Python 3.13.16 / OpenSSL 3.5.7 against mysql.example.com: TLSv1.3,
    # TLS_AES_256_GCM_SHA384, peer authenticated.
    context.verify_flags &= ~ssl.VERIFY_X509_STRICT

    return context


