"""Adapters — the concrete edges of the process tier (ADR 0012).

Everything here talks to something real: Neon, the ingest gateway, an SMTP server. Nothing
here is imported by `core`, `definition` or `money`, which is what keeps those testable with
no dependencies at all.

Deliberately stdlib-or-existing-dependency: `psycopg` is already a base dependency of this
repo, and `smtplib`/`urllib.request` ship with Python. So the `processes` optional extra
stays empty until the Prefect entry point actually needs it.
"""
