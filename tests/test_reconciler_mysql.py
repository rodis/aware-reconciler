"""The MySQL store adapter (reconciler.adapters.mysql) and the store selection in `app`.

No database: `connection_args` is pure, and the two readers touch MySQL through exactly one
method (`_Reader._query`), which these tests replace with a list of rows. That is the same
shape the rest of the tier tests in — the point of a port is that the thing behind it can be a
fake.

What is pinned here is mostly **the decisions that are invisible at the call site**: whether a
connection is encrypted, whether a missing env var names itself, and which store a given
configuration actually selects. The row-mapping tests are the cheap half.
"""

import pathlib
import urllib.parse

import ssl

import pytest

from reconciler import app
from reconciler.adapters.mysql import MysqlMilestones, MysqlTasks, connection_args
from reconciler.adapters.neon import NeonMilestones, NeonTasks
from reconciler.core import Cycle
from reconciler.definition import GENESIS_STAGE

DSN = "mysql://bob:s3cret@db.example.com:3307/reconciler"

# A REAL certificate of the right shape, kept under tests/ rather than read out of `deploy/`.
#
# It has to be real because `ssl.create_default_context(cafile=...)` parses it, and it has to be
# a MySQL auto-generated one because the properties under test are properties of that shape —
# self-signed, and missing `authorityKeyIdentifier`, which is exactly what Python 3.13's
# VERIFY_X509_STRICT rejects.
#
# It is NOT the deployed CA, and that is the point. Reading `deploy/**` from the code suite
# couples the two halves of a split push: CI checks out the code-only commit, the manifest is
# not in it, and 22 tests die on FileNotFoundError against a tree that is green locally and
# green on merged main. Cost one red `main` on 2026-10-11 (run 38154300627). The deployed
# certificate is still asserted — see the end of this file — just not depended upon.
CA = str(pathlib.Path(__file__).resolve().parent / "fixtures" / "mysql-auto-generated-ca.pem")
CA_DSN = f"{DSN}?ssl_ca={CA}"

# The certificate the pods actually mount. Absent on a code-only commit, which is legitimate.
DEPLOYED_CA = (pathlib.Path(__file__).resolve().parents[1]
               / "deploy/kustomize/base/reconciler/mysql-ca.pem")
ENV_VARS = ("RECONCILER_STORE", "NEON_DATABASE_URL", "MYSQL_DATABASE_URL", "MYSQL_SSL_CA")


@pytest.fixture
def clean_env(monkeypatch):
    """No ambient store configuration — a developer's `.env` must not decide a test."""
    for var in ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


def reader(cls, rows):
    """One reader whose only database call returns `rows`."""
    instance = cls(CA_DSN)
    instance._query = lambda sql, params: rows        # noqa: SLF001 — the seam being faked
    return instance


class Definition:
    """Just enough `ProcessDefinition` for the two queries that take one."""

    name = "dreamhost_invoice"


# --- connection_args: every decision about encryption lives here ------------------------------

def test_a_connection_without_a_ca_is_refused_rather_than_downgraded():
    """The bug this replaces: `ssl={}` reads as "TLS, nothing to verify against", but PyMySQL
    branches on TRUTHINESS and an empty dict is falsy — so it disabled TLS outright. Measured
    against the live server, `SHOW STATUS LIKE 'Ssl_cipher'` came back empty while the adapter
    logged "encrypted but UNAUTHENTICATED". That was not an understated caveat, it was a false
    statement about the wire, which is why there is now no CA-less path at all.
    """
    with pytest.raises(ValueError, match="CA certificate is required"):
        connection_args(DSN)


def test_tls_cannot_be_disabled_at_all():
    """Refused as POLICY, and the docstring says so rather than claiming MySQL forces it.

    It does not: `caching_sha2_password` falls back to an RSA public-key exchange, and a
    plaintext login against the real server succeeds. The reason is that this link crosses
    the public internet carrying invoice amounts and email metadata — which is reason enough
    without borrowing a false one.
    """
    with pytest.raises(ValueError, match="public internet"):
        connection_args(CA_DSN + "&ssl=disable")


def test_the_ca_is_pinned_into_a_real_context():
    args = connection_args(CA_DSN)
    assert isinstance(args["ssl"], ssl.SSLContext)
    # PyMySQL passes an SSLContext through untouched, so the flag pair it would otherwise
    # consult is deliberately absent — two sources of truth for one decision.
    assert "ssl_verify_cert" not in args and "ssl_verify_identity" not in args


def test_the_peer_is_still_authenticated():
    """The two deviations in `tls_context` are narrow, and this is what says so: the hostname
    check is off and the strict-extension audit is off, but certificate verification against
    the pinned CA is NOT — so loosening either one further would fail here."""
    context = connection_args(CA_DSN)["ssl"]
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert len(context.get_ca_certs()) == 1


def test_the_hostname_is_not_checked_against_the_certificate():
    """The CN is `MySQL_Server_8.4.8_Auto_Generated_Server_Certificate`, not
    `mysql.example.com`, so an identity check would reject a server the CA had just
    vouched for."""
    assert connection_args(CA_DSN)["ssl"].check_hostname is False


def test_strict_x509_is_cleared_and_that_is_load_bearing():
    """Pinned so nobody "hardens" it back on.

    Python 3.13 enables VERIFY_X509_STRICT by default. MySQL 8.4's auto-generated CA carries
    only `basicConstraints: CA:TRUE` and omits `authorityKeyIdentifier`, which RFC 5280 strict
    validation requires — so with the flag set, every connection fails as
    CERTIFICATE_VERIFY_FAILED "Missing Authority Key Identifier", which reads like a trust
    problem and is not one. The second assertion is the honest other half: the committed CA
    really does lack the extension, so this is a property of the certificate rather than a
    blanket loosening.
    """
    context = connection_args(CA_DSN)["ssl"]
    assert not context.verify_flags & ssl.VERIFY_X509_STRICT

    strict = ssl.create_default_context(cafile=CA)
    assert strict.verify_flags & ssl.VERIFY_X509_STRICT, "3.13 default changed; revisit"


def test_a_missing_ca_file_fails_at_wiring_not_at_first_query():
    """A mount that did not happen should stop the run while the error still names the path,
    not surface as a TLS failure in the middle of advancing a cycle."""
    with pytest.raises(FileNotFoundError):
        connection_args(f"{DSN}?ssl_ca=/no/such/ca.pem")


def test_credentials_and_target_are_parsed():
    args = connection_args(CA_DSN)
    assert (args["host"], args["port"]) == ("db.example.com", 3307)
    assert (args["user"], args["password"]) == ("bob", "s3cret")
    assert args["database"] == "reconciler"


def test_the_port_defaults_rather_than_being_required():
    assert connection_args(f"mysql://u:p@host/db?ssl_ca={CA}")["port"] == 3306


def test_a_password_with_url_syntax_survives_without_corrupting_the_target():
    """The credential is ONE field — a whole DSN — so a generated password is percent-encoded
    into it, and getting that wrong does not fail as "bad password".

    Every URL metacharacter is exercised together because they fail in different places:
    `@` and `:` are survivable unencoded (the authority splits on the LAST `@`, and a password
    may contain colons), while `/`, `?` and `#` each end the authority early. So the thing
    worth pinning is not just that the password round-trips but that the *host, port and
    database still point where they should* — a DSN that silently aims elsewhere is a
    credential bug wearing a network bug's clothes.
    """
    password = "p@ss:w/ord?x#y"
    encoded = urllib.parse.quote(password, safe="")
    args = connection_args(
        f"mysql://rods_reconciler:{encoded}@mysql.example.com:3306/rods_reconciler",
        ssl_ca=CA)

    assert args["password"] == password
    assert args["user"] == "rods_reconciler"
    assert (args["host"], args["port"]) == ("mysql.example.com", 3306)
    assert args["database"] == "rods_reconciler"


@pytest.mark.parametrize("char", ["/", "?", "#"])
def test_an_unencoded_password_says_the_password_is_wrong_not_the_port(char):
    """`urlsplit` reports this as "Port could not be cast to integer value", which sends you
    looking at the DSN's port — the one part that is almost certainly fine. The real cause is
    an unencoded character in the password, and the error has to say so, because the person
    reading it has a secret they cannot print to compare."""
    dsn = f"mysql://rods_reconciler:p{char}ss@mysql.example.com:3306/rods_reconciler"
    with pytest.raises(ValueError, match="percent-encode the password"):
        connection_args(dsn)


@pytest.mark.parametrize("dsn, missing", [
    ("postgres://u:p@host/db", "scheme"),
    ("mysql:///db", "host"),
    ("mysql://u:p@host", "database"),
])
def test_an_unusable_dsn_says_which_part_is_wrong(dsn, missing):
    with pytest.raises(ValueError, match=missing):
        connection_args(dsn)


# --- reading this tier's own state ------------------------------------------------------------

def test_cycles_come_back_as_cycles():
    rows = [("dh_invoice_2026_004", "rods", 1788000000, '{"invoice_number": 4}')]
    cycles = reader(MysqlMilestones, rows).cycles(Definition())

    assert len(cycles) == 1
    assert cycles[0].key == "dh_invoice_2026_004"
    assert cycles[0].process == "dreamhost_invoice"
    assert cycles[0].opened_at == 1788000000
    assert cycles[0].context == {"invoice_number": 4}


def test_context_needs_no_reserved_key_stripping():
    """The Postgres reader rebuilds `context` by deleting `id`/`name`/`cycle_key`/… from the
    genesis body, because there the context and the envelope are one row. Here it is its own
    column, so what is stored is what comes back — and a stripping pass would be a bug waiting
    for a context field that happens to be called `name`."""
    rows = [("k", "rods", 1, '{"name": "a line item", "invoice_number": 4}')]
    assert reader(MysqlMilestones, rows).cycles(Definition())[0].context == {
        "name": "a line item", "invoice_number": 4}


def test_milestones_are_keyed_by_stage_including_genesis():
    """A stage may depend on nothing and still need somewhere to look back to, so the genesis
    event is a milestone like any other rather than a special case."""
    rows = [(GENESIS_STAGE, 1788000000, '{"invoice_number": 4}'),
            ("data_approved", 1788003600, '{"matched_at": 1788003000}')]
    found = reader(MysqlMilestones, rows).milestones(Definition(), Cycle(
        key="dh_invoice_2026_004", process="dreamhost_invoice",
        user_id="rods", opened_at=1788000000))

    assert set(found) == {GENESIS_STAGE, "data_approved"}
    assert found["data_approved"].timestamp == 1788003600
    assert found["data_approved"].payload == {"matched_at": 1788003000}


@pytest.mark.parametrize("stored, expected", [
    ('{"a": 1}', {"a": 1}),                 # PyMySQL hands a JSON column back as text
    (b'{"a": 1}', {"a": 1}),                # ...or as bytes, depending on the charset
    ({"a": 1}, {"a": 1}),                   # ...or decoded, if a future driver does that
    (None, {}),                             # a column never written is empty, not a parse error
])
def test_a_json_column_is_read_whatever_the_driver_returns(stored, expected):
    rows = [("k", "rods", 1, stored)]
    assert reader(MysqlMilestones, rows).cycles(Definition())[0].context == expected


def test_the_store_cannot_read_awares_event_log():
    """`signals()` is deliberately absent, and this test is the whole point of splitting
    `core.EventLog` out of `core.MilestoneStore`.

    A signal is a fact *Aware* recorded that a process is waiting on ("you got home"); it lives
    in Neon and this store genuinely cannot see it. Implementing the method and raising would
    push that discovery to the moment a cycle is mid-flight; not having it means a process whose
    `await` names an Aware event fails to wire up instead, which is the failure this tier
    prefers — `world.NotYetImplemented` exists for exactly the same reason.
    """
    assert not hasattr(MysqlMilestones(CA_DSN), "signals")
    assert hasattr(NeonMilestones("postgres://x/y"), "signals")


# --- reading the open task list ---------------------------------------------------------------

def test_open_tasks_come_back_keyed_by_upstream_id():
    rows = [("mail-1", "Renew car insurance", "AXA", "insurance@example.com", 1788000000)]
    found = reader(MysqlTasks, rows).open_tasks("rods", "aware/todo")

    assert set(found) == {"mail-1"}
    assert found["mail-1"].subject == "Renew car insurance"
    assert found["mail-1"].from_address == "insurance@example.com"
    assert found["mail-1"].opened_at == 1788000000


def test_a_task_with_null_text_columns_is_still_a_task():
    """A task is identified by its Gmail id, so missing prose must degrade the row rather than
    drop it — an untitled mail still has to be tickable, and `None` reaching the dashboard as a
    subject would render as the string "None"."""
    rows = [("mail-1", None, None, None, None)]
    task = reader(MysqlTasks, rows).open_tasks("rods", "aware/todo")["mail-1"]
    assert (task.subject, task.from_name, task.from_address) == ("", "", "")
    assert task.opened_at == 0


def test_openness_is_a_column_not_an_anti_join():
    """The latest-open-vs-latest-close pair of CTEs collapses to one predicate here. Pinned
    because the *reopen* semantics it protected must survive the move: re-labelling a finished
    mail has to reopen it, which on this schema is a write that clears `closed_at` (Phase 3),
    not an `IS NULL` test that can never be undone."""
    from reconciler.adapters.mysql import _OPEN_TASKS_SQL
    assert "closed_at IS NULL" in _OPEN_TASKS_SQL
    assert "DISTINCT" not in _OPEN_TASKS_SQL.upper()


# --- which store a configuration selects ------------------------------------------------------

def test_neon_is_the_default_because_cutover_follows_a_backfill(clean_env):
    """An empty MySQL reads as "nothing is open" rather than as an error, which would make a
    sweep re-open every labelled mail. So the move has to be a deliberate act taken after the
    backfill — and the rollback is the same act in reverse."""
    clean_env.setenv("NEON_DATABASE_URL", "postgres://x/y")
    assert isinstance(app.milestone_store(), NeonMilestones)
    assert isinstance(app.task_store(), NeonTasks)


def test_selecting_mysql_switches_both_stores(clean_env):
    clean_env.setenv("RECONCILER_STORE", "mysql")
    clean_env.setenv("MYSQL_DATABASE_URL", DSN)
    clean_env.setenv("MYSQL_SSL_CA", CA)
    assert isinstance(app.milestone_store(), MysqlMilestones)
    assert isinstance(app.task_store(), MysqlTasks)


def test_a_missing_dsn_names_the_variable_the_choice_implies(clean_env):
    """The two stores read different env vars, so "not set" has to say WHICH — otherwise
    flipping the store and forgetting its DSN reports the other one's name."""
    clean_env.setenv("RECONCILER_STORE", "mysql")
    with pytest.raises(app.ConfigurationError, match="MYSQL_DATABASE_URL"):
        app.milestone_store()


def test_an_unknown_store_is_refused_rather_than_defaulted(clean_env):
    """A typo must not silently fall back to Neon: that is how a cutover appears to have
    happened while every read still goes to the database it was meant to leave."""
    clean_env.setenv("RECONCILER_STORE", "mysqll")
    clean_env.setenv("NEON_DATABASE_URL", "postgres://x/y")
    with pytest.raises(app.ConfigurationError, match="mysqll"):
        app.milestone_store()


# --- where the CA path comes from -------------------------------------------------------------

def test_the_ca_path_can_come_from_the_manifest_instead_of_the_secret():
    """A mount path is deploy-time config, not a credential.

    Threading it through the DSN would mean Doppler held a filesystem path, so moving the
    volume became a secret edit — a console change with no diff and no revert, which is the
    shape this tier already rejected for `RECONCILER_STORE`.
    """
    assert isinstance(connection_args(DSN, ssl_ca=CA)["ssl"], ssl.SSLContext)


def test_the_argument_wins_over_a_ca_in_the_dsn():
    """Both sources stay supported — the query param so a DSN is self-contained for a one-off
    laptop run — but the manifest governs a deployed pod, so it wins rather than collides.

    The DSN here names a path that does not exist, which is what makes the assertion mean
    something: if the query param won, this would raise FileNotFoundError instead.
    """
    args = connection_args(f"{DSN}?ssl_ca=/stale/from-the-secret.pem", ssl_ca=CA)
    assert args["ssl"].get_ca_certs()[0]["subject"][0][0][1].startswith("MySQL_Server_")


def test_the_store_takes_the_ca_path_from_the_environment(clean_env):
    """`MYSQL_SSL_CA` is read at the composition root, so `connection_args` stays a pure
    function of its arguments and the adapter never reaches for os.environ."""
    clean_env.setenv("RECONCILER_STORE", "mysql")
    clean_env.setenv("MYSQL_DATABASE_URL", DSN)
    clean_env.setenv("MYSQL_SSL_CA", CA)
    for store in (app.milestone_store(), app.task_store()):
        assert isinstance(store._args["ssl"], ssl.SSLContext)


def test_an_empty_ca_variable_is_refused_like_an_absent_one(clean_env):
    """A Doppler field that exists but is blank, or a manifest default nobody filled in, must
    not quietly become "no CA" — which, before the fix this pins, meant no TLS."""
    clean_env.setenv("RECONCILER_STORE", "mysql")
    clean_env.setenv("MYSQL_DATABASE_URL", DSN)
    clean_env.setenv("MYSQL_SSL_CA", "")
    with pytest.raises(ValueError, match="CA certificate is required"):
        app.milestone_store()


@pytest.mark.skipif(not DEPLOYED_CA.exists(),
                    reason="deploy/ not in this tree — a code-only commit of a split push")
def test_the_deployed_ca_is_a_usable_trust_root():
    """The certificate in `deploy/` is not decoration — it is the trust root the pods mount, so
    a truncated or swapped file must fail here rather than at the first connection from a pod.

    Skipped rather than failed when `deploy/` is absent: a split push puts the code on `main`
    one commit before the manifest, and that intermediate tree is legitimate. The skip is what
    lets this assertion exist at all without re-coupling the suite to the deploy tree.
    """
    certs = ssl.create_default_context(cafile=str(DEPLOYED_CA)).get_ca_certs()
    assert len(certs) == 1
    assert "Auto_Generated_CA_Certificate" in str(certs[0]["subject"])
