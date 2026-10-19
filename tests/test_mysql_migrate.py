"""The migration runner (`scripts/mysql_migrate.py`) and the schema it applies.

Two things are pinned here, and neither is about SQL being well-formed.

The first is the statement splitter, which is naive by design and was *immediately* exercised
by the real file: three of its comments contain a semicolon ("orders by opened_at; this serves
both"). Splitting before stripping comments cuts a CREATE TABLE in half, and the halves are
still syntactically plausible enough to reach the server.

The second is that `migrations/001_initial_schema.sql` and
`src/reconciler/adapters/mysql.py` agree about column names. They are two files that must
describe one schema, with nothing but attention connecting them — and the failure mode is an
`Unknown column` at the first query from a pod, long after the migration reported success.
"""

import importlib.util
import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "migrations" / "001_initial_schema.sql"

_spec = importlib.util.spec_from_file_location("mysql_migrate", ROOT / "scripts" / "mysql_migrate.py")
migrate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migrate)

from reconciler.adapters import mysql as adapter   # noqa: E402


# --- the splitter -----------------------------------------------------------------------------

def test_a_semicolon_inside_a_comment_does_not_split_a_statement():
    """The case the real migration hit on its first parse. A comment is prose, and prose has
    semicolons in it; splitting first turns one CREATE TABLE into two fragments."""
    sql = """
    CREATE TABLE t (
        -- ordered by opened_at; this serves both without a sort
        a INT
    );
    """
    assert len(migrate.statements(sql)) == 1


def test_comments_are_stripped_from_what_gets_executed():
    assert "--" not in "".join(migrate.statements("-- a comment\nSELECT 1;"))


def test_blank_statements_are_dropped():
    assert migrate.statements(";;\n\n;") == []


def test_the_real_migration_is_three_create_tables():
    stmts = migrate.statements(MIGRATION.read_text())
    assert len(stmts) == 3
    assert all(s.upper().startswith("CREATE TABLE") for s in stmts)


# --- checksum bookkeeping ---------------------------------------------------------------------

class FakeCursor:
    def __init__(self, applied):
        self._applied = applied

    def execute(self, sql, params=None):
        pass

    def fetchall(self):
        return list(self._applied.items())


def test_an_unapplied_migration_is_pending():
    assert migrate.pending(FakeCursor({}), [MIGRATION]) == [MIGRATION]


def test_an_applied_migration_is_skipped():
    import hashlib
    digest = hashlib.sha256(MIGRATION.read_bytes()).hexdigest()
    assert migrate.pending(FakeCursor({MIGRATION.name: digest}), [MIGRATION]) == []


def test_editing_an_applied_migration_is_refused():
    """The database and this directory would otherwise disagree about what the schema IS, and
    every later reader of `migrations/` would be reading fiction. One hash prevents an
    afternoon of "but the file says..."."""
    with pytest.raises(SystemExit, match="has been edited"):
        migrate.pending(FakeCursor({MIGRATION.name: "0" * 64}), [MIGRATION])


# --- the schema and the adapter must describe the same tables ---------------------------------

def schema_columns() -> dict[str, set[str]]:
    """Table -> column names, read out of the DDL.

    A deliberately small parser rather than a dependency: it only has to understand the three
    tables in this repo, and a test that needed sqlglot to run would not run in CI, which
    installs four packages.
    """
    tables: dict[str, set[str]] = {}
    for statement in migrate.statements(MIGRATION.read_text()):
        name = re.search(r"CREATE TABLE (\w+)", statement, re.I).group(1)
        tables[name] = {
            m.group(1) for m in re.finditer(
                r"^\s*(\w+)\s+(?:VARCHAR|BIGINT|JSON|TEXT|CHAR)\b",
                statement, re.I | re.M)
        }
    return tables


@pytest.mark.parametrize("table, expected", [
    ("cycles", {"cycle_key", "process", "user_id", "opened_at", "context"}),
    ("milestones", {"cycle_key", "stage", "occurred_at", "payload"}),
    ("tasks", {"upstream_id", "user_id", "label", "subject", "from_name",
               "from_address", "opened_at", "closed_at", "closed_via"}),
])
def test_the_schema_defines_the_columns_it_is_meant_to(table, expected):
    assert schema_columns()[table] == expected


@pytest.mark.parametrize("sql_const, table", [
    ("_CYCLES_SQL", "cycles"),
    ("_MILESTONES_SQL", "milestones"),
    ("_OPEN_TASKS_SQL", "tasks"),
])
def test_every_column_the_adapter_selects_exists_in_the_schema(sql_const, table):
    """The guard that connects two files nothing else connects.

    Rename a column in the migration and the adapter still looks correct, CI still passes, the
    migration still applies — and the first query from a pod fails with `Unknown column`,
    hours later and nowhere near the change.
    """
    statement = getattr(adapter, sql_const)
    selected = statement.split("FROM")[0].replace("SELECT", "")
    columns = {c.strip() for c in selected.split(",") if c.strip()}
    # The WHERE clause's columns matter just as much as the projection's.
    columns |= set(re.findall(r"(?:WHERE|AND)\s+(\w+)\s*(?:=|IS)", statement))

    unknown = columns - schema_columns()[table]
    assert not unknown, f"{sql_const} uses {unknown}, absent from `{table}`"
