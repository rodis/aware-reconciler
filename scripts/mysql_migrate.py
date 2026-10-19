#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["pymysql>=1.1"]
# ///
"""Apply `migrations/*.sql` to the reconciler's MySQL, in order, once each.

A migration runner rather than a framework, because three tables do not justify Alembic and
there is no precedent for one anywhere in this estate. The whole mechanism: numbered files,
applied in filename order, recorded in `schema_migrations` so a re-run is a no-op. No
downgrades — a rollback here is "restore from the event log", which `backfill_mysql.py` does
better than a reverse migration could.

    MYSQL_DATABASE_URL=mysql://... MYSQL_SSL_CA=deploy/.../mysql-ca.pem \\
        uv run scripts/mysql_migrate.py [--dry-run]

Reuses `reconciler.adapters.mysql_dsn.connection_args`, so the TLS story is the one the
adapter already proves out — a pinned CA, no plaintext path, and the same refusals. A migration runner
that connected more loosely than the application would be the wrong place to discover that.

Each file runs in ONE transaction with its `schema_migrations` row, so a half-applied file
cannot be recorded as done. MySQL DDL is not transactional (each CREATE TABLE commits
implicitly), so that guarantee is weaker than it looks: what it actually buys is that the
bookkeeping cannot outrun the DDL, and a file that dies midway leaves the tables it created
and no record, so the re-run fails loudly on "table already exists" rather than silently
skipping. That is the right failure — it asks a human to look.
"""

import argparse
import hashlib
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))

from reconciler.adapters.mysql_dsn import connection_args   # noqa: E402

MIGRATIONS = pathlib.Path(__file__).resolve().parent.parent / "migrations"

_BOOKKEEPING = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        filename    VARCHAR(255) NOT NULL,
        checksum    CHAR(64)     NOT NULL,
        applied_at  BIGINT       NOT NULL,
        PRIMARY KEY (filename)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def pending(cursor, files: list[pathlib.Path]) -> list[pathlib.Path]:
    """Files not yet recorded — and a loud failure if an applied one has since been edited.

    The checksum is not ceremony. A migration edited after it ran means the database and the
    repository disagree about what the schema *is*, and every later reader of this directory
    would be reading fiction. Catching it here costs one hash; not catching it costs an
    afternoon of "but the file says...".
    """
    cursor.execute("SELECT filename, checksum FROM schema_migrations")
    applied = dict(cursor.fetchall())

    todo = []
    for path in files:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if path.name not in applied:
            todo.append(path)
        elif applied[path.name] != digest:
            raise SystemExit(
                f"{path.name} was applied with checksum {applied[path.name][:12]}… but now "
                f"hashes to {digest[:12]}…. An applied migration has been edited; the database "
                f"and this directory no longer describe the same schema. Add a new migration "
                f"instead of changing a shipped one.")
    return todo


def statements(sql: str) -> list[str]:
    """Split a file into statements on `;`, dropping comments and blanks.

    Deliberately naive — it would mangle a `;` inside a string literal or a trigger body, and
    these files contain neither. Said out loud so the next person adding a stored routine
    knows to reach for a real parser rather than discovering this at 2am.
    """
    stripped = "\n".join(line for line in sql.splitlines()
                         if not line.strip().startswith("--"))
    return [s.strip() for s in stripped.split(";") if s.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="say what would be applied, connect but change nothing")
    args = parser.parse_args()

    dsn = os.environ.get("MYSQL_DATABASE_URL")
    if not dsn:
        raise SystemExit("MYSQL_DATABASE_URL is not set")

    files = sorted(MIGRATIONS.glob("*.sql"))
    if not files:
        raise SystemExit(f"no migrations found in {MIGRATIONS}")

    import pymysql
    connection = pymysql.connect(
        **connection_args(dsn, ssl_ca=os.environ.get("MYSQL_SSL_CA") or None))
    try:
        with connection.cursor() as cursor:
            cursor.execute(_BOOKKEEPING)
            todo = pending(cursor, files)

            if not todo:
                print(f"up to date — {len(files)} migration(s) already applied")
                return 0

            print(f"pending: {', '.join(p.name for p in todo)}")
            if args.dry_run:
                print("[dry-run] nothing applied")
                return 0

            for path in todo:
                for statement in statements(path.read_text()):
                    cursor.execute(statement)
                cursor.execute(
                    "INSERT INTO schema_migrations (filename, checksum, applied_at) "
                    "VALUES (%s, %s, UNIX_TIMESTAMP())",
                    (path.name, hashlib.sha256(path.read_bytes()).hexdigest()))
                connection.commit()
                print(f"applied {path.name}")
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
