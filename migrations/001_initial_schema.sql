-- The process tier's own store (ADR 0012, amended — see src/reconciler/adapters/mysql.py).
--
-- This is the first schema this estate has authored: there is no DDL anywhere else in these
-- repos, and no migration framework. Numbered files applied in order by scripts/mysql_migrate.py
-- is the whole mechanism, and three tables do not justify more.
--
-- WHAT THIS IS NOT. It is not a second event log. Milestones still reach Neon through the
-- ingest gateway exactly as before (ADR 0012 rule 1: the reconciler acts, Aware observes), so
-- every row here also exists as a raw event there. That redundancy is deliberate and load-
-- bearing twice over: the Aware timeline and both dashboard boards keep working untouched, and
-- this database stays *reconstructible* — scripts/backfill_mysql.py rebuilds it from the event
-- log, which is what keeps a store with no backup story from being a new way to lose data.
--
-- COLUMN WIDTHS ARE MEASURED, not guessed. Against the live Neon data (3 cycles, 33 milestones,
-- 8 task events): cycle_key 19, process 17, user_id 4, event name 36, payload 1673 bytes,
-- upstream_id 16, subject 103, from_name 14, from_address 32, label 13, closed_via 9. Every
-- width below is that measurement with room to grow, not a round number someone liked.
--
-- TIME IS EPOCH SECONDS (BIGINT), not DATETIME, because that is what the domain model holds:
-- `core.Cycle.opened_at` and `core.Milestone.timestamp` are ints, and the Postgres reader
-- already converts with EXTRACT(EPOCH ...). Storing DATETIME would add a conversion on both
-- sides and a timezone question that the int does not have. Event time, never ingest time —
-- a satisfied `await` carries its EVIDENCE's timestamp (see core.EVIDENCE_TIME_KEY), so these
-- values are deliberately not monotonic with insertion order.
--
-- IDENTIFIERS COLLATE BINARY. The server default is utf8mb4_0900_ai_ci, which is
-- case- and accent-insensitive — fine for prose, wrong for a key. Two Gmail message ids
-- differing only in case are different messages, and a case-insensitive PRIMARY KEY would
-- silently merge them into one task. Prose columns keep the default collation.

CREATE TABLE cycles (
    -- `dh_invoice_2026_004` — the identity, minted from the definition's `cycle_key` template.
    -- Globally unique across processes by construction (the template is process-prefixed),
    -- which is why `milestones` can be looked up by cycle_key alone.
    cycle_key    VARCHAR(64)  NOT NULL COLLATE utf8mb4_bin,
    process      VARCHAR(64)  NOT NULL COLLATE utf8mb4_bin,
    -- Aware's entity key. The cycle NEVER goes here — keying state by cycle would fragment one
    -- person across as many buckets as they have ever had cycles (ADR 0012).
    user_id      VARCHAR(64)  NOT NULL COLLATE utf8mb4_bin,
    opened_at    BIGINT       NOT NULL,
    -- Whatever `cycle_opened` carried: invoice_number, worked_period, and whatever a future
    -- process opens with. A column per key would make `opens:` a schema change, which is
    -- exactly the definitions-as-data rule this tier is built on.
    context      JSON         NOT NULL,

    PRIMARY KEY (cycle_key),
    -- `cycles()` filters on process and orders by opened_at; this serves both without a sort.
    KEY cycles_process_opened (process, opened_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE milestones (
    cycle_key    VARCHAR(64)  NOT NULL COLLATE utf8mb4_bin,
    -- The stage name alone (`data_approved`), NOT the emitted event name
    -- (`dreamhost_invoice_data_approved`). The Postgres reader strips the process prefix in
    -- Python on every read; storing it stripped moves that to write time, where it happens once.
    stage        VARCHAR(64)  NOT NULL COLLATE utf8mb4_bin,
    occurred_at  BIGINT       NOT NULL,
    payload      JSON         NOT NULL,

    -- THE PRIMARY KEY IS THE LAST-WRITE-WINS RULE. `NeonMilestones.milestones` implements it in
    -- Python, overwriting as it builds a dict, because a retry that got through twice must not
    -- produce two conflicting views of one stage. Here an upsert against this key *is* that
    -- rule, enforced by the server rather than by every reader remembering to.
    PRIMARY KEY (cycle_key, stage),

    -- A milestone for a cycle that does not exist is corrupt, not merely odd, and the tier's
    -- house rule is to fail loudly rather than carry on looking like progress. Writes are
    -- cycle-first by construction (`cycle_opened` is the genesis stage), so this constrains
    -- nothing a correct run does. CASCADE so a voided-and-purged cycle leaves no orphans.
    CONSTRAINT milestones_cycle FOREIGN KEY (cycle_key)
        REFERENCES cycles (cycle_key) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tasks (
    -- The Gmail message id — the join key the two task events share, and the task's identity.
    upstream_id   VARCHAR(64)  NOT NULL COLLATE utf8mb4_bin,
    user_id       VARCHAR(64)  NOT NULL COLLATE utf8mb4_bin,
    label         VARCHAR(128) NOT NULL COLLATE utf8mb4_bin,

    -- Prose, so default collation. TEXT rather than VARCHAR for the subject because 103 is
    -- what the current six happen to measure, not a property of email.
    subject       TEXT         NULL,
    from_name     VARCHAR(255) NULL,
    from_address  VARCHAR(320) NULL,   -- RFC 5321's maximum path length

    opened_at     BIGINT       NOT NULL,
    -- NULL IS THE WHOLE ANTI-JOIN. In Postgres "open" is a latest-open-vs-latest-close
    -- comparison across two CTEs; here it is `closed_at IS NULL`.
    --
    -- REOPENING MUST KEEP WORKING, and it is the reason the Postgres version compares
    -- timestamps instead of testing for the absence of a close: re-labelling a mail you
    -- finished last month is a legitimate way to reopen it, and an `IS NULL` anti-join over
    -- two append-only event streams would refuse forever. On this schema the equivalent is a
    -- write — clear `closed_at` and bump `opened_at` — which is both expressible and
    -- undoable. Do not add a NOT NULL or a "closed tasks" table that makes it unreachable.
    closed_at     BIGINT       NULL,
    closed_via    VARCHAR(32)  NULL,

    PRIMARY KEY (upstream_id),
    -- Exactly the open-tasks predicate: user_id = ? AND label = ? AND closed_at IS NULL.
    -- closed_at trails so the index serves the first two columns even when it is not NULL.
    KEY tasks_open (user_id, label, closed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
