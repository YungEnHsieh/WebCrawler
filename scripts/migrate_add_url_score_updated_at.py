"""
Migration: add `url_score_updated_at` and Golden Discovery Ranker v1 indexes.

The production ranker writes its score into the existing `url_score` column.
This nullable timestamp is the only added DB field; it marks which rows have
already been scored by the active ranker. Existing rows remain NULL after the
migration, so they can be scored without a separate version column.

PG 11+ handles ADD COLUMN without a DEFAULT as metadata-only,
so this does not rewrite table data.

The ranker then repeatedly finds crawlable rows whose timestamp is still NULL.
Each current-shard table gets partial indexes for the background scorer lookup
and the Golden Discovery offerer selection path. Indexes are created
concurrently after the column transaction commits, because PostgreSQL does not
allow CREATE INDEX CONCURRENTLY inside a transaction block.

Usage:
    uv run scripts/migrate_add_url_score_updated_at.py [--dry-run]
"""

import argparse
import logging

import psycopg2

try:
    from scripts.constants import CRAWLERDB, NUM_SHARDS
except ModuleNotFoundError:
    from constants import CRAWLERDB, NUM_SHARDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CURRENT_PREFIX = "url_state_current"
HISTORY_PREFIX = "url_state_history"
STATE_PREFIXES = (CURRENT_PREFIX, HISTORY_PREFIX)


def iter_state_tables(prefixes=STATE_PREFIXES, num_shards=NUM_SHARDS):
    for prefix in prefixes:
        for shard_id in range(num_shards):
            yield f"{prefix}_{shard_id:03d}"


def add_url_score_updated_at_sql(table: str) -> str:
    return (
        f"ALTER TABLE {table} "
        "ADD COLUMN IF NOT EXISTS url_score_updated_at TIMESTAMPTZ"
    )


def golden_discovery_unscored_index_name(shard_id: int) -> str:
    return f"idx_url_state_current_{shard_id:03d}_golden_discovery_v1_unscored"


def golden_discovery_selection_index_name(shard_id: int) -> str:
    return f"idx_url_state_current_{shard_id:03d}_golden_discovery_v1_selection"


def create_golden_discovery_unscored_index_sql(shard_id: int) -> str:
    table = f"{CURRENT_PREFIX}_{shard_id:03d}"
    return (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        f"{golden_discovery_unscored_index_name(shard_id)} "
        f"ON {table} (first_seen ASC NULLS LAST) "
        "WHERE should_crawl = TRUE AND url_score_updated_at IS NULL"
    )


def create_golden_discovery_selection_index_sql(shard_id: int) -> str:
    table = f"{CURRENT_PREFIX}_{shard_id:03d}"
    return (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        f"{golden_discovery_selection_index_name(shard_id)} "
        f"ON {table} ("
        "domain_id, "
        "((CASE WHEN url_score_updated_at IS NULL THEN 1 ELSE 0 END)), "
        "url_score DESC NULLS LAST, "
        "domain_score DESC NULLS LAST, "
        "last_scheduled ASC NULLS FIRST, "
        "first_seen ASC"
        ") WHERE should_crawl = TRUE"
    )


def add_columns(conn, dry_run: bool) -> int:
    count = 0
    with conn.cursor() as cur:
        for table in iter_state_tables():
            sql = add_url_score_updated_at_sql(table)
            count += 1
            if dry_run:
                log.info("[DRY-RUN] %s", sql)
            else:
                cur.execute(sql)

    if not dry_run:
        conn.commit()
    return count


def create_golden_discovery_indexes(conn, dry_run: bool) -> int:
    count = 0
    previous_autocommit = conn.autocommit

    if not dry_run:
        conn.autocommit = True

    try:
        with conn.cursor() as cur:
            for shard_id in range(NUM_SHARDS):
                for sql in (
                    create_golden_discovery_unscored_index_sql(shard_id),
                    create_golden_discovery_selection_index_sql(shard_id),
                ):
                    count += 1
                    if dry_run:
                        log.info("[DRY-RUN] %s", sql)
                    else:
                        cur.execute(sql)
    finally:
        if not dry_run:
            conn.autocommit = previous_autocommit

    return count


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Add url_score_updated_at and Golden Discovery Ranker indexes "
            "to url_state shard tables"
        )
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print SQL without executing"
    )
    args = parser.parse_args()

    conn = psycopg2.connect(**CRAWLERDB)

    try:
        altered = add_columns(conn, args.dry_run)
        indexed = create_golden_discovery_indexes(conn, args.dry_run)

        if args.dry_run:
            log.info("[DRY-RUN] Would alter %d tables and create %d indexes", altered, indexed)
        else:
            log.info(
                "Done: altered %d tables and created %d Golden Discovery Ranker indexes",
                altered,
                indexed,
            )

    except Exception:
        if not conn.autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
