"""
Migration: add `has_json_ld BOOLEAN` to all url_state_current_{shard} and
url_state_history_{shard} tables (512 ALTERs total).

  has_json_ld BOOLEAN
    NULL  unknown (failed fetch / non-HTML / pre-migration row)
    TRUE  page contained at least one <script type="application/ld+json">
    FALSE successful HTML fetch with no JSON-LD block

PG 11+ handles ADD COLUMN without a volatile DEFAULT as metadata-only,
so this does not rewrite existing table data.

Usage:
    uv run scripts/migrate_add_has_json_ld.py [--dry-run]
"""

import argparse
import logging

import psycopg2

from constants import NUM_SHARDS, CRAWLERDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Add has_json_ld column to url_state shard tables"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print SQL without executing"
    )
    args = parser.parse_args()

    conn = psycopg2.connect(**CRAWLERDB)
    cur = conn.cursor()

    prefixes = ("url_state_current", "url_state_history")

    try:
        statements = 0
        for prefix in prefixes:
            for i in range(NUM_SHARDS):
                table = f"{prefix}_{i:03d}"
                sql = f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS has_json_ld BOOLEAN"
                statements += 1

                if args.dry_run:
                    log.info("[DRY-RUN] %s", sql)
                else:
                    cur.execute(sql)

        if not args.dry_run:
            conn.commit()
            log.info("Done: ran %d ALTER TABLE statements", statements)
        else:
            log.info("[DRY-RUN] Would run %d ALTER TABLE statements", statements)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
