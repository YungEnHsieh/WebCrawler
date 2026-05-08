"""
Migration: create shard_split_subdomain and seed it with the hosts that
were previously in shard_split.yaml. After this runs, the router and
migrate_* scripts read the whitelist from the DB; refresh_split_whitelist.py
writes there too.

Idempotent: CREATE TABLE IF NOT EXISTS, INSERT ... ON CONFLICT DO NOTHING.

Usage:
    uv run scripts/migrate_split_subdomains_to_db.py [--dry-run]
"""

import argparse
import logging

import psycopg2

from constants import CRAWLERDB
from libs.db.sharding.key import SPLIT_TABLE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


SEED_HOSTS = [
    "developers.facebook.com",
    "research.facebook.com",
    "tech.facebook.com",
    "work.facebook.com",
    "docs.x.com",
    "business.x.com",
    "shop.x.com",
    "privacy.x.com",
    "gdpr.x.com",
    "about.x.com",
    "privacycenter.instagram.com",
    "www.business.reddit.com",
    "business.linkedin.com",
    "economicgraph.linkedin.com",
    "learning.linkedin.com",
    "shop.bbc.com",
]

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SPLIT_TABLE} (
    host VARCHAR PRIMARY KEY,
    migrated_at TIMESTAMPTZ
)
"""
ALTER_SQL = f"ALTER TABLE {SPLIT_TABLE} ADD COLUMN IF NOT EXISTS migrated_at TIMESTAMPTZ"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print actions without executing")
    args = parser.parse_args()

    if args.dry_run:
        log.info("[DRY-RUN] %s", CREATE_SQL)
        log.info("[DRY-RUN] %s", ALTER_SQL)
        for h in SEED_HOSTS:
            log.info("[DRY-RUN] INSERT %s", h)
        return

    conn = psycopg2.connect(**CRAWLERDB)
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
            cur.execute(ALTER_SQL)
            cur.executemany(
                f"INSERT INTO {SPLIT_TABLE}(host) VALUES (%s) ON CONFLICT DO NOTHING",
                [(h,) for h in SEED_HOSTS],
            )
        conn.commit()
        log.info("seeded %d hosts into %s", len(SEED_HOSTS), SPLIT_TABLE)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
