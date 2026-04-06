from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text


DEFAULT_DSN = "postgresql+psycopg2://crawler:crawler@localhost:5432/crawlerdb"
DEFAULT_NUM_SHARDS = 256


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Repair url_state_current shard rows with NULL should_crawl."
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL", DEFAULT_DSN),
        help="SQLAlchemy PostgreSQL DSN. Defaults to DATABASE_URL or local crawlerdb.",
    )
    parser.add_argument(
        "--num-shards",
        type=int,
        default=DEFAULT_NUM_SHARDS,
        help="Number of shard tables to inspect. Default: 256.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    engine = create_engine(args.dsn, future=True)

    updated = 0
    with engine.begin() as conn:
        for shard_id in range(args.num_shards):
            table = f"url_state_current_{shard_id:03d}"
            result = conn.execute(
                text(f"""
                UPDATE {table}
                SET should_crawl = TRUE
                WHERE should_crawl IS NULL
                """)
            )
            updated += result.rowcount or 0

    print(f"Repaired should_crawl flags: updated={updated}")


if __name__ == "__main__":
    main()
