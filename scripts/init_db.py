from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine

from libs.db import (
    Base,
    content_feature_current_table,
    content_feature_history_table,
    url_event_counter_table,
    url_state_current_table,
    url_state_history_table,
)


DEFAULT_DSN = "postgresql+psycopg2://crawler:crawler@localhost:5432/crawlerdb"
DEFAULT_NUM_SHARDS = 256


def materialize_sharded_tables(num_shards: int) -> None:
    for shard_id in range(num_shards):
        url_state_current_table(shard_id)
        url_state_history_table(shard_id)
        url_event_counter_table(shard_id)
        content_feature_current_table(shard_id)
        content_feature_history_table(shard_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialize PostgreSQL schema for the crawler project."
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
        help="Number of shard families to create. Default: 256.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.num_shards <= 0:
        raise SystemExit("--num-shards must be > 0")

    materialize_sharded_tables(args.num_shards)

    engine = create_engine(args.dsn, future=True)
    Base.metadata.create_all(bind=engine)

    total_tables = 4 + (args.num_shards * 5)
    print(
        f"Initialized schema on {args.dsn} with {args.num_shards} shards "
        f"({total_tables} tables)."
    )


if __name__ == "__main__":
    main()
