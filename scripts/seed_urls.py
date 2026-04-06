from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import tldextract
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from containers.scheduler_ingest.ingestor.db_ops import IngestDB
from containers.scheduler_ingest.router.domain_resolver import DomainResolver
from containers.scheduler_ingest.router.routing import ShardRouter


DEFAULT_DSN = "postgresql+psycopg2://crawler:crawler@localhost:5432/crawlerdb"
DEFAULT_URL_FILE = "seeds/urls.txt"
DEFAULT_NUM_SHARDS = 256
DEFAULT_SHARDS_PER_INGESTOR = 16
DEFAULT_DOMAIN_OVERRIDES = {
    "wikipedia.org": 0,
    "x.com": 32,
    "youtube.com": 64,
    "reddit.com": 96,
    "instagram.com": 128,
    "espn.com": 160,
    "facebook.com": 192,
    "yahoo.com": 224,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed crawler URLs from a text file into PostgreSQL."
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL", DEFAULT_DSN),
        help="SQLAlchemy PostgreSQL DSN. Defaults to DATABASE_URL or local crawlerdb.",
    )
    parser.add_argument(
        "--file",
        default=DEFAULT_URL_FILE,
        help="Path to text file containing one URL per line.",
    )
    parser.add_argument(
        "--num-shards",
        type=int,
        default=DEFAULT_NUM_SHARDS,
        help="Total shard count. Default: 256.",
    )
    parser.add_argument(
        "--shards-per-ingestor",
        type=int,
        default=DEFAULT_SHARDS_PER_INGESTOR,
        help="Shard fanout for routing logic. Default: 16.",
    )
    return parser.parse_args()


def iter_seed_urls(path: Path):
    seen: set[str] = set()
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        value = raw.strip()
        if not value or value.startswith("#"):
            continue
        if value in seen:
            continue
        seen.add(value)
        yield line_no, value


def extract_domain(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("unsupported URL scheme")
    if not parsed.netloc:
        raise ValueError("missing hostname")

    extracted = tldextract.extract(url)
    domain = ".".join(part for part in (extracted.domain, extracted.suffix) if part)
    if not domain:
        raise ValueError("unable to extract registrable domain")
    return domain


def main() -> None:
    args = parse_args()
    if args.num_shards <= 0:
        raise SystemExit("--num-shards must be > 0")
    if args.shards_per_ingestor <= 0:
        raise SystemExit("--shards-per-ingestor must be > 0")

    url_file = Path(args.file)
    if not url_file.exists():
        raise SystemExit(f"seed file not found: {url_file}")

    engine = create_engine(args.dsn, future=True)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    ingestor = IngestDB(Session)
    sharder = ShardRouter(
        num_shards=args.num_shards,
        shards_per_ingestor=args.shards_per_ingestor,
        domain_overrides=DEFAULT_DOMAIN_OVERRIDES,
    )

    inserted = 0
    skipped = 0

    for line_no, url in iter_seed_urls(url_file):
        try:
            domain = extract_domain(url)
            shard_id = sharder.domain_to_shard(domain)

            with Session() as sess:
                resolver = DomainResolver(sess)
                with sess.begin():
                    domain_id, domain_score = resolver.ensure_and_get(domain, shard_id)

            is_new = ingestor.process_link(
                {
                    "url": url,
                    "shard_id": shard_id,
                    "domain_id": domain_id,
                    "domain_score": domain_score,
                }
            )
            if is_new:
                inserted += 1
            else:
                skipped += 1

        except Exception as exc:
            raise SystemExit(f"failed on line {line_no}: {url} ({exc})") from exc

    print(
        f"Seed complete from {url_file}: inserted={inserted}, skipped_existing={skipped}"
    )


if __name__ == "__main__":
    main()
