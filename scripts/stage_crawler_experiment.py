from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage a lightweight crawler experiment by writing URL queue files."
    )
    parser.add_argument(
        "--file",
        default="seeds/urls.txt",
        help="Path to text file containing one URL per line.",
    )
    parser.add_argument(
        "--experiment-root",
        required=True,
        help="Host path for the experiment root directory.",
    )
    parser.add_argument(
        "--crawler-id",
        type=int,
        default=0,
        help="Crawler ID to feed. Default: 0.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max URL count to queue.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="URLs per queue file. Default: 100.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete the existing experiment root before staging.",
    )
    return parser.parse_args()


def iter_urls(path: Path):
    seen: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        value = raw.strip()
        if not value or value.startswith("#"):
            continue
        if value in seen:
            continue
        seen.add(value)
        yield value


def write_queue_files(queue_dir: Path, urls: list[str], batch_size: int) -> int:
    queue_dir.mkdir(parents=True, exist_ok=True)

    batches = 0
    for start in range(0, len(urls), batch_size):
        payload = {"urls": urls[start : start + batch_size]}
        path = queue_dir / f"{batches:06d}.json"
        path.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        batches += 1
    return batches


def main() -> None:
    args = parse_args()
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be > 0")
    if args.limit is not None and args.limit <= 0:
        raise SystemExit("--limit must be > 0")
    if args.crawler_id < 0:
        raise SystemExit("--crawler-id must be >= 0")

    url_file = Path(args.file)
    if not url_file.exists():
        raise SystemExit(f"seed file not found: {url_file}")

    experiment_root = Path(args.experiment_root)
    if args.reset and experiment_root.exists():
        shutil.rmtree(experiment_root)

    urls = list(iter_urls(url_file))
    if args.limit is not None:
        urls = urls[: args.limit]
    if not urls:
        raise SystemExit("no URLs to queue")

    queue_dir = experiment_root / "url_queue" / f"crawler_{args.crawler_id:02d}"
    result_dir = experiment_root / "crawl_result" / f"crawler_{args.crawler_id:02d}"
    result_dir.mkdir(parents=True, exist_ok=True)

    batches = write_queue_files(queue_dir, urls, args.batch_size)

    print(f"experiment_root: {experiment_root}")
    print(f"url_file: {url_file}")
    print(f"crawler_id: {args.crawler_id}")
    print(f"queued_urls: {len(urls)}")
    print(f"queue_batches: {batches}")
    print(f"queue_dir: {queue_dir}")
    print(f"result_dir: {result_dir}")


if __name__ == "__main__":
    main()
