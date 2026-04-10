from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
import shutil


DEFAULT_CRAWL_ROOT = os.environ.get("CRAWL_RESULT_ROOT", "data/ipc/crawl_result")


@dataclass(frozen=True, order=True)
class BucketKey:
    date: str
    time: str


@dataclass(frozen=True)
class ProgressState:
    date: str
    time: str

    @property
    def key(self) -> BucketKey:
        return BucketKey(self.date, self.time)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Delete ingestor_* time buckets only after both ingestor and extractor "
            "progress have advanced past them. Dry-run by default."
        )
    )
    parser.add_argument(
        "--crawl-root",
        default=DEFAULT_CRAWL_ROOT,
        help="crawl_result root. Default: CRAWL_RESULT_ROOT or data/ipc/crawl_result",
    )
    parser.add_argument(
        "--progress-root",
        default=None,
        help="progress root. Default: sibling 'progress' next to crawl root",
    )
    parser.add_argument(
        "--ingestor-id",
        action="append",
        type=int,
        default=[],
        help="Specific ingestor id to clean. Repeatable. Default: all ingestor_* dirs.",
    )
    parser.add_argument(
        "--max-buckets",
        type=int,
        default=0,
        help="Limit deletions per run. 0 means no limit. Default: 0.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete data. Without this flag the script only prints a dry-run plan.",
    )
    return parser.parse_args()


def parse_progress(path: Path) -> ProgressState | None:
    if not path.exists():
        return None

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    date = raw.get("date")
    time = raw.get("time")
    if not isinstance(date, str) or not isinstance(time, str):
        return None
    if len(date) != 8 or len(time) != 4 or not date.isdigit() or not time.isdigit():
        return None
    return ProgressState(date=date, time=time)


def min_progress(left: ProgressState | None, right: ProgressState | None) -> ProgressState | None:
    if left is None or right is None:
        return None
    return left if left.key <= right.key else right


def bucket_dir_size(path: Path) -> int:
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for name in filenames:
            try:
                total += (Path(dirpath) / name).stat().st_size
            except FileNotFoundError:
                continue
    return total


def format_bytes(num_bytes: int) -> str:
    value = float(num_bytes)
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.1f}{unit}"
        value /= 1024.0
    return f"{num_bytes}B"


def remove_empty_parents(time_dir: Path, ingestor_dir: Path) -> None:
    date_dir = time_dir.parent
    try:
        date_dir.rmdir()
    except OSError:
        pass
    try:
        ingestor_dir.rmdir()
    except OSError:
        pass


def iter_bucket_dirs(ingestor_dir: Path) -> list[tuple[BucketKey, Path]]:
    out: list[tuple[BucketKey, Path]] = []
    if not ingestor_dir.exists():
        return out

    for date_dir in sorted(ingestor_dir.iterdir()):
        if not date_dir.is_dir():
            continue
        date = date_dir.name
        if len(date) != 8 or not date.isdigit():
            continue

        for time_dir in sorted(date_dir.iterdir()):
            if not time_dir.is_dir():
                continue
            time = time_dir.name
            if len(time) != 4 or not time.isdigit():
                continue
            out.append((BucketKey(date, time), time_dir))
    return out


def resolve_ingestor_ids(crawl_root: Path, requested_ids: list[int]) -> list[int]:
    if requested_ids:
        return sorted(set(requested_ids))

    ids: list[int] = []
    for entry in crawl_root.iterdir():
        if not entry.is_dir() or not entry.name.startswith("ingestor_"):
            continue
        suffix = entry.name.removeprefix("ingestor_")
        if suffix.isdigit():
            ids.append(int(suffix))
    return sorted(set(ids))


def main() -> None:
    args = parse_args()
    crawl_root = Path(args.crawl_root)
    if not crawl_root.exists():
        raise SystemExit(f"crawl root not found: {crawl_root}")

    progress_root = Path(args.progress_root) if args.progress_root else crawl_root.parent / "progress"
    if not progress_root.exists():
        raise SystemExit(f"progress root not found: {progress_root}")

    ingestor_ids = resolve_ingestor_ids(crawl_root, args.ingestor_id)
    if not ingestor_ids:
        raise SystemExit("no ingestor_* directories found")

    deleted_buckets = 0
    deleted_bytes = 0
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"[{mode}] crawl_root={crawl_root}")
    print(f"[{mode}] progress_root={progress_root}")

    for ingestor_id in ingestor_ids:
        ingestor_dir = crawl_root / f"ingestor_{ingestor_id:02d}"
        ingestor_progress = parse_progress(progress_root / "ingestor" / f"{ingestor_id:02d}.json")
        extractor_progress = parse_progress(progress_root / "extractor" / f"{ingestor_id:02d}.json")
        watermark = min_progress(ingestor_progress, extractor_progress)

        if watermark is None:
            print(
                f"[{mode}] ingestor_{ingestor_id:02d}: skipped "
                f"(missing or invalid ingestor/extractor progress)"
            )
            continue

        print(
            f"[{mode}] ingestor_{ingestor_id:02d}: "
            f"ingestor={ingestor_progress.date}/{ingestor_progress.time} "
            f"extractor={extractor_progress.date}/{extractor_progress.time} "
            f"watermark={watermark.date}/{watermark.time}"
        )

        for bucket_key, time_dir in iter_bucket_dirs(ingestor_dir):
            if args.max_buckets > 0 and deleted_buckets >= args.max_buckets:
                break
            if bucket_key > watermark.key:
                continue

            size_bytes = bucket_dir_size(time_dir)
            print(
                f"[{mode}] delete {time_dir} "
                f"(bucket={bucket_key.date}/{bucket_key.time}, size={format_bytes(size_bytes)})"
            )

            if args.apply:
                shutil.rmtree(time_dir)
                remove_empty_parents(time_dir, ingestor_dir)

            deleted_buckets += 1
            deleted_bytes += size_bytes

        if args.max_buckets > 0 and deleted_buckets >= args.max_buckets:
            break

    print(
        f"[{mode}] buckets={'deleted' if args.apply else 'planned'}={deleted_buckets} "
        f"bytes={deleted_bytes} ({format_bytes(deleted_bytes)})"
    )


if __name__ == "__main__":
    main()
