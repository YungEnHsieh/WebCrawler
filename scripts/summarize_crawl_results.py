from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path


DEFAULT_ROOT = "data/ipc/crawl_result"


def parse_ts(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize crawler result files for a time window."
    )
    parser.add_argument(
        "--root",
        default=DEFAULT_ROOT,
        help="Root crawl_result directory. Default: data/ipc/crawl_result",
    )
    parser.add_argument(
        "--minutes",
        type=int,
        default=None,
        help="Look back this many minutes from now.",
    )
    parser.add_argument(
        "--since",
        default=None,
        help="Inclusive UTC ISO timestamp, e.g. 2026-04-06T01:00:00+00:00",
    )
    parser.add_argument(
        "--until",
        default=None,
        help="Exclusive UTC ISO timestamp, e.g. 2026-04-06T02:00:00+00:00",
    )
    parser.add_argument(
        "--label",
        default="run",
        help="Label shown in the summary output.",
    )
    return parser.parse_args()


def resolve_window(args: argparse.Namespace) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    until = parse_ts(args.until) if args.until else now
    if args.since:
        since = parse_ts(args.since)
    elif args.minutes is not None:
        since = until - timedelta(minutes=args.minutes)
    else:
        raise SystemExit("provide either --minutes or --since")

    if since >= until:
        raise SystemExit("invalid window: since must be earlier than until")
    return since, until


@dataclass
class Summary:
    total: int = 0
    ok: int = 0
    fail: int = 0
    http_429: int = 0
    unique_urls: int = 0
    unique_domains: int = 0


def main() -> None:
    args = parse_args()
    since, until = resolve_window(args)
    root = Path(args.root)

    if not root.exists():
        raise SystemExit(f"crawl_result root not found: {root}")

    fail_reasons: Counter[str] = Counter()
    urls: set[str] = set()
    domains: set[str] = set()

    total = 0
    ok = 0
    fail = 0
    http_429 = 0

    for path in root.glob("crawler_*/*/*/*.jsonl"):
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                fetched_at = rec.get("fetched_at")
                if not fetched_at:
                    continue

                ts = parse_ts(fetched_at)
                if ts < since or ts >= until:
                    continue

                total += 1
                url = rec.get("url")
                domain = rec.get("domain")
                status = rec.get("status")
                fail_reason = rec.get("fail_reason")

                if url:
                    urls.add(str(url))
                if domain:
                    domains.add(str(domain))

                if status == "ok":
                    ok += 1
                else:
                    fail += 1
                    if fail_reason:
                        fail_reasons[str(fail_reason)] += 1
                        if str(fail_reason).startswith("HttpError 429"):
                            http_429 += 1

    duration_hours = (until - since).total_seconds() / 3600.0

    summary = Summary(
        total=total,
        ok=ok,
        fail=fail,
        http_429=http_429,
        unique_urls=len(urls),
        unique_domains=len(domains),
    )

    print(f"label: {args.label}")
    print(f"window_utc: {since.isoformat()} -> {until.isoformat()}")
    print(f"total_requests: {summary.total}")
    print(f"ok: {summary.ok}")
    print(f"fail: {summary.fail}")
    print(f"http_429: {summary.http_429}")
    print(f"unique_urls: {summary.unique_urls}")
    print(f"unique_domains: {summary.unique_domains}")
    print(f"req_per_hour: {summary.total / duration_hours:.2f}")
    print(f"ok_per_hour: {summary.ok / duration_hours:.2f}")
    print(f"429_per_hour: {summary.http_429 / duration_hours:.2f}")

    if fail_reasons:
        print("top_fail_reasons:")
        for reason, count in fail_reasons.most_common(10):
            print(f"  {reason}: {count}")


if __name__ == "__main__":
    main()
