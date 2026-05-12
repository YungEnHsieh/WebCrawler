#!/usr/bin/env python3
"""
Refresh the shard_split_subdomain table from live fetch_ok counts.

For each eTLD+1 in scope, aggregate per-hostname fetch_ok counts and check
robots.txt for the Scrapy UA. Scope selection (in order of precedence):

  --domain D            scan only D
  --top-error-rate N    top-N eTLD+1s by fail_pct over the last --days
                        days, restricted to >= --min-attempts attempts
  (default)             keys of ingest.yaml::domain_overrides

The script only grows the whitelist:

  ADD     host has fetch_ok >= --min-fetch-ok and robots=ok and is not
          the apex; not currently whitelisted.
  REMOVE  host is whitelisted but robots.txt now blocks the Scrapy UA.
          Low fetch_ok alone never triggers removal, that stays a human
          decision via migrate_unsplit_subdomains.py.

Dry-run prints the proposed diff. --execute INSERTs / DELETEs against
shard_split_subdomain. domain_overrides in ingest.yaml is left alone,
that one stays human-curated.

    scripts/refresh_split_whitelist.py
    scripts/refresh_split_whitelist.py --domain facebook.com
    scripts/refresh_split_whitelist.py --top-error-rate 10
    scripts/refresh_split_whitelist.py --execute

After --execute, run scripts/migrate_shard_split.py --execute (with
scheduler_ingest paused) to move existing rows for the new entries.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.robotparser import RobotFileParser

import psycopg2

from constants import CRAWLERDB
from libs.config.loader import load_yaml
from libs.db.sharding.key import SPLIT_TABLE, load_split_subdomains

REPO = Path(__file__).resolve().parents[1]
INGEST_CFG = REPO / "containers/scheduler_ingest/config/ingest.yaml"

HOST_EXTRACT = "LOWER(SUBSTRING(url FROM '^https?://([^/:]+)'))"
ROBOTS_UA = "Scrapy"
ROBOTS_TIMEOUT = 5.0


def check_robots(host: str) -> str:
    """Return 'ok' / 'blocked' / 'err:<reason>'."""
    rp = RobotFileParser()
    rp.set_url(f"https://{host}/robots.txt")
    try:
        import socket
        socket.setdefaulttimeout(ROBOTS_TIMEOUT)
        rp.read()
    except Exception as e:
        return f"err:{type(e).__name__}"
    return "ok" if rp.can_fetch(ROBOTS_UA, f"https://{host}/") else "blocked"


def fetch_host_counts(cur, shard_id: int, domain_id: int) -> list[tuple[str, int]]:
    cur.execute(
        f"""
        SELECT {HOST_EXTRACT} AS host, COUNT(*)
        FROM url_state_current_{shard_id:03d}
        WHERE domain_id = %s AND last_fetch_ok IS NOT NULL
        GROUP BY 1
        """,
        (domain_id,),
    )
    return [(h, c) for h, c in cur.fetchall() if h]


def fetch_top_error_parents(cur, n: int, days: int, min_attempts: int) -> list[str]:
    cur.execute(
        """
        SELECT ds.domain
        FROM domain_stats_daily dsd
        JOIN domain_state ds USING (domain_id)
        WHERE dsd.event_date >= CURRENT_DATE - %s::int
        GROUP BY ds.domain
        HAVING SUM(dsd.num_fetch_ok + dsd.num_fetch_fail) >= %s
        ORDER BY SUM(dsd.num_fetch_fail)::float
                 / NULLIF(SUM(dsd.num_fetch_ok + dsd.num_fetch_fail), 0) DESC NULLS LAST,
                 SUM(dsd.num_fetch_fail) DESC
        LIMIT %s
        """,
        (days, min_attempts, n),
    )
    return [r[0] for r in cur.fetchall()]


def collect_counts(cur, parent: str) -> dict[str, int]:
    cur.execute(
        """
        SELECT shard_id, domain_id FROM domain_state
        WHERE domain = %s OR domain LIKE %s
        """,
        (parent, f"%.{parent}"),
    )
    counts: dict[str, int] = defaultdict(int)
    for shard_id, domain_id in cur.fetchall():
        for host, cnt in fetch_host_counts(cur, shard_id, domain_id):
            counts[host] += cnt
    return counts


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--domain", help="scan only this eTLD+1")
    p.add_argument("--top-error-rate", type=int, metavar="N",
                   help="scan top-N eTLD+1s by fail_pct over --days")
    p.add_argument("--days", type=int, default=7,
                   help="window for --top-error-rate (default 7)")
    p.add_argument("--min-attempts", type=int, default=10000,
                   help="--top-error-rate filter: min total attempts (default 10000)")
    p.add_argument("--min-fetch-ok", type=int, default=10000)
    p.add_argument("--execute", action="store_true",
                   help="apply INSERT/DELETE to shard_split_subdomain")
    args = p.parse_args()

    if args.domain and args.top_error_rate:
        p.error("--domain and --top-error-rate are mutually exclusive")

    ingest = load_yaml(str(INGEST_CFG))
    overrides = (ingest.get("router") or {}).get("domain_overrides") or {}

    counts: dict[str, int] = {}
    with psycopg2.connect(**CRAWLERDB) as conn:
        current = load_split_subdomains(conn)
        with conn.cursor() as cur:
            if args.domain:
                parents = [args.domain.lower()]
            elif args.top_error_rate:
                parents = fetch_top_error_parents(
                    cur, args.top_error_rate, args.days, args.min_attempts)
            else:
                parents = sorted(overrides.keys())

            print(f"scope: {len(parents)} parent(s): {', '.join(parents)}")
            print(f"min_fetch_ok: {args.min_fetch_ok:,}\n")

            parent_set = set(parents)
            for parent in parents:
                for host, cnt in collect_counts(cur, parent).items():
                    if host in parent_set:
                        continue  # apex
                    counts[host] = counts.get(host, 0) + cnt

        candidates = sorted(
            ((h, c) for h, c in counts.items() if c >= args.min_fetch_ok or h in current),
            key=lambda kv: kv[1], reverse=True,
        )

        robots: dict[str, str] = {}
        if candidates:
            with ThreadPoolExecutor(max_workers=8) as ex:
                for host, status in zip(
                    (h for h, _ in candidates),
                    ex.map(check_robots, (h for h, _ in candidates)),
                ):
                    robots[host] = status

        add, remove, keep = [], [], []
        for host, cnt in candidates:
            in_list = host in current
            rb = robots.get(host)
            if in_list and rb == "blocked":
                remove.append((host, cnt))
            elif in_list:
                keep.append((host, cnt))
            elif cnt >= args.min_fetch_ok and rb == "ok":
                add.append((host, cnt))

        def _print(label: str, sign: str, items: list[tuple[str, int]]) -> None:
            print(f"{label} ({len(items)}):")
            for host, cnt in items:
                print(f"  {sign} {host:55s} fetch_ok={cnt:>10,}  robots={robots.get(host, '?')}")
            print()

        _print("ADD", "+", add)
        _print("REMOVE", "-", remove)
        _print("KEEP", "=", keep)

        final = sorted((current | {h for h, _ in add}) - {h for h, _ in remove})

        if args.execute:
            if args.domain:
                print("refusing to --execute with --domain (partial scan); rerun without --domain")
                return
            with conn.cursor() as cur:
                if add:
                    cur.executemany(
                        f"INSERT INTO {SPLIT_TABLE}(host) VALUES (%s) ON CONFLICT DO NOTHING",
                        [(h,) for h, _ in add],
                    )
                if remove:
                    cur.executemany(
                        f"DELETE FROM {SPLIT_TABLE} WHERE host = %s",
                        [(h,) for h, _ in remove],
                    )
            conn.commit()
            print(f"updated {SPLIT_TABLE}: +{len(add)} / -{len(remove)} ({len(final)} entries total)")
            print("next: scripts/migrate_shard_split.py --execute (with scheduler_ingest paused)")
        else:
            print(f"dry-run. final list would be {len(final)} entries.")


if __name__ == "__main__":
    main()
