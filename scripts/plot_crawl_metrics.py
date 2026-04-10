from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import html
import json
import os
from pathlib import Path
from typing import Iterator


DEFAULT_ROOT = os.environ.get("CRAWL_RESULT_ROOT", "data/ipc/crawl_result")
DEFAULT_OUTPUT = "tmp/crawl_metrics.html"
PALETTE = [
    "#0f766e",
    "#dc2626",
    "#2563eb",
    "#ca8a04",
    "#7c3aed",
    "#ea580c",
    "#0891b2",
    "#65a30d",
    "#be185d",
    "#4f46e5",
]


@dataclass(frozen=True)
class DomainSeries:
    domain: str
    counts: list[int]


def parse_ts(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate offline HTML charts for crawl volume and per-domain QPS."
    )
    parser.add_argument(
        "--root",
        default=DEFAULT_ROOT,
        help="Root crawl_result directory. Default: CRAWL_RESULT_ROOT or data/ipc/crawl_result",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Output HTML path. Default: tmp/crawl_metrics.html",
    )
    parser.add_argument(
        "--bucket-seconds",
        type=int,
        default=60,
        help="Bucket size for time-series aggregation. Default: 60.",
    )
    parser.add_argument(
        "--top-domains",
        type=int,
        default=8,
        help="If --domain is omitted, chart the top N domains by volume. Default: 8.",
    )
    parser.add_argument(
        "--domain",
        action="append",
        default=[],
        help="Specific domain to chart. Repeatable.",
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
    if args.bucket_seconds <= 0:
        raise SystemExit("--bucket-seconds must be > 0")
    return since, until


def floor_to_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


def floor_to_bucket(ts: datetime, bucket_seconds: int) -> datetime:
    ts = ts.astimezone(timezone.utc)
    epoch = int(ts.timestamp())
    bucket_start = epoch - (epoch % bucket_seconds)
    return datetime.fromtimestamp(bucket_start, tz=timezone.utc)


def iter_candidate_paths(root: Path, since: datetime, until: datetime) -> Iterator[Path]:
    crawler_dirs = [
        entry.path
        for entry in os.scandir(root)
        if entry.is_dir() and entry.name.startswith("crawler_")
    ]

    current = floor_to_minute(since)
    while current < until:
        ymd = current.strftime("%Y%m%d")
        hm = current.strftime("%H%M")
        for crawler_dir in crawler_dirs:
            minute_dir = Path(crawler_dir) / ymd / hm
            if not minute_dir.is_dir():
                continue
            for entry in os.scandir(minute_dir):
                if entry.is_file() and entry.name.endswith(".jsonl"):
                    yield Path(entry.path)
        current += timedelta(minutes=1)


def iter_jsonl_records(path: Path) -> Iterator[dict]:
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                print(
                    f"warning: skipped malformed JSONL line at {path}:{line_no}",
                    flush=True,
                )
                continue
            if not isinstance(rec, dict):
                continue
            yield rec


def iter_buckets(since: datetime, until: datetime, bucket_seconds: int) -> list[datetime]:
    buckets: list[datetime] = []
    current = floor_to_bucket(since, bucket_seconds)
    while current < until:
        buckets.append(current)
        current += timedelta(seconds=bucket_seconds)
    return buckets


def aggregate(
    root: Path,
    since: datetime,
    until: datetime,
    bucket_seconds: int,
) -> tuple[list[datetime], Counter[datetime], dict[str, Counter[datetime]], Counter[str]]:
    buckets = iter_buckets(since, until, bucket_seconds)
    total_counts: Counter[datetime] = Counter()
    domain_counts: dict[str, Counter[datetime]] = defaultdict(Counter)
    domain_totals: Counter[str] = Counter()

    for path in iter_candidate_paths(root, since, until):
        for rec in iter_jsonl_records(path):
            fetched_at = rec.get("fetched_at")
            if not fetched_at:
                continue
            ts = parse_ts(fetched_at)
            if ts < since or ts >= until:
                continue

            bucket = floor_to_bucket(ts, bucket_seconds)
            domain = str(rec.get("domain") or "(unknown)")

            total_counts[bucket] += 1
            domain_counts[domain][bucket] += 1
            domain_totals[domain] += 1

    return buckets, total_counts, domain_counts, domain_totals


def choose_domains(args: argparse.Namespace, domain_totals: Counter[str]) -> list[str]:
    if args.domain:
        requested = []
        seen: set[str] = set()
        for domain in args.domain:
            if domain in seen:
                continue
            requested.append(domain)
            seen.add(domain)
        return requested

    return [domain for domain, _ in domain_totals.most_common(max(args.top_domains, 0))]


def bucket_counts_series(buckets: list[datetime], counts: Counter[datetime]) -> list[int]:
    return [counts.get(bucket, 0) for bucket in buckets]


def domain_series_list(
    buckets: list[datetime],
    domains: list[str],
    domain_counts: dict[str, Counter[datetime]],
) -> list[DomainSeries]:
    return [
        DomainSeries(domain=domain, counts=bucket_counts_series(buckets, domain_counts.get(domain, Counter())))
        for domain in domains
    ]


def qps_values(counts: list[int], bucket_seconds: int) -> list[float]:
    return [count / bucket_seconds for count in counts]


def escape(value: str) -> str:
    return html.escape(value, quote=True)


def format_bucket_label(ts: datetime, include_date: bool) -> str:
    return ts.strftime("%m-%d %H:%M") if include_date else ts.strftime("%H:%M")


def tick_indexes(length: int, max_ticks: int = 8) -> list[int]:
    if length <= 0:
        return []
    if length <= max_ticks:
        return list(range(length))

    step = max(1, (length - 1) // (max_ticks - 1))
    indexes = list(range(0, length, step))
    if indexes[-1] != length - 1:
        indexes.append(length - 1)
    return indexes


def render_line_chart(
    title: str,
    subtitle: str,
    buckets: list[datetime],
    series: list[tuple[str, list[float], str]],
    y_label: str,
    include_date: bool,
    width: int = 1100,
    height: int = 360,
) -> str:
    if not buckets:
        return (
            f"<section class='card'><h2>{escape(title)}</h2>"
            f"<p class='muted'>{escape(subtitle)}</p><p>No data in the requested window.</p></section>"
        )

    left = 72
    right = 24
    top = 24
    bottom = 52
    plot_width = width - left - right
    plot_height = height - top - bottom

    max_value = max((max(values) for _, values, _ in series if values), default=0.0)
    if max_value <= 0:
        max_value = 1.0

    def x_pos(index: int) -> float:
        if len(buckets) == 1:
            return left + (plot_width / 2)
        return left + (index / (len(buckets) - 1)) * plot_width

    def y_pos(value: float) -> float:
        return top + plot_height - ((value / max_value) * plot_height)

    y_ticks = 5
    grid_lines = []
    tick_labels = []
    for tick in range(y_ticks + 1):
        value = max_value * tick / y_ticks
        y = y_pos(value)
        grid_lines.append(
            f"<line x1='{left}' y1='{y:.2f}' x2='{left + plot_width}' y2='{y:.2f}' class='chart-grid' />"
        )
        tick_labels.append(
            f"<text x='{left - 10}' y='{y + 4:.2f}' class='axis-text' text-anchor='end'>{value:.2f}</text>"
        )

    x_ticks = []
    for index in tick_indexes(len(buckets)):
        x = x_pos(index)
        label = format_bucket_label(buckets[index], include_date)
        x_ticks.append(
            f"<line x1='{x:.2f}' y1='{top + plot_height}' x2='{x:.2f}' y2='{top + plot_height + 6}' class='axis' />"
            f"<text x='{x:.2f}' y='{height - 12}' class='axis-text' text-anchor='middle'>{escape(label)}</text>"
        )

    polylines = []
    legend = []
    for name, values, color in series:
        points = " ".join(
            f"{x_pos(index):.2f},{y_pos(value):.2f}"
            for index, value in enumerate(values)
        )
        polylines.append(
            f"<polyline points='{points}' fill='none' stroke='{color}' stroke-width='2.5' stroke-linejoin='round' stroke-linecap='round' />"
        )
        legend.append(
            "<span class='legend-item'>"
            f"<span class='legend-swatch' style='background:{color}'></span>"
            f"{escape(name)}</span>"
        )

    return f"""
<section class="card">
  <div class="card-head">
    <div>
      <h2>{escape(title)}</h2>
      <p class="muted">{escape(subtitle)}</p>
    </div>
    <div class="legend">{''.join(legend)}</div>
  </div>
  <svg viewBox="0 0 {width} {height}" class="chart" role="img" aria-label="{escape(title)}">
    <rect x="{left}" y="{top}" width="{plot_width}" height="{plot_height}" class="plot-bg" />
    {''.join(grid_lines)}
    <line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}" class="axis" />
    <line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" class="axis" />
    {''.join(tick_labels)}
    {''.join(x_ticks)}
    {''.join(polylines)}
    <text x="{left - 50}" y="{top + (plot_height / 2):.2f}" class="axis-text" text-anchor="middle"
          transform="rotate(-90 {left - 50} {top + (plot_height / 2):.2f})">{escape(y_label)}</text>
  </svg>
</section>
"""


def render_summary_table(
    selected_domains: list[str],
    domain_totals: Counter[str],
    domain_series: list[DomainSeries],
    bucket_seconds: int,
) -> str:
    if not selected_domains:
        return ""

    rows = []
    series_map = {series.domain: series for series in domain_series}
    for domain in selected_domains:
        counts = series_map.get(domain, DomainSeries(domain, [])).counts
        peak_bucket = max(counts, default=0)
        avg_qps = (sum(counts) / len(counts) / bucket_seconds) if counts else 0.0
        peak_qps = peak_bucket / bucket_seconds
        rows.append(
            "<tr>"
            f"<td>{escape(domain)}</td>"
            f"<td>{domain_totals.get(domain, 0)}</td>"
            f"<td>{avg_qps:.4f}</td>"
            f"<td>{peak_qps:.4f}</td>"
            "</tr>"
        )

    return f"""
<section class="card">
  <h2>Selected Domains</h2>
  <table>
    <thead>
      <tr>
        <th>domain</th>
        <th>total_requests</th>
        <th>avg_qps</th>
        <th>peak_qps</th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows)}
    </tbody>
  </table>
</section>
"""


def build_html(
    args: argparse.Namespace,
    since: datetime,
    until: datetime,
    buckets: list[datetime],
    total_counts: Counter[datetime],
    domain_totals: Counter[str],
    selected_domains: list[str],
    domain_series: list[DomainSeries],
) -> str:
    include_date = since.date() != until.date()
    bucket_seconds = args.bucket_seconds
    total_series = bucket_counts_series(buckets, total_counts)
    total_qps = qps_values(total_series, bucket_seconds)
    duration_seconds = max(1.0, (until - since).total_seconds())
    total_requests = sum(total_series)
    avg_total_qps = total_requests / duration_seconds
    peak_total_qps = max(total_qps, default=0.0)

    overview = f"""
<section class="grid">
  <article class="stat-card">
    <h2>Total Requests</h2>
    <p>{total_requests}</p>
  </article>
  <article class="stat-card">
    <h2>Average Total QPS</h2>
    <p>{avg_total_qps:.4f}</p>
  </article>
  <article class="stat-card">
    <h2>Peak Total QPS</h2>
    <p>{peak_total_qps:.4f}</p>
  </article>
  <article class="stat-card">
    <h2>Domains In Window</h2>
    <p>{len(domain_totals)}</p>
  </article>
</section>
"""

    total_chart = render_line_chart(
        title="Crawl Volume Over Time",
        subtitle=f"Bucket size: {bucket_seconds}s. Value shown: requests per bucket.",
        buckets=buckets,
        series=[("total_requests", [float(v) for v in total_series], PALETTE[0])],
        y_label="requests",
        include_date=include_date,
    )

    qps_chart = render_line_chart(
        title="Domain QPS Over Time",
        subtitle=(
            f"Domains: {', '.join(selected_domains)}"
            if selected_domains
            else "No domains selected."
        ),
        buckets=buckets,
        series=[
            (series.domain, qps_values(series.counts, bucket_seconds), PALETTE[index % len(PALETTE)])
            for index, series in enumerate(domain_series)
        ],
        y_label="qps",
        include_date=include_date,
    )

    domain_cards = []
    for index, series in enumerate(domain_series):
        domain_cards.append(
            render_line_chart(
                title=f"Domain Detail: {series.domain}",
                subtitle=f"QPS by {bucket_seconds}s bucket",
                buckets=buckets,
                series=[(series.domain, qps_values(series.counts, bucket_seconds), PALETTE[index % len(PALETTE)])],
                y_label="qps",
                include_date=include_date,
                height=260,
            )
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Crawl Metrics</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f5f1e8;
      --card: #fffdf8;
      --ink: #172033;
      --muted: #5a6472;
      --line: #d5cfbf;
      --grid: #e9e1d1;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", serif;
      background:
        radial-gradient(circle at top left, rgba(15, 118, 110, 0.14), transparent 24rem),
        linear-gradient(180deg, #f7f3ea 0%, var(--bg) 100%);
      color: var(--ink);
    }}
    main {{
      max-width: 1240px;
      margin: 0 auto;
      padding: 32px 20px 56px;
    }}
    h1, h2 {{
      margin: 0;
      font-weight: 700;
    }}
    h1 {{
      font-size: 2.2rem;
      margin-bottom: 10px;
    }}
    h2 {{
      font-size: 1.2rem;
      margin-bottom: 6px;
    }}
    p {{
      margin: 0;
      line-height: 1.5;
    }}
    .muted {{
      color: var(--muted);
      font-size: 0.95rem;
    }}
    .header {{
      margin-bottom: 24px;
    }}
    .header-meta {{
      margin-top: 10px;
      color: var(--muted);
      font-size: 0.96rem;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
      margin-bottom: 18px;
    }}
    .stat-card, .card {{
      background: var(--card);
      border: 1px solid rgba(23, 32, 51, 0.08);
      border-radius: 16px;
      box-shadow: 0 12px 30px rgba(23, 32, 51, 0.06);
    }}
    .stat-card {{
      padding: 18px;
    }}
    .stat-card p {{
      font-size: 1.7rem;
      margin-top: 8px;
    }}
    .card {{
      padding: 18px;
      margin-top: 18px;
    }}
    .card-head {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
      margin-bottom: 10px;
      flex-wrap: wrap;
    }}
    .legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px 14px;
      color: var(--muted);
      font-size: 0.92rem;
      justify-content: flex-end;
    }}
    .legend-item {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }}
    .legend-swatch {{
      width: 12px;
      height: 12px;
      border-radius: 999px;
      display: inline-block;
    }}
    .chart {{
      width: 100%;
      height: auto;
      display: block;
    }}
    .plot-bg {{
      fill: #fffaf0;
    }}
    .chart-grid {{
      stroke: var(--grid);
      stroke-width: 1;
    }}
    .axis {{
      stroke: #8b95a6;
      stroke-width: 1;
    }}
    .axis-text {{
      fill: var(--muted);
      font-size: 12px;
      font-family: ui-monospace, "SFMono-Regular", Menlo, monospace;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 10px;
      font-family: ui-monospace, "SFMono-Regular", Menlo, monospace;
      font-size: 0.92rem;
    }}
    th, td {{
      text-align: left;
      padding: 10px 8px;
      border-bottom: 1px solid var(--grid);
    }}
    thead th {{
      color: var(--muted);
      font-weight: 600;
    }}
    @media (max-width: 700px) {{
      main {{ padding: 20px 12px 40px; }}
      h1 {{ font-size: 1.8rem; }}
      .card, .stat-card {{ padding: 14px; }}
    }}
  </style>
</head>
<body>
  <main>
    <header class="header">
      <h1>Crawl Metrics Report</h1>
      <p class="muted">Offline charts generated directly from crawl_result JSONL files.</p>
      <p class="header-meta">
        window_utc: {escape(since.isoformat())} -> {escape(until.isoformat())}<br />
        root: {escape(str(args.root))}<br />
        bucket_seconds: {bucket_seconds}<br />
        selected_domains: {escape(', '.join(selected_domains) if selected_domains else '(none)')}
      </p>
    </header>
    {overview}
    {total_chart}
    {qps_chart}
    {render_summary_table(selected_domains, domain_totals, domain_series, bucket_seconds)}
    {''.join(domain_cards)}
  </main>
</body>
</html>
"""


def main() -> None:
    args = parse_args()
    since, until = resolve_window(args)
    root = Path(args.root)
    if not root.exists():
        raise SystemExit(f"crawl_result root not found: {root}")

    buckets, total_counts, domain_counts, domain_totals = aggregate(
        root=root,
        since=since,
        until=until,
        bucket_seconds=args.bucket_seconds,
    )
    selected_domains = choose_domains(args, domain_totals)
    domain_series = domain_series_list(buckets, selected_domains, domain_counts)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        build_html(
            args=args,
            since=since,
            until=until,
            buckets=buckets,
            total_counts=total_counts,
            domain_totals=domain_totals,
            selected_domains=selected_domains,
            domain_series=domain_series,
        ),
        encoding="utf-8",
    )
    print(f"Wrote crawl metrics report to {out_path}")


if __name__ == "__main__":
    main()
