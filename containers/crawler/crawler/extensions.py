from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Deque

import tldextract
from scrapy import signals
from scrapy.exceptions import NotConfigured
from twisted.internet.task import LoopingCall

from libs.ipc.jsonio import append_jsonl


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class DomainSampleWindow:
    completions: Deque[tuple[float, float | None]] = field(default_factory=deque)
    inflight: int = 0
    last_activity: float = field(default_factory=monotonic)


class DomainQpsLoggerExtension:
    def __init__(
        self,
        metrics_dir_template: str,
        interval_seconds: float,
        window_seconds: float,
    ) -> None:
        self.metrics_dir_template = metrics_dir_template
        self.interval_seconds = interval_seconds
        self.window_seconds = window_seconds
        self.output_path: Path | None = None
        self.loop: LoopingCall | None = None
        self.states: dict[str, DomainSampleWindow] = {}
        self.crawler_id = 0

    @classmethod
    def from_crawler(cls, crawler):
        enabled = crawler.settings.getbool("DOMAIN_QPS_LOG_ENABLED", False)
        if not enabled:
            raise NotConfigured

        interval_seconds = float(crawler.settings.getfloat("DOMAIN_QPS_LOG_INTERVAL", 1.0))
        window_seconds = float(
            crawler.settings.getfloat("DOMAIN_QPS_WINDOW_SECONDS", 5.0)
        )
        metrics_dir_template = str(crawler.settings.get("METRICS_DIR_TEMPLATE"))

        if interval_seconds <= 0:
            raise NotConfigured("DOMAIN_QPS_LOG_INTERVAL must be > 0")
        if window_seconds <= 0:
            raise NotConfigured("DOMAIN_QPS_WINDOW_SECONDS must be > 0")

        ext = cls(metrics_dir_template, interval_seconds, window_seconds)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(
            ext.request_reached_downloader,
            signal=signals.request_reached_downloader,
        )
        crawler.signals.connect(
            ext.request_left_downloader,
            signal=signals.request_left_downloader,
        )
        return ext

    def spider_opened(self, spider) -> None:
        self.crawler_id = int(getattr(spider, "crawler_id", 0))
        metrics_dir = Path(self.metrics_dir_template.format(id=self.crawler_id))
        metrics_dir.mkdir(parents=True, exist_ok=True)
        self.output_path = metrics_dir / "domain_qps.jsonl"
        self.loop = LoopingCall(self.flush_snapshot)
        self.loop.start(self.interval_seconds, now=False)

    def spider_closed(self, spider) -> None:
        if self.loop is not None and self.loop.running:
            self.loop.stop()
        self.flush_snapshot()

    def request_reached_downloader(self, request, spider) -> None:
        now = monotonic()
        domain = self._extract_domain(request.url)
        request.meta["_domain_qps_domain"] = domain
        request.meta["_domain_qps_start"] = now

        state = self.states.setdefault(domain, DomainSampleWindow())
        state.inflight += 1
        state.last_activity = now

    def request_left_downloader(self, request, spider) -> None:
        now = monotonic()
        domain = str(request.meta.get("_domain_qps_domain") or self._extract_domain(request.url))
        start = request.meta.get("_domain_qps_start")
        latency_seconds = None
        if isinstance(start, (int, float)):
            latency_seconds = max(0.0, now - float(start))

        state = self.states.setdefault(domain, DomainSampleWindow())
        if state.inflight > 0:
            state.inflight -= 1
        state.completions.append((now, latency_seconds))
        state.last_activity = now

    def flush_snapshot(self) -> None:
        if self.output_path is None:
            return

        now = monotonic()
        sample_at = _now_iso()
        lines: list[dict[str, object]] = []

        for domain, state in list(self.states.items()):
            self._prune_state(state, now)

            completed_in_window = len(state.completions)
            latencies_ms = [
                latency * 1000.0
                for _, latency in state.completions
                if latency is not None
            ]

            if completed_in_window == 0 and state.inflight == 0:
                if now - state.last_activity > self.window_seconds:
                    del self.states[domain]
                continue

            lines.append(
                {
                    "sample_at": sample_at,
                    "crawler_id": self.crawler_id,
                    "domain": domain,
                    "window_seconds": self.window_seconds,
                    "interval_seconds": self.interval_seconds,
                    "completed_in_window": completed_in_window,
                    "qps": round(completed_in_window / self.window_seconds, 6),
                    "inflight_requests": state.inflight,
                    "avg_latency_ms": round(sum(latencies_ms) / len(latencies_ms), 3)
                    if latencies_ms
                    else None,
                }
            )

        for rec in lines:
            append_jsonl(self.output_path, rec)

    def _prune_state(self, state: DomainSampleWindow, now: float) -> None:
        cutoff = now - self.window_seconds
        while state.completions and state.completions[0][0] < cutoff:
            state.completions.popleft()

    def _extract_domain(self, url: str) -> str:
        extracted = tldextract.extract(url)
        domain = ".".join([p for p in [extracted.domain, extracted.suffix] if p])
        return domain or extracted.fqdn or url
