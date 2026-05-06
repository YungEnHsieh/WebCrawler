from __future__ import annotations

import math
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import numpy as np
from scipy import sparse

from .url_features import bucket_key, domain_of, path_depth, tld_of, tokenize_url


@dataclass(frozen=True)
class GoldenDiscoveryPriorMaps:
    domain_url_count: dict[str, int]
    domain_query_sum: dict[str, float]
    domain_row_sum: dict[str, float]
    domain_best_rank_score: dict[str, float]
    domain_discovered_rate: dict[str, float]
    domain_crawled_rate: dict[str, float]
    bucket_url_count: dict[str, int]
    bucket_query_sum: dict[str, float]
    parent_domain_count: dict[str, int]
    parent_url_child_count: dict[str, int] = field(default_factory=dict)
    graph_domain_discovery_events: dict[str, int] = field(default_factory=dict)
    graph_domain_first_parent_children: dict[str, int] = field(default_factory=dict)
    graph_domain_golden_child_events: dict[str, int] = field(default_factory=dict)
    graph_domain_golden_first_parent_children: dict[str, int] = field(default_factory=dict)
    graph_domain_parent_http_2xx_events: dict[str, int] = field(default_factory=dict)
    source_pending_url_count: dict[str, int] = field(default_factory=dict)
    source_unscored_url_count: dict[str, int] = field(default_factory=dict)
    source_best_url_score: dict[str, float] = field(default_factory=dict)
    source_best_full_url_score: dict[str, float] = field(default_factory=dict)
    source_domain_score: dict[str, float] = field(default_factory=dict)
    source_num_scheduled: dict[str, int] = field(default_factory=dict)
    source_num_fetch_ok: dict[str, int] = field(default_factory=dict)
    source_num_fetch_fail: dict[str, int] = field(default_factory=dict)


def _max_int(values: dict[str, int]) -> int:
    return max([1, *values.values()])


def _max_float(values: dict[str, float]) -> float:
    return max([1.0, *[float(v) for v in values.values()]])


def _scaled_count(value: float, max_value: float) -> float:
    return min(math.log1p(value) / math.log1p(max_value), 1.0)


def numeric_feature_matrix(urls: list[str], priors: GoldenDiscoveryPriorMaps) -> sparse.csr_matrix:
    """Numeric features matching the Golden Discovery Ranker v1 artifact contract.

    Keep this in production code instead of importing the training script: the
    scorer needs deterministic feature parity, not experiment dependencies.
    """
    rows: list[list[float]] = []
    max_domain_url = _max_int(priors.domain_url_count)
    max_domain_query = _max_float(priors.domain_query_sum)
    max_domain_row = _max_float(priors.domain_row_sum)
    max_bucket_url = _max_int(priors.bucket_url_count)
    max_bucket_query = _max_float(priors.bucket_query_sum)
    max_parent_domain = _max_int(priors.parent_domain_count)
    max_parent_url = _max_int(priors.parent_url_child_count)
    max_graph_events = _max_int(priors.graph_domain_discovery_events)
    max_graph_children = _max_int(priors.graph_domain_first_parent_children)
    max_graph_golden_events = _max_int(priors.graph_domain_golden_child_events)
    max_graph_golden_children = _max_int(priors.graph_domain_golden_first_parent_children)
    max_source_pending = _max_int(priors.source_pending_url_count)
    max_source_scheduled = _max_int(priors.source_num_scheduled)

    for url in urls:
        domain = domain_of(url)
        bucket = bucket_key(url)
        tokens = tokenize_url(url)
        token_count = len(tokens)
        digit_chars = sum(ch.isdigit() for ch in url)
        alpha_chars = sum(ch.isalpha() for ch in url)
        slash_count = url.count("/")
        dot_count = url.count(".")
        hyphen_count = url.count("-")
        underscore_count = url.count("_")
        query_count = url.count("?") + url.count("&")
        path = urlsplit(url).path or ""
        ext = path.rsplit(".", 1)[-1].lower() if "." in path.rsplit("/", 1)[-1] else ""
        ext_is_html = 1.0 if ext in {"html", "htm", "php", "asp", "aspx"} else 0.0
        ext_is_feed = 1.0 if ext in {"rss", "xml", "atom"} else 0.0
        ext_is_binary = 1.0 if ext in {"jpg", "jpeg", "png", "gif", "pdf", "zip", "mp4", "webp"} else 0.0

        graph_events = float(priors.graph_domain_discovery_events.get(domain, 0))
        graph_children = float(priors.graph_domain_first_parent_children.get(domain, 0))
        graph_golden_events = float(priors.graph_domain_golden_child_events.get(domain, 0))
        graph_golden_children = float(priors.graph_domain_golden_first_parent_children.get(domain, 0))
        graph_2xx = float(priors.graph_domain_parent_http_2xx_events.get(domain, 0))
        source_ok = float(priors.source_num_fetch_ok.get(domain, 0))
        source_fail = float(priors.source_num_fetch_fail.get(domain, 0))
        source_sched = float(priors.source_num_scheduled.get(domain, 0))

        rows.append(
            [
                min(math.log1p(len(url)) / 8.0, 1.0),
                min(path_depth(url) / 10.0, 1.0),
                min(token_count / 40.0, 1.0),
                min(digit_chars / max(len(url), 1), 1.0),
                min(alpha_chars / max(len(url), 1), 1.0),
                min(slash_count / 20.0, 1.0),
                min(dot_count / 10.0, 1.0),
                min(hyphen_count / 30.0, 1.0),
                min(underscore_count / 20.0, 1.0),
                min(query_count / 10.0, 1.0),
                1.0 if url.startswith("https://") else 0.0,
                ext_is_html,
                ext_is_feed,
                ext_is_binary,
                _scaled_count(float(priors.domain_url_count.get(domain, 0)), max_domain_url),
                _scaled_count(float(priors.domain_query_sum.get(domain, 0.0)), max_domain_query),
                _scaled_count(float(priors.domain_row_sum.get(domain, 0.0)), max_domain_row),
                float(priors.domain_best_rank_score.get(domain, 0.0)),
                float(priors.domain_discovered_rate.get(domain, 0.0)),
                float(priors.domain_crawled_rate.get(domain, 0.0)),
                _scaled_count(float(priors.bucket_url_count.get(bucket, 0)), max_bucket_url),
                _scaled_count(float(priors.bucket_query_sum.get(bucket, 0.0)), max_bucket_query),
                _scaled_count(float(priors.parent_domain_count.get(domain, 0)), max_parent_domain),
                1.0 if tld_of(domain) in {"com", "org", "net", "gov", "edu"} else 0.0,
                _scaled_count(float(priors.parent_url_child_count.get(url, 0)), max_parent_url),
                _scaled_count(graph_events, max_graph_events),
                _scaled_count(graph_children, max_graph_children),
                _scaled_count(graph_golden_events, max_graph_golden_events),
                _scaled_count(graph_golden_children, max_graph_golden_children),
                graph_golden_events / max(graph_events, 1.0),
                graph_golden_children / max(graph_children, 1.0),
                graph_2xx / max(graph_events, 1.0),
                _scaled_count(float(priors.source_pending_url_count.get(domain, 0)), max_source_pending),
                _scaled_count(float(priors.source_unscored_url_count.get(domain, 0)), max_source_pending),
                float(priors.source_best_url_score.get(domain, 0.0)),
                float(priors.source_best_full_url_score.get(domain, 0.0)),
                float(priors.source_domain_score.get(domain, 0.0)),
                _scaled_count(source_sched, max_source_scheduled),
                source_ok / max(source_ok + source_fail, 1.0),
                source_fail / max(source_ok + source_fail, 1.0),
            ]
        )
    return sparse.csr_matrix(np.asarray(rows, dtype=np.float32))


# Legacy artifact compatibility: existing joblib files serialize this dataclass
# under the training-time name `C2PriorMaps`.
C2PriorMaps = GoldenDiscoveryPriorMaps
