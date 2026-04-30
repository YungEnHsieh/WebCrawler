"""Single source of truth for domain -> shard_id routing.

Rules (in order):
  1. key = name if name is in `split_subdomains`, else the eTLD+1.
  2. `overrides[key]` if present.
  3. `md5(key) % num_shards`.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable

import tldextract

from libs.config.loader import load_yaml


def shard_key(name: str, split_subdomains: Iterable[str] | None = None) -> str:
    if not name:
        return "unknown"
    if split_subdomains and name in set(split_subdomains):
        return name
    ext = tldextract.extract(name)
    return ext.registered_domain or name


def compute_shard(
    name: str,
    num_shards: int,
    overrides: dict[str, int] | None = None,
    split_subdomains: Iterable[str] | None = None,
) -> int:
    key = shard_key(name, split_subdomains)
    if overrides and key in overrides:
        return int(overrides[key])
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(h, 16) % num_shards


def load_split_subdomains(path: str | Path) -> set[str]:
    p = Path(path)
    if not p.exists():
        return set()
    cfg = load_yaml(str(p))
    return set(cfg.get("split_subdomains") or [])


def load_sharding_config(
    ingest_path: str | Path,
    split_path: str | Path,
) -> tuple[dict[str, int], set[str]]:
    """Returns (overrides, split_subdomains)."""
    ingest = load_yaml(str(ingest_path)) if Path(ingest_path).exists() else {}
    overrides = dict((ingest.get("router") or {}).get("domain_overrides") or {})
    split_subdomains = load_split_subdomains(split_path)
    return overrides, split_subdomains
