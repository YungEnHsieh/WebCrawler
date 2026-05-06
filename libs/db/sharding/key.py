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


SPLIT_TABLE = "shard_split_subdomain"


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


def load_split_subdomains(conn) -> set[str]:
    """Read whitelist from DB. conn is a psycopg2 connection."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT host FROM {SPLIT_TABLE}")
        return {r[0] for r in cur.fetchall()}


def load_sharding_config(
    ingest_path: str | Path,
    conn,
) -> tuple[dict[str, int], set[str]]:
    """Returns (overrides, split_subdomains). conn is a psycopg2 connection."""
    ingest = load_yaml(str(ingest_path)) if Path(ingest_path).exists() else {}
    overrides = dict((ingest.get("router") or {}).get("domain_overrides") or {})
    split_subdomains = load_split_subdomains(conn)
    return overrides, split_subdomains
