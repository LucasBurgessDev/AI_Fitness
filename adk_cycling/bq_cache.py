"""In-process TTL cache for BigQuery query results.

Keyed on MD5 of the SQL string. Only successful (non-error) results are cached.
Thread-safe via a single lock. Uses only stdlib — no new dependencies.
"""
from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
from typing import Optional

LOGGER = logging.getLogger(__name__)

_TTL_SECONDS: int = int(os.getenv("BQ_CACHE_TTL", "3600"))  # default 1 hour

_lock = threading.Lock()
_store: dict[str, tuple[str, float]] = {}  # key → (result, expires_at)


def _key(sql: str) -> str:
    return hashlib.md5(sql.encode()).hexdigest()


def get(sql: str) -> Optional[str]:
    """Return cached result for this SQL, or None if missing/expired."""
    k = _key(sql)
    with _lock:
        entry = _store.get(k)
        if entry is not None and entry[1] > time.monotonic():
            return entry[0]
    return None


def put(sql: str, result: str) -> None:
    """Store a successful query result."""
    k = _key(sql)
    with _lock:
        _store[k] = (result, time.monotonic() + _TTL_SECONDS)


def clear() -> None:
    """Evict all cached entries (e.g. after a Garmin sync is triggered)."""
    with _lock:
        count = len(_store)
        _store.clear()
    LOGGER.info("BQ cache cleared (%d entries evicted)", count)


def query(client, sql: str) -> list:
    """Run a BQ query with TTL caching. Returns list of plain dicts.

    On a cache hit the BQ client is not used at all. On a miss the result is
    serialised to JSON and stored so the next call is instant.
    """
    import json
    import decimal

    cached = get(sql)
    if cached is not None:
        LOGGER.debug("BQ cache hit")
        return json.loads(cached)

    LOGGER.debug("BQ cache miss, running query")
    raw = list(client.query(sql).result())

    def _to_json(v):
        if v is None or isinstance(v, (bool, int, float, str)):
            return v
        if isinstance(v, decimal.Decimal):
            return float(v)
        if hasattr(v, "isoformat"):
            return v.isoformat()
        return str(v)

    result = [{k: _to_json(v) for k, v in dict(row).items()} for row in raw]
    put(sql, json.dumps(result))
    return result
