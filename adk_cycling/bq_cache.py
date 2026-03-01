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

_TTL_SECONDS: int = int(os.getenv("BQ_CACHE_TTL", "14400"))  # default 4 hours

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
