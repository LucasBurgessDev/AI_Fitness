"""In-process TTL cache for BigQuery query results.

Keyed on MD5 of the SQL string. Only successful (non-error) results are cached.
Thread-safe via a single lock. Uses only stdlib — no new dependencies.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import threading
import time
from typing import Optional

LOGGER = logging.getLogger(__name__)

_TTL_SECONDS: int = int(os.getenv("BQ_CACHE_TTL", "3600"))  # default 1 hour

_UNRECOGNIZED_NAME_RE = re.compile(r"Unrecognized name: (\w+)")

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


def patch_missing_column(sql: str, exc: Exception) -> Optional[str]:
    """If `exc` is BigQuery's "Unrecognized name: X", return `sql` with that column
    swapped for `NULL AS X` — or None if `exc` isn't that error, or X can't be
    found in `sql` to patch.

    New garmin_stats columns land in this code before BigQuery's ALLOW_FIELD_ADDITION
    adds them to the live table — that only happens once the pipeline next writes a
    row containing them (see pipeline/bigquery_writer.py). Until then, a query that
    selects one 400s. Callers should retry with the patched SQL in a loop (more than
    one column can be missing at once) instead of failing outright.

    Only safe for simple `SELECT col1, col2, ... FROM table` queries where each
    selected column name appears nowhere else in the SQL (no WHERE/JOIN/alias
    referencing it) — true for the fixed garmin_stats queries this is used for, not
    for arbitrary caller-supplied SQL.
    """
    match = _UNRECOGNIZED_NAME_RE.search(str(exc))
    if not match:
        return None
    col = match.group(1)
    patched, n = re.subn(rf"(?<![.\w]){re.escape(col)}\b", f"NULL AS {col}", sql, count=1)
    return patched if n else None


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
