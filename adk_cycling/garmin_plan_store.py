"""
GCS-backed read-only cache of the Garmin Coach training plan, written by
pipeline/garmin_training_plan_daily.py. Nothing in adk_cycling ever writes this
blob — same cross-service, single-writer convention already used for
garmin/token_cache.tar.gz (pipeline writes it, this app never touches it).

Mirrors profile.py's load()-with-cache shape, but read-only (no save()) since
there's nothing here for the app to persist back.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Optional

LOGGER = logging.getLogger(__name__)

_GCS_BUCKET = os.environ.get("GCS_PROFILE_BUCKET", "")
_GCS_OBJECT = "cycling-coach/garmin_training_plan.json"
_CACHE_TTL_S = 300  # 5 min — this data changes at most once a day upstream

_cache: Optional[dict[str, Any]] = None
_cache_ts: float = 0.0


def load_raw() -> Optional[dict[str, Any]]:
    """Return {"fetched_at": <iso str>, "training_plan": {...} | None}, or None
    if the blob doesn't exist yet (pipeline hasn't run since this feature
    shipped) or GCS is unavailable. Callers must treat None as "nothing synced
    yet", not an error."""
    global _cache, _cache_ts

    now = time.monotonic()
    if _cache is not None and (now - _cache_ts) < _CACHE_TTL_S:
        return _cache

    if not _GCS_BUCKET:
        LOGGER.warning("GCS_PROFILE_BUCKET not set — no Garmin plan cache available")
        return None
    try:
        from google.cloud import storage
        client = storage.Client()
        blob = client.bucket(_GCS_BUCKET).blob(_GCS_OBJECT)
        if not blob.exists(client):
            return None
        data = json.loads(blob.download_as_text())
        _cache = data
        _cache_ts = now
        return data
    except Exception as exc:
        LOGGER.warning("Could not load Garmin plan cache (%s)", exc)
        return None


def invalidate_cache() -> None:
    """Force the next load_raw() call to re-fetch from GCS."""
    global _cache_ts
    _cache_ts = 0.0
