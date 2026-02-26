"""
GCS-backed user profile store with in-memory cache.

Editable fields: ftp, weight_kg, height_cm, age, stats_date, goals, equipment.
Falls back to defaults and in-memory-only if GCS_PROFILE_BUCKET is unset.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

LOGGER = logging.getLogger(__name__)

_GCS_BUCKET = os.environ.get("GCS_PROFILE_BUCKET", "")
_GCS_OBJECT = "cycling-coach/profile.json"
_CACHE_TTL_S = 30  # seconds — short so UI edits are seen quickly

DEFAULTS: dict[str, Any] = {
    "ftp": 191,
    "weight_kg": 90.0,
    "height_cm": 178,
    "age": 31,
    "stats_date": "03/01/2026",
    "goals": (
        "- Complete The Cape Peninsula Loop on either 22, 23, or 24 April 2026 "
        "(depending on weather)\n"
        "- Lose weight to be more competitive\n"
        "- Lose upper body fat to fit into wedding suits!"
    ),
    "equipment": (
        "Zwift, Zwift Cog, Wahoo Kickr Core, Trek Domane, Triban RC500, "
        "Garmin Vivoactive 4, Ant+ Receiver"
    ),
    "reminders": {
        "morning_checkin_enabled": False,
        "morning_checkin_time": "07:30",
        "training_reminder_enabled": False,
        "training_reminder_time": "17:00",
    },
}

_cache: dict[str, Any] | None = None
_cache_ts: float = 0.0


def _get_blob():
    """Return (client, blob) or None if GCS is not configured."""
    if not _GCS_BUCKET:
        return None
    try:
        from google.cloud import storage
        client = storage.Client()
        return client.bucket(_GCS_BUCKET).blob(_GCS_OBJECT)
    except Exception as exc:
        LOGGER.warning("GCS unavailable: %s", exc)
        return None


def load() -> dict[str, Any]:
    """Return the current profile, merging with defaults for any missing keys."""
    global _cache, _cache_ts

    now = time.monotonic()
    if _cache is not None and (now - _cache_ts) < _CACHE_TTL_S:
        return dict(_cache)

    blob = _get_blob()
    if blob is not None:
        try:
            data = json.loads(blob.download_as_text())
            _cache = {**DEFAULTS, **data}
            _cache_ts = now
            return dict(_cache)
        except Exception as exc:
            LOGGER.warning("Could not load profile from GCS (%s); using defaults", exc)

    _cache = dict(DEFAULTS)
    _cache_ts = now
    return dict(_cache)


def save(profile: dict[str, Any]) -> None:
    """Persist the profile to GCS and update the local cache immediately."""
    global _cache, _cache_ts

    # Merge with defaults so no keys are lost
    merged = {**DEFAULTS, **profile}

    blob = _get_blob()
    if blob is not None:
        try:
            blob.upload_from_string(
                json.dumps(merged, indent=2, ensure_ascii=False),
                content_type="application/json",
            )
            LOGGER.info("Profile saved to gs://%s/%s", _GCS_BUCKET, _GCS_OBJECT)
        except Exception as exc:
            LOGGER.error("Could not save profile to GCS: %s", exc)
            raise
    else:
        LOGGER.warning("GCS_PROFILE_BUCKET not set — profile saved in-memory only")

    _cache = merged
    _cache_ts = time.monotonic()


def invalidate_cache() -> None:
    """Force the next load() call to re-fetch from GCS."""
    global _cache_ts
    _cache_ts = 0.0
