"""
GCS-backed training-plan store with in-memory cache.

Mirrors profile.py's exact pattern: a single JSON document (single-user app,
no per-email path), short-TTL cache, deep-merge-on-load so new default keys
survive old saved blobs. Falls back to defaults and in-memory-only if
GCS_PROFILE_BUCKET is unset.

The plan itself lives here; the *read-only* progress computation (matching
Garmin activities against planned sessions, ATL/CTL trajectory) lives in
plan_progress.py, which loads/saves through this module.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

LOGGER = logging.getLogger(__name__)

_GCS_BUCKET = os.environ.get("GCS_PROFILE_BUCKET", "")
_GCS_OBJECT = "cycling-coach/training_plan.json"
_CACHE_TTL_S = 30  # seconds — short so UI/chat edits are seen quickly

# "No active plan" is the default state every existing user starts in —
# every reader must treat active=False as a normal, first-class state.
DEFAULTS: dict[str, Any] = {
    "active": False,
    "plan_id": None,
    "created_at": None,
    "goal": None,           # {"raw_text", "goal_type", "discipline", "target_date", "feasibility_note"}
    "baseline": None,       # {"captured_on", "ctl", "atl", "ftp_w", "weekly_tss_avg_4wk", "weekly_km_avg_4wk"}
    "phases": [],           # [{"name", "start_date", "end_date", "weeks", "load_start", "load_end"}]
    "sessions": [],         # see plan_generator.generate_plan for the per-session shape
    "progress": {
        "last_computed_at": None,
        "weeks": [],
        "overall_adherence_pct": None,
        "trajectory_status": None,  # "on_track" | "ahead" | "behind"
    },
    "milestone_state": {
        "initialized": False,
        "phase_hits": {},
    },
}

_cache: dict[str, Any] | None = None
_cache_ts: float = 0.0


def _get_blob():
    """Return a GCS blob handle, or None if GCS is not configured."""
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
    """Return the current training plan, merging with defaults for any missing keys."""
    global _cache, _cache_ts

    now = time.monotonic()
    if _cache is not None and (now - _cache_ts) < _CACHE_TTL_S:
        return dict(_cache)

    blob = _get_blob()
    if blob is not None:
        try:
            data = json.loads(blob.download_as_text())
            merged = {**DEFAULTS, **data}
            # Deep-merge nested dicts so new default sub-keys aren't dropped
            # when the saved blob predates them — same trick as profile.py.
            merged["progress"] = {**DEFAULTS["progress"], **(data.get("progress") or {})}
            merged["milestone_state"] = {**DEFAULTS["milestone_state"], **(data.get("milestone_state") or {})}
            _cache = merged
            _cache_ts = now
            return dict(_cache)
        except Exception as exc:
            LOGGER.warning("Could not load training plan from GCS (%s); using defaults", exc)

    _cache = dict(DEFAULTS)
    _cache_ts = now
    return dict(_cache)


def save(plan: dict[str, Any]) -> None:
    """Persist the training plan to GCS and update the local cache immediately."""
    global _cache, _cache_ts

    merged = {**DEFAULTS, **plan}

    blob = _get_blob()
    if blob is not None:
        try:
            blob.upload_from_string(
                json.dumps(merged, indent=2, ensure_ascii=False, default=str),
                content_type="application/json",
            )
            LOGGER.info("Training plan saved to gs://%s/%s", _GCS_BUCKET, _GCS_OBJECT)
        except Exception as exc:
            LOGGER.error("Could not save training plan to GCS: %s", exc)
            raise
    else:
        LOGGER.warning("GCS_PROFILE_BUCKET not set — training plan saved in-memory only")

    _cache = merged
    _cache_ts = time.monotonic()


def invalidate_cache() -> None:
    """Force the next load() call to re-fetch from GCS."""
    global _cache_ts
    _cache_ts = 0.0
