"""
Circuit-breaker for Garmin auth failures.

State stored at gs://{bucket}/garmin/auth-circuit-breaker.json
  {"tripped_at": "2026-06-20T20:00:00Z", "consecutive_failures": 3}

Once tripped, all runs are skipped for BACKOFF_HOURS. This prevents
thousands of failed login attempts hammering Garmin's SSO and causing
account-level rate limits or bans.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone, timedelta

LOGGER = logging.getLogger(__name__)

BACKOFF_HOURS = 6
_GCS_OBJECT = "garmin/auth-circuit-breaker.json"

_AUTH_FAILURE_MARKERS = (
    "GarminConnectAuthenticationError",
    "Not authenticated",
    "Too Many Requests",
    "401",
    "SSO authentication failed",
)


def _bucket() -> str:
    uri = os.environ.get("TOKEN_CACHE_GCS_URI", "")
    # gs://bucket-name/path → bucket-name
    if uri.startswith("gs://"):
        return uri[5:].split("/")[0]
    return ""


def _get_client():
    from google.cloud import storage
    return storage.Client()


def _load_state() -> dict:
    bucket = _bucket()
    if not bucket:
        return {}
    try:
        blob = _get_client().bucket(bucket).blob(_GCS_OBJECT)
        if blob.exists():
            return json.loads(blob.download_as_text())
    except Exception as exc:
        LOGGER.warning("Circuit-breaker: could not load state: %s", exc)
    return {}


def _save_state(state: dict) -> None:
    bucket = _bucket()
    if not bucket:
        return
    try:
        blob = _get_client().bucket(bucket).blob(_GCS_OBJECT)
        blob.upload_from_string(json.dumps(state), content_type="application/json")
    except Exception as exc:
        LOGGER.warning("Circuit-breaker: could not save state: %s", exc)


def is_open() -> bool:
    """Return True if the circuit is open (skip this run)."""
    state = _load_state()
    tripped_at = state.get("tripped_at")
    if not tripped_at:
        return False
    try:
        tripped = datetime.fromisoformat(tripped_at)
        if tripped.tzinfo is None:
            tripped = tripped.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - tripped
        if age < timedelta(hours=BACKOFF_HOURS):
            hours_left = BACKOFF_HOURS - age.total_seconds() / 3600
            LOGGER.warning(
                "Circuit-breaker OPEN — Garmin auth failed %d times. "
                "Skipping run. Will retry in %.1fh.",
                state.get("consecutive_failures", 1),
                hours_left,
            )
            return True
        LOGGER.info("Circuit-breaker backoff elapsed — attempting Garmin auth again")
    except Exception as exc:
        LOGGER.warning("Circuit-breaker: bad tripped_at value: %s", exc)
    return False


def record_failure() -> None:
    """Call when Garmin auth fails. Trips the circuit-breaker."""
    state = _load_state()
    state["tripped_at"] = datetime.now(timezone.utc).isoformat()
    state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1
    _save_state(state)
    LOGGER.warning(
        "Circuit-breaker TRIPPED (failure #%d). Garmin runs paused for %dh.",
        state["consecutive_failures"],
        BACKOFF_HOURS,
    )


def record_success() -> None:
    """Call when data is collected successfully. Resets the circuit-breaker."""
    state = _load_state()
    if state:
        _save_state({})
        LOGGER.info("Circuit-breaker reset — Garmin auth succeeded")


def contains_auth_failure(text: str) -> bool:
    """Return True if subprocess output indicates a Garmin auth error."""
    return any(marker in text for marker in _AUTH_FAILURE_MARKERS)
