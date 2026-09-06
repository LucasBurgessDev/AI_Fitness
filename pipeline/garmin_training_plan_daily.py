"""
Fetches the user's active Garmin Coach training plan and caches it to GCS so
adk_cycling can read it (gs://{bucket}/cycling-coach/garmin_training_plan.json)
without needing its own Garmin credentials — same cross-service, one-writer
convention already used for garmin/token_cache.tar.gz.

Rate-limit guarded, unlike garmin_activities_daily.py/garmin_stats_daily.py:
a Garmin Coach plan is slow-moving data (it typically only adjusts after a
workout syncs, not every 30 min), and a live 429 ("Mobile login returned 429 —
IP rate limited by Garmin") surfaced during this feature's own development
made clear that hitting these endpoints every hour for data that barely
changes isn't free. So before calling either Garmin API, this script checks
the existing cached blob's `fetched_at` timestamp and no-ops unless it's
stale by more than GARMIN_PLAN_SYNC_HOURS (default 24) — no Scheduler changes
needed, most hourly runs just skip straight through.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from garminconnect import Garmin

import garmin_plan_gcs

LOGGER = logging.getLogger(__name__)

TOKEN_DIR = os.getenv("GARMIN_TOKENSTORE", ".garminconnect")
SYNC_HOURS = float(os.getenv("GARMIN_PLAN_SYNC_HOURS", "24"))
OBJECT_PATH = "cycling-coach/garmin_training_plan.json"


def _bucket_name() -> str:
    """The bucket adk_cycling's GCS_PROFILE_BUCKET already points at. Falls back
    to the bucket parsed out of TOKEN_CACHE_GCS_URI if unset, since it's the same
    bucket in every deployed environment today."""
    bucket = os.getenv("GCS_PROFILE_BUCKET", "")
    if bucket:
        return bucket
    uri = os.environ["TOKEN_CACHE_GCS_URI"]  # gs://bucket/path
    return uri[len("gs://"):].split("/", 1)[0]


def _is_fresh(existing: dict | None) -> bool:
    if not existing or not existing.get("fetched_at"):
        return False
    try:
        fetched_at = datetime.fromisoformat(existing["fetched_at"].replace("Z", "+00:00"))
    except ValueError:
        return False
    age_hours = (datetime.now(timezone.utc) - fetched_at).total_seconds() / 3600
    return age_hours < SYNC_HOURS


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    bucket = _bucket_name()

    existing = None
    try:
        existing = garmin_plan_gcs.download_json(bucket, OBJECT_PATH)
    except Exception as exc:
        LOGGER.warning("Could not check existing Garmin plan cache (continuing): %s", exc)

    if _is_fresh(existing):
        LOGGER.info("Garmin training plan synced within the last %sh — skipping", SYNC_HOURS)
        return

    try:
        api = Garmin(os.getenv("GARMIN_EMAIL"), os.getenv("GARMIN_PASSWORD"))
        api.login(tokenstore=TOKEN_DIR)
    except Exception as exc:
        print(f"Login Error: {exc}")
        return

    training_plan = None
    try:
        plans = api.get_training_plans() or {}
        plan_list = plans.get("trainingPlanList") or []
        if plan_list:
            plan_id = plan_list[0]["trainingPlanId"]
            training_plan = api.get_adaptive_training_plan_by_id(plan_id)
            LOGGER.info("Fetched Garmin training plan: %s", plan_list[0].get("name"))
        else:
            LOGGER.info("No active Garmin training plan on this account.")
    except Exception as exc:
        LOGGER.warning("Could not fetch Garmin training plan (non-fatal): %s", exc)
        return

    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "training_plan": training_plan,
    }
    try:
        garmin_plan_gcs.upload_json(bucket, OBJECT_PATH, payload)
        LOGGER.info("Garmin training plan cached to gs://%s/%s", bucket, OBJECT_PATH)
    except Exception as exc:
        LOGGER.warning("Could not upload Garmin training plan (non-fatal): %s", exc)


if __name__ == "__main__":
    main()
