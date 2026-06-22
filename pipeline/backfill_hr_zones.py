#!/usr/bin/env python3
"""Re-fetch HR zone data from Garmin for cycling activities with null hr_zone_1_secs in BQ.

Usage (run from pipeline/ directory):
    BQ_PROJECT_ID=health-data-482722 GARTH_DIR=.garth python backfill_hr_zones.py

The script:
  1. Queries BQ for cycling activities with null hr_zone_1_secs (up to --limit rows).
  2. For each, calls get_activity_hr_in_timezones() and extract_hr_zones() from the
     activity detail summaryDTO.
  3. Issues a BQ UPDATE to patch zone columns in-place.

Rate-limited to one Garmin API call per second to avoid 429s.
"""
from __future__ import annotations

import argparse
import logging
import os
import time
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("backfill_hr_zones")

PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "")
GARTH_DIR  = os.environ.get("GARTH_DIR", ".garth")

CYCLING_TYPES = (
    "cycling", "road_cycling", "gravel_cycling", "mountain_biking",
    "indoor_cycling", "virtual_ride", "spinning",
)


def safe_float(v) -> Optional[float]:
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def extract_hr_zones_from_summary(detail: dict) -> list:
    zones = [None] * 5
    if not isinstance(detail, dict):
        return zones
    summary = detail.get("summaryDTO") or {}
    zone_arr = summary.get("hrTimeInZone", [])
    if isinstance(zone_arr, list) and zone_arr:
        for i, v in enumerate(zone_arr[:5]):
            zones[i] = safe_float(v)
        return zones
    for i in range(1, 6):
        v = summary.get(f"hrTimeInZone_{i}") or summary.get(f"zone{i}Time")
        if v is not None:
            zones[i - 1] = safe_float(v)
    return zones


def fetch_hr_zones_from_endpoint(api, activity_id: str) -> list:
    zones = [None] * 5
    fn = getattr(api, "get_activity_hr_in_timezones", None)
    if not callable(fn):
        return zones
    try:
        data = fn(activity_id)
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("timeInZoneList", [])
        else:
            return zones
        for item in items:
            z = int(item.get("zoneNumber", 0))
            if 1 <= z <= 5:
                zones[z - 1] = safe_float(item.get("secsInZone"))
    except Exception as e:
        LOGGER.warning("  hr_in_timezones error: %s", e)
    return zones


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=500,
                        help="Max activities to patch (default 500)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch zones but don't write to BQ")
    args = parser.parse_args()

    if not PROJECT_ID:
        raise SystemExit("BQ_PROJECT_ID env var is required.")

    from garminconnect import Garmin
    api = Garmin(os.environ.get("GARMIN_EMAIL", ""), os.environ.get("GARMIN_PASSWORD", ""))
    api.login(tokenstore=GARTH_DIR)
    LOGGER.info("Garmin login OK")

    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT_ID)

    types_sql = ", ".join(f"'{t}'" for t in CYCLING_TYPES)
    query = f"""
    SELECT activity_id
    FROM `{PROJECT_ID}.garmin.garmin_activities`
    WHERE activity_type IN ({types_sql})
      AND hr_zone_1_secs IS NULL
    GROUP BY activity_id
    ORDER BY MAX(date) DESC
    LIMIT {args.limit}
    """
    rows = list(bq.query(query).result())
    LOGGER.info("Found %d activities with null hr_zone_1_secs", len(rows))

    patched = skipped = 0
    for i, row in enumerate(rows):
        act_id = str(row["activity_id"])
        LOGGER.info("[%d/%d] activity_id=%s", i + 1, len(rows), act_id)

        # Try endpoint first (faster, no full detail needed)
        zones = fetch_hr_zones_from_endpoint(api, act_id)

        if not any(z for z in zones if z is not None and z > 0):
            # Fall back to summaryDTO from activity detail
            try:
                detail = api.get_activity_details(act_id)
                zones = extract_hr_zones_from_summary(detail)
            except Exception as e:
                LOGGER.warning("  get_activity_details error: %s", e)

        non_null = [z for z in zones if z is not None]
        if not non_null:
            LOGGER.info("  → no zone data found")
            skipped += 1
            time.sleep(1)
            continue

        LOGGER.info("  → zones: z1=%.0f z2=%.0f z3=%.0f z4=%.0f z5=%.0f",
                    zones[0] or 0, zones[1] or 0, zones[2] or 0,
                    zones[3] or 0, zones[4] or 0)

        if not args.dry_run:
            update_sql = f"""
            UPDATE `{PROJECT_ID}.garmin.garmin_activities`
            SET hr_zone_1_secs = @z1,
                hr_zone_2_secs = @z2,
                hr_zone_3_secs = @z3,
                hr_zone_4_secs = @z4,
                hr_zone_5_secs = @z5
            WHERE activity_id = @act_id
              AND hr_zone_1_secs IS NULL
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("z1", "FLOAT64", zones[0]),
                    bigquery.ScalarQueryParameter("z2", "FLOAT64", zones[1]),
                    bigquery.ScalarQueryParameter("z3", "FLOAT64", zones[2]),
                    bigquery.ScalarQueryParameter("z4", "FLOAT64", zones[3]),
                    bigquery.ScalarQueryParameter("z5", "FLOAT64", zones[4]),
                    bigquery.ScalarQueryParameter("act_id", "STRING", act_id),
                ]
            )
            bq.query(update_sql, job_config=job_config).result()
            patched += 1

        time.sleep(1)

    LOGGER.info("Done. patched=%d skipped=%d total=%d", patched, skipped, len(rows))
    if args.dry_run:
        LOGGER.info("(dry-run — no BQ writes performed)")


if __name__ == "__main__":
    main()
