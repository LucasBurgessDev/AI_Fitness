#!/usr/bin/env python3
"""Re-fetch HR data from Garmin for cycling activities that have null avg_hr in BQ.

Usage (run from pipeline/ directory):
    BQ_PROJECT_ID=health-data-482722 GARTH_DIR=.garth python backfill_hr.py

The script:
  1. Queries BQ for cycling activities with null avg_hr (up to --limit rows).
  2. For each, fetches the activity detail from Garmin Connect and extracts
     avg_hr / max_hr from the metrics timeseries (the most reliable source).
  3. Issues a BQ UPDATE to patch the row in-place.

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
LOGGER = logging.getLogger("backfill_hr")

PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "")
GARTH_DIR   = os.environ.get("GARTH_DIR", ".garth")

CYCLING_TYPES = (
    "cycling", "road_cycling", "gravel_cycling", "mountain_biking",
    "indoor_cycling", "virtual_ride", "spinning",
)


# ── Garmin helpers (copied from garmin_activities_daily.py) ──────────────────

def safe_float(v) -> Optional[float]:
    try:
        f = float(v)
        return f if f == f else None  # NaN guard
    except (TypeError, ValueError):
        return None


def extract_hr_from_metrics(detail: dict) -> tuple:
    """Return (avg_hr, max_hr) computed from the per-second metrics timeseries."""
    descriptors = detail.get("metricDescriptors") or []
    hr_idx = None
    for d in descriptors:
        if "HEART_RATE" in str(d.get("metricsType") or "").upper():
            hr_idx = d.get("metricsIndex")
            break
    if hr_idx is None:
        return None, None

    hr_vals = []
    for sample in (detail.get("activityDetailMetrics") or []):
        row = sample.get("metrics") or []
        if hr_idx < len(row) and row[hr_idx] is not None:
            v = safe_float(row[hr_idx])
            if v and 30 <= v <= 250:
                hr_vals.append(v)

    if not hr_vals:
        return None, None
    return round(sum(hr_vals) / len(hr_vals), 1), max(hr_vals)


def extract_summary_hr(detail: dict) -> tuple:
    """Return (avg_hr, max_hr) from summaryDTO fields."""
    s = detail.get("summaryDTO") or {}
    avg = safe_float(s.get("averageHR") or s.get("avgHR"))
    mx  = safe_float(s.get("maxHR"))
    return avg, mx


def fetch_detail(api, activity_id: str) -> dict:
    result = {}
    fn = getattr(api, "get_activity_details", None)
    if callable(fn):
        try:
            d = fn(activity_id)
            if isinstance(d, dict):
                result = d
        except Exception:
            pass
    try:
        summary = api.connectapi(f"activity-service/activity/{activity_id}", params={})
        if isinstance(summary, dict):
            if not result:
                result = summary
            elif not result.get("summaryDTO"):
                result["summaryDTO"] = summary.get("summaryDTO") or {}
    except Exception:
        pass
    return result


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=200,
                        help="Max activities to patch (default 200)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch HR but don't write to BQ")
    args = parser.parse_args()

    if not PROJECT_ID:
        raise SystemExit("BQ_PROJECT_ID env var is required.")

    # ── Connect to Garmin ────────────────────────────────────────────────────
    from garminconnect import Garmin
    api = Garmin(os.environ.get("GARMIN_EMAIL", ""), os.environ.get("GARMIN_PASSWORD", ""))
    api.login(tokenstore=GARTH_DIR)
    LOGGER.info("Garmin login OK")

    # ── Connect to BigQuery ──────────────────────────────────────────────────
    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT_ID)

    types_sql = ", ".join(f"'{t}'" for t in CYCLING_TYPES)
    query = f"""
    SELECT activity_id
    FROM `{PROJECT_ID}.garmin.garmin_activities`
    WHERE activity_type IN ({types_sql})
      AND avg_hr IS NULL
    GROUP BY activity_id
    ORDER BY MAX(date) DESC
    LIMIT {args.limit}
    """
    rows = list(bq.query(query).result())
    LOGGER.info("Found %d activities with null avg_hr", len(rows))

    patched = 0
    for i, row in enumerate(rows):
        act_id = str(row["activity_id"])
        LOGGER.info("[%d/%d] activity_id=%s — fetching detail…", i + 1, len(rows), act_id)

        detail = fetch_detail(api, act_id)

        avg_hr, max_hr = extract_summary_hr(detail)
        if avg_hr is None:
            avg_hr, max_hr = extract_hr_from_metrics(detail)

        if avg_hr is None:
            LOGGER.warning("  → no HR found in API for %s", act_id)
            time.sleep(1)
            continue

        LOGGER.info("  → avg_hr=%.1f  max_hr=%s", avg_hr, max_hr)

        if not args.dry_run:
            update_sql = f"""
            UPDATE `{PROJECT_ID}.garmin.garmin_activities`
            SET avg_hr = @avg_hr, max_hr = @max_hr
            WHERE activity_id = @act_id
              AND avg_hr IS NULL
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("avg_hr", "FLOAT64", avg_hr),
                    bigquery.ScalarQueryParameter("max_hr", "FLOAT64", max_hr),
                    bigquery.ScalarQueryParameter("act_id", "STRING", act_id),
                ]
            )
            bq.query(update_sql, job_config=job_config).result()
            patched += 1

        time.sleep(1)  # stay well within Garmin rate limits

    LOGGER.info("Done. %d/%d activities patched.", patched, len(rows))
    if args.dry_run:
        LOGGER.info("(dry-run — no BQ writes performed)")


if __name__ == "__main__":
    main()
