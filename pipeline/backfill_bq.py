#!/usr/bin/env python3
"""One-off backfill: download CSVs from Google Drive and append all rows into BigQuery.

Skips a table if it already contains data (idempotent re-runs).
Each row uses its own date as run_date so history is correctly partitioned.

Run via:
    make backfill-bq
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

import bigquery_writer
from drive_uploader import download_file_if_exists
from token_cache_gcs import download_token_cache

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
LOGGER = logging.getLogger("backfill_bq")

PROJECT_ID = os.environ["BQ_PROJECT_ID"]
DRIVE_FOLDER_ID = os.environ["DRIVE_FOLDER_ID"]
TOKEN_CACHE_GCS_URI = os.environ["TOKEN_CACHE_GCS_URI"]
SAVE_PATH = Path(os.getenv("SAVE_PATH", "/tmp"))


def _table_row_count(client: bigquery.Client, table_id: str) -> int:
    """Return the approximate row count of a BQ table (0 if not found)."""
    try:
        result = client.query(f"SELECT COUNT(*) AS n FROM `{table_id}`").result()
        return next(iter(result))["n"]
    except Exception:
        return 0


def _backfill_table(
    df: pd.DataFrame,
    table_id: str,
    schema: list,
    coerce_fn,
) -> int:
    """Append all rows to table using each row's 'date' as run_date.

    Skips the table if it already contains rows (to avoid duplicates on re-runs).
    """
    client = bigquery.Client(project=PROJECT_ID)

    existing = _table_row_count(client, table_id)
    if existing > 0:
        LOGGER.info("Table %s already has %d rows — skipping backfill", table_id, existing)
        return 0

    if "date" not in df.columns:
        LOGGER.error("No 'date' column — skipping %s", table_id)
        return 0

    df = df[df["date"].notna() & (df["date"].astype(str).str.strip() != "")]
    if df.empty:
        LOGGER.warning("No valid rows for %s", table_id)
        return 0

    df.insert(0, "run_date", df["date"].astype(str))
    df.insert(1, "batch_id", "backfill")

    df = coerce_fn(df)
    df = bigquery_writer._ensure_schema_cols(df, schema)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="run_date",
        ),
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    LOGGER.info("Backfilled %d rows → %s", len(df), table_id)
    return len(df)


def backfill_stats(df: pd.DataFrame) -> int:
    out = df.rename(columns=bigquery_writer._CSV_TO_STATS_COLS).copy()

    def coerce(d):
        d = bigquery_writer._coerce_int_cols(d, {
            "sleep_score", "rhr", "min_hr", "max_hr", "avg_stress",
            "body_battery", "steps", "step_goal", "cals_total", "cals_active",
        })
        return bigquery_writer._coerce_float_cols(d, {
            "weight_lbs", "muscle_mass_lbs", "body_fat_pct", "water_pct",
            "sleep_total_hr", "sleep_deep_hr", "sleep_rem_hr",
            "respiration", "spo2", "vo2_max", "hrv_avg",
        })

    return _backfill_table(
        out,
        f"{PROJECT_ID}.garmin.garmin_stats",
        bigquery_writer._STATS_SCHEMA,
        coerce,
    )


def backfill_activities(df: pd.DataFrame) -> int:
    out = df.copy()

    # activity_id is numeric in the CSV but STRING in BQ schema
    if "activity_id" in out.columns:
        out["activity_id"] = out["activity_id"].astype(str)

    if "avg_pace_min_mile" in out.columns:
        out["avg_pace_min_mile"] = out["avg_pace_min_mile"].apply(
            bigquery_writer._pace_str_to_float
        )

    def coerce(d):
        d = bigquery_writer._coerce_int_cols(d, {
            "calories", "avg_hr", "max_hr",
            "running_cadence_spm", "cycling_cadence_rpm",
        })
        return bigquery_writer._coerce_float_cols(d, {
            "distance_m", "duration_s", "avg_speed_mps", "max_speed_mps",
            "avg_pace_min_mile", "avg_power_w", "max_power_w",
            "elevation_gain_m", "aerobic_te", "anaerobic_te",
            "best_20m_watts", "ftp_watts", "normalized_power_w",
            "intensity_factor", "tss",
        })

    return _backfill_table(
        out,
        f"{PROJECT_ID}.garmin.garmin_activities",
        bigquery_writer._ACTIVITIES_SCHEMA,
        coerce,
    )


def main() -> None:
    SAVE_PATH.mkdir(parents=True, exist_ok=True)

    garth_dir = download_token_cache(TOKEN_CACHE_GCS_URI, SAVE_PATH)
    os.environ["GARTH_DIR"] = str(garth_dir)

    stats_csv = SAVE_PATH / "garmin_stats.csv"
    acts_csv = SAVE_PATH / "garmin_activities.csv"

    LOGGER.info("Downloading CSVs from Drive folder %s", DRIVE_FOLDER_ID)
    download_file_if_exists(DRIVE_FOLDER_ID, "garmin_stats.csv", stats_csv)
    download_file_if_exists(DRIVE_FOLDER_ID, "garmin_activities.csv", acts_csv)

    total = 0

    if stats_csv.exists():
        df = pd.read_csv(stats_csv)
        LOGGER.info("garmin_stats.csv: %d rows", len(df))
        total += backfill_stats(df)
    else:
        LOGGER.warning("garmin_stats.csv not downloaded — skipping stats backfill")

    if acts_csv.exists():
        df = pd.read_csv(acts_csv)
        LOGGER.info("garmin_activities.csv: %d rows", len(df))
        total += backfill_activities(df)
    else:
        LOGGER.warning("garmin_activities.csv not downloaded — skipping activities backfill")

    LOGGER.info("Backfill complete: %d total rows written to BigQuery", total)


if __name__ == "__main__":
    main()
