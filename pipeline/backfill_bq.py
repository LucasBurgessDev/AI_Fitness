#!/usr/bin/env python3
"""Backfill CSVs (from a local directory or Google Drive) into BigQuery.

Idempotent: uses row-level dedup so it is safe to re-run against a table
that already contains data.  Stats are deduplicated on (date, timestamp);
activities on activity_id.

Run via make targets:
    make backfill-bq                              # reads from Google Drive
    LOCAL_DATA_PATH=../historic_data make backfill-bq-local   # reads locally
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd

import bigquery_writer

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
LOGGER = logging.getLogger("backfill_bq")

PROJECT_ID = os.environ["BQ_PROJECT_ID"]
SAVE_PATH = Path(os.getenv("SAVE_PATH", "/tmp"))


def _load_stats_csvs(data_dir: Path) -> pd.DataFrame:
    """Load and concatenate all stats CSVs found in data_dir."""
    frames = []
    for name in ("garmin_stats.csv", "Copy of garmin_stats.csv"):
        p = data_dir / name
        if p.exists():
            df = pd.read_csv(p)
            LOGGER.info("Loaded %s: %d rows", p.name, len(df))
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    # Rename to BQ column names so we can dedup on 'date' and 'timestamp'
    combined = combined.rename(columns=bigquery_writer._CSV_TO_STATS_COLS)
    if "date" in combined.columns and "timestamp" in combined.columns:
        combined = combined.drop_duplicates(subset=["date", "timestamp"])
    LOGGER.info("Combined stats: %d rows after dedup", len(combined))
    return combined


def main() -> None:
    local_data_path = os.getenv("LOCAL_DATA_PATH")

    if local_data_path:
        # ----------------------------------------------------------------
        # Local mode: read files directly from disk (no Drive/GCS needed)
        # ----------------------------------------------------------------
        data_dir = Path(local_data_path)
        LOGGER.info("Using local data from: %s", data_dir.resolve())

        stats_df = _load_stats_csvs(data_dir)
        acts_csv = data_dir / "garmin_activities.csv"
        acts_df = pd.read_csv(acts_csv) if acts_csv.exists() else pd.DataFrame()
        if not acts_df.empty:
            LOGGER.info("Loaded garmin_activities.csv: %d rows", len(acts_df))

    else:
        # ----------------------------------------------------------------
        # Drive mode: download CSVs from Google Drive then read them
        # ----------------------------------------------------------------
        from drive_uploader import download_file_if_exists
        from token_cache_gcs import download_token_cache

        drive_folder_id = os.environ["DRIVE_FOLDER_ID"]
        token_cache_uri = os.environ["TOKEN_CACHE_GCS_URI"]

        SAVE_PATH.mkdir(parents=True, exist_ok=True)
        garth_dir = download_token_cache(token_cache_uri, SAVE_PATH)
        os.environ["GARTH_DIR"] = str(garth_dir)

        LOGGER.info("Downloading CSVs from Drive folder %s", drive_folder_id)
        for fname in ("garmin_stats.csv", "garmin_activities.csv"):
            download_file_if_exists(drive_folder_id, fname, SAVE_PATH / fname)

        stats_csv = SAVE_PATH / "garmin_stats.csv"
        acts_csv = SAVE_PATH / "garmin_activities.csv"

        if stats_csv.exists():
            raw = pd.read_csv(stats_csv)
            LOGGER.info("garmin_stats.csv: %d rows", len(raw))
            stats_df = raw.rename(columns=bigquery_writer._CSV_TO_STATS_COLS)
        else:
            stats_df = pd.DataFrame()

        acts_df = pd.read_csv(acts_csv) if acts_csv.exists() else pd.DataFrame()
        if not acts_df.empty:
            LOGGER.info("garmin_activities.csv: %d rows", len(acts_df))

    # ------------------------------------------------------------------
    # Write to BigQuery using row-level dedup (safe to re-run)
    # ------------------------------------------------------------------
    total = 0

    if not stats_df.empty and "date" in stats_df.columns:
        dates = sorted(stats_df["date"].astype(str).dropna().unique().tolist())
        LOGGER.info("Writing stats for %d dates (%s → %s)", len(dates), dates[0], dates[-1])
        total += bigquery_writer.write_stats_range(stats_df, PROJECT_ID, dates, "backfill")
    else:
        LOGGER.warning("No stats data — skipping stats backfill")

    if not acts_df.empty and "date" in acts_df.columns:
        dates = sorted(acts_df["date"].astype(str).dropna().unique().tolist())
        LOGGER.info("Writing activities for %d dates (%s → %s)", len(dates), dates[0], dates[-1])
        total += bigquery_writer.write_activities_range(acts_df, PROJECT_ID, dates, "backfill")
    else:
        LOGGER.warning("No activities data — skipping activities backfill")

    LOGGER.info("Backfill complete: %d total rows written to BigQuery", total)


if __name__ == "__main__":
    main()
