from __future__ import annotations

import logging
import os
import subprocess
from datetime import date
from pathlib import Path

import batch_control
import bigquery_writer
from token_cache_gcs import download_token_cache, upload_token_cache
from drive_uploader import upload_all_csvs, download_file_if_exists


def _setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


LOGGER = logging.getLogger("cloud_run_entrypoint")


def run_cmd(cmd: list[str]) -> None:
    LOGGER.info("Running: %s", " ".join(cmd))
    p = subprocess.run(cmd, capture_output=True, text=True)
    LOGGER.info("Return code: %s", p.returncode)
    if p.stdout:
        LOGGER.info("STDOUT:\n%s", p.stdout[-4000:])
    if p.stderr:
        LOGGER.info("STDERR:\n%s", p.stderr[-4000:])
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")


def list_dir(path: Path) -> None:
    if not path.exists():
        LOGGER.info("Path missing: %s", str(path))
        return
    LOGGER.info("Listing: %s", str(path))
    for p in sorted(path.glob("*")):
        try:
            size = p.stat().st_size
        except Exception:
            size = None
        LOGGER.info("  - %s size=%s", p.name, size)


def main() -> None:
    _setup_logging()

    token_uri = os.environ["TOKEN_CACHE_GCS_URI"]
    drive_folder_id = os.environ["DRIVE_FOLDER_ID"]
    project_id = os.getenv("BQ_PROJECT_ID", "")

    LOGGER.info("Starting job")
    LOGGER.info("TOKEN_CACHE_GCS_URI=%s", token_uri)
    LOGGER.info("DRIVE_FOLDER_ID=%s", drive_folder_id)
    LOGGER.info("BQ_PROJECT_ID=%s", project_id or "(not set — BigQuery write skipped)")

    # Download token cache to /tmp/.garth
    garth_dir = download_token_cache(token_uri, Path("/tmp"))
    os.environ["GARTH_DIR"] = str(garth_dir)
    LOGGER.info("GARTH_DIR=%s", os.environ["GARTH_DIR"])

    save_path = Path(os.getenv("SAVE_PATH", "/tmp"))
    save_path.mkdir(parents=True, exist_ok=True)
    LOGGER.info("SAVE_PATH=%s", str(save_path))

    # Pull existing CSVs from Drive so we append reliably across executions
    download_file_if_exists(drive_folder_id, "garmin_activities.csv", save_path / "garmin_activities.csv")
    download_file_if_exists(drive_folder_id, "garmin_stats.csv", save_path / "garmin_stats.csv")

    # Run data collection scripts
    run_cmd(["python", "garmin_activities_daily.py"])
    run_cmd(["python", "garmin_stats_daily.py"])

    # Confirm CSVs exist
    list_dir(save_path)
    csvs = list(save_path.glob("*.csv"))
    LOGGER.info("CSV count in SAVE_PATH: %s", len(csvs))

    # ------------------------------------------------------------------
    # BigQuery write (skipped gracefully if BQ_PROJECT_ID is not set)
    # ------------------------------------------------------------------
    if project_id:
        import pandas as pd

        today = date.today()
        batch_id = batch_control.start_batch(project_id, "garmin-fitness-daily")
        total_rows = 0

        try:
            stats_csv = save_path / "garmin_stats.csv"
            acts_csv = save_path / "garmin_activities.csv"

            if stats_csv.exists():
                df_stats = pd.read_csv(stats_csv)
                total_rows += bigquery_writer.write_stats(df_stats, project_id, today, batch_id)

            if acts_csv.exists():
                df_acts = pd.read_csv(acts_csv)
                total_rows += bigquery_writer.write_activities(df_acts, project_id, today, batch_id)

            batch_control.end_batch(project_id, batch_id, total_rows, "SUCCESS")
            LOGGER.info("BigQuery write complete: %d rows", total_rows)

        except Exception as bq_err:
            LOGGER.error("BigQuery write failed: %s", bq_err)
            try:
                batch_control.end_batch(project_id, batch_id, total_rows, "FAILED", str(bq_err))
            except Exception:
                pass
    # ------------------------------------------------------------------

    # Upload updated CSVs to Drive (non-fatal — quota errors shouldn't abort the job)
    LOGGER.info("Uploading CSVs to Drive folder")
    try:
        upload_all_csvs(drive_folder_id, save_path)
        LOGGER.info("Drive upload complete")
    except Exception as drive_err:
        LOGGER.error("Drive upload failed (continuing): %s", drive_err)

    # Persist refreshed token cache
    upload_token_cache(token_uri, garth_dir)
    LOGGER.info("Token cache uploaded back to GCS, job complete")


if __name__ == "__main__":
    main()
