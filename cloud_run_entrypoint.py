from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

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

    LOGGER.info("Starting job")
    LOGGER.info("TOKEN_CACHE_GCS_URI=%s", token_uri)
    LOGGER.info("DRIVE_FOLDER_ID=%s", drive_folder_id)

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


    # Run scripts
    run_cmd(["python", "garmin_activities_daily.py"])
    run_cmd(["python", "garmin_stats_daily.py"])

    # Confirm CSVs exist
    list_dir(save_path)
    csvs = list(save_path.glob("*.csv"))
    LOGGER.info("CSV count in SAVE_PATH: %s", len(csvs))

    # Upload to Drive
    LOGGER.info("Uploading CSVs to Drive folder")
    upload_all_csvs(drive_folder_id, save_path)
    LOGGER.info("Drive upload complete")

    # Persist refreshed token cache
    upload_token_cache(token_uri, garth_dir)
    LOGGER.info("Token cache uploaded back to GCS, job complete")


if __name__ == "__main__":
    main()
