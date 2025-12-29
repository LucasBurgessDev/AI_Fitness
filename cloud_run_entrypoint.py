from __future__ import annotations

import os
import subprocess
from pathlib import Path

from token_cache_gcs import download_token_cache, upload_token_cache


def run(cmd: list[str]) -> None:
    p = subprocess.run(cmd, check=False)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")


def main() -> None:
    gcs_uri = os.getenv("TOKEN_CACHE_GCS_URI")
    if not gcs_uri:
        raise RuntimeError("Missing TOKEN_CACHE_GCS_URI")

    # Cloud Run writable area
    tmp_parent = Path("/tmp")
    garth_dir = download_token_cache(gcs_uri, tmp_parent)

    # Ensure all our scripts use this directory
    os.environ["GARTH_DIR"] = str(garth_dir)

    # Run daily pulls
    run(["python", "garmin_activities_daily.py"])
    run(["python", "garmin_stats_daily.py"])

    # Push back refreshed token cache
    upload_token_cache(gcs_uri, garth_dir)


if __name__ == "__main__":
    main()
