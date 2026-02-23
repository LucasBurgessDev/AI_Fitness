from __future__ import annotations

import os
from pathlib import Path

from token_cache_gcs import upload_token_cache


def main() -> None:
    gcs_uri = os.getenv("TOKEN_CACHE_GCS_URI")
    if not gcs_uri:
        raise RuntimeError("Missing TOKEN_CACHE_GCS_URI, example: gs://my-bucket/garmin/token_cache.tar.gz")

    garth_dir = Path(os.getenv("GARTH_DIR", ".garth")).resolve()
    upload_token_cache(gcs_uri, garth_dir)
    print(f"Uploaded {garth_dir} to {gcs_uri}")


if __name__ == "__main__":
    main()
