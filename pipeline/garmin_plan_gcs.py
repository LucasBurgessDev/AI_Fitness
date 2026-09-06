from __future__ import annotations

import json
from typing import Any, Optional

from google.cloud import storage


def upload_json(bucket_name: str, object_path: str, data: dict[str, Any]) -> None:
    """Upload a JSON-serialisable dict to gs://{bucket_name}/{object_path}."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_string(
        json.dumps(data, default=str),
        content_type="application/json",
    )


def download_json(bucket_name: str, object_path: str) -> Optional[dict[str, Any]]:
    """Return the JSON dict at gs://{bucket_name}/{object_path}, or None if it
    doesn't exist yet (first run) — never raises for a missing blob."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    if not blob.exists(client):
        return None
    return json.loads(blob.download_as_text())
