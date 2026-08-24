"""
GCS-backed binary store for skin-tracking photos.

Photos are stored per user at:
  gs://{bucket}/cycling-coach/skin-photos/{email_safe}/{lesion_id}/{photo_id}.jpg
  gs://{bucket}/cycling-coach/skin-photos/{email_safe}/{lesion_id}/{photo_id}_thumb.jpg

Private only — never made public and never served via signed URL. Bytes are
streamed back to the browser through an authenticated FastAPI route (see
app.py's /api/skin/photo/{photo_id}) so access always goes through the same
session check as the rest of the app.
"""
from __future__ import annotations

import logging
import os
import re

LOGGER = logging.getLogger(__name__)

_GCS_BUCKET = os.environ.get("GCS_PROFILE_BUCKET", "")
_PREFIX = "cycling-coach/skin-photos"


def _email_safe(email: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "_", email)


def _photo_path(email: str, lesion_id: str, photo_id: str, thumb: bool = False) -> str:
    suffix = "_thumb.jpg" if thumb else ".jpg"
    return f"{_PREFIX}/{_email_safe(email)}/{lesion_id}/{photo_id}{suffix}"


def _client():
    if not _GCS_BUCKET:
        return None
    try:
        from google.cloud import storage
        return storage.Client()
    except Exception as exc:
        LOGGER.warning("GCS unavailable: %s", exc)
        return None


def save_photo(email: str, lesion_id: str, photo_id: str, full_bytes: bytes, thumb_bytes: bytes) -> tuple[str, str]:
    """Upload the full-size and thumbnail JPEG bytes for a photo.

    Returns (full_gcs_path, thumb_gcs_path). Raises if GCS is unavailable —
    unlike the JSON stores, a photo with nowhere to live is a hard failure,
    not something to silently no-op.
    """
    if not _GCS_BUCKET:
        raise RuntimeError("GCS_PROFILE_BUCKET is not set — cannot store photos")
    client = _client()
    if client is None:
        raise RuntimeError("GCS client unavailable — cannot store photos")

    full_path = _photo_path(email, lesion_id, photo_id, thumb=False)
    thumb_path = _photo_path(email, lesion_id, photo_id, thumb=True)
    bucket = client.bucket(_GCS_BUCKET)
    bucket.blob(full_path).upload_from_string(full_bytes, content_type="image/jpeg")
    bucket.blob(thumb_path).upload_from_string(thumb_bytes, content_type="image/jpeg")
    LOGGER.info("Saved skin photo for %s: %s", email, full_path)
    return full_path, thumb_path


def load_photo_bytes(gcs_path: str) -> bytes | None:
    """Download raw bytes for a stored photo by its full GCS path."""
    client = _client()
    if client is None:
        return None
    try:
        blob = client.bucket(_GCS_BUCKET).blob(gcs_path)
        return blob.download_as_bytes()
    except Exception as exc:
        LOGGER.error("skin_store.load_photo_bytes error for %s: %s", gcs_path, exc)
        return None


def delete_photo(gcs_path: str, thumb_gcs_path: str) -> None:
    """Best-effort delete of both objects for a photo."""
    client = _client()
    if client is None:
        return
    bucket = client.bucket(_GCS_BUCKET)
    for path in (gcs_path, thumb_gcs_path):
        try:
            bucket.blob(path).delete()
        except Exception as exc:
            LOGGER.warning("skin_store.delete_photo could not delete %s: %s", path, exc)
