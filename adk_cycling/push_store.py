"""
GCS-backed push subscription store.

Subscriptions are stored per user at:
  gs://{bucket}/cycling-coach/push-subscriptions/{email_safe}.json
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import List

LOGGER = logging.getLogger(__name__)

_GCS_BUCKET = os.environ.get("GCS_PROFILE_BUCKET", "")
_PREFIX = "cycling-coach/push-subscriptions"


def _email_safe(email: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "_", email)


def _blob_path(email: str) -> str:
    return f"{_PREFIX}/{_email_safe(email)}.json"


def save_subscription(email: str, subscription: dict) -> None:
    """Upsert a push subscription for a user (deduplicated by endpoint)."""
    if not _GCS_BUCKET:
        return
    try:
        from google.cloud import storage
        client = storage.Client()
        blob = client.bucket(_GCS_BUCKET).blob(_blob_path(email))
        subs: list = []
        if blob.exists():
            subs = json.loads(blob.download_as_text()).get("subscriptions", [])
        endpoint = subscription.get("endpoint", "")
        subs = [s for s in subs if s.get("endpoint") != endpoint]
        subs.append(subscription)
        blob.upload_from_string(
            json.dumps({"email": email, "subscriptions": subs}),
            content_type="application/json",
        )
        LOGGER.info("Saved push subscription for %s", email)
    except Exception as exc:
        LOGGER.error("push_store.save_subscription error: %s", exc)


def remove_subscription(email: str, endpoint: str) -> None:
    """Remove a push subscription by endpoint."""
    if not _GCS_BUCKET:
        return
    try:
        from google.cloud import storage
        client = storage.Client()
        blob = client.bucket(_GCS_BUCKET).blob(_blob_path(email))
        if not blob.exists():
            return
        data = json.loads(blob.download_as_text())
        data["subscriptions"] = [
            s for s in data.get("subscriptions", []) if s.get("endpoint") != endpoint
        ]
        blob.upload_from_string(json.dumps(data), content_type="application/json")
    except Exception as exc:
        LOGGER.error("push_store.remove_subscription error: %s", exc)


def get_subscriptions(email: str) -> List[dict]:
    """Return all active push subscriptions for a user."""
    if not _GCS_BUCKET:
        return []
    try:
        from google.cloud import storage
        client = storage.Client()
        blob = client.bucket(_GCS_BUCKET).blob(_blob_path(email))
        if not blob.exists():
            return []
        return json.loads(blob.download_as_text()).get("subscriptions", [])
    except Exception as exc:
        LOGGER.error("push_store.get_subscriptions error: %s", exc)
        return []


def list_all_emails() -> List[str]:
    """Return emails of all users who have push subscriptions."""
    if not _GCS_BUCKET:
        return []
    try:
        from google.cloud import storage
        client = storage.Client()
        blobs = list(client.list_blobs(_GCS_BUCKET, prefix=f"{_PREFIX}/"))
        emails = []
        for blob in blobs:
            try:
                data = json.loads(blob.download_as_text())
                if data.get("email") and data.get("subscriptions"):
                    emails.append(data["email"])
            except Exception:
                pass
        return emails
    except Exception as exc:
        LOGGER.error("push_store.list_all_emails error: %s", exc)
        return []
