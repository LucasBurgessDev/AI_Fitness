"""
GCS-backed conversation session store.

Sessions are stored at:
  gs://{GCS_PROFILE_BUCKET}/cycling-coach/sessions/{email_safe}/{session_id}.json

Session JSON shape:
{
  "session_id": "uuid4",
  "email": "...",
  "title": "First 60 chars of first user message",
  "created_at": "ISO8601",
  "updated_at": "ISO8601",
  "messages": [{"role": "user|assistant", "content": "...", "timestamp": "ISO8601"}]
}
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

LOGGER = logging.getLogger(__name__)

_GCS_BUCKET = os.environ.get("GCS_PROFILE_BUCKET", "")
_SESSION_PREFIX = "cycling-coach/sessions"
_MAX_SESSIONS = 100
_RESTORE_MESSAGES = 20


def _email_safe(email: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "_", email)


def _blob_path(email: str, session_id: str) -> str:
    return f"{_SESSION_PREFIX}/{_email_safe(email)}/{session_id}.json"


def _get_client():
    if not _GCS_BUCKET:
        return None, None
    try:
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(_GCS_BUCKET)
        return client, bucket
    except Exception as exc:
        LOGGER.warning("GCS unavailable: %s", exc)
        return None, None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_session(email: str, session_id: str) -> dict:
    """Create an empty session in GCS and return it."""
    session: dict[str, Any] = {
        "session_id": session_id,
        "email": email,
        "title": "New conversation",
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "messages": [],
    }
    _, bucket = _get_client()
    if bucket is not None:
        try:
            blob = bucket.blob(_blob_path(email, session_id))
            blob.upload_from_string(
                json.dumps(session, indent=2, ensure_ascii=False),
                content_type="application/json",
            )
        except Exception as exc:
            LOGGER.error("session_store.create_session error: %s", exc)
    return session


def load_session(email: str, session_id: str) -> dict | None:
    """Load a session from GCS. Returns None if not found."""
    _, bucket = _get_client()
    if bucket is None:
        return None
    try:
        blob = bucket.blob(_blob_path(email, session_id))
        if not blob.exists():
            return None
        return json.loads(blob.download_as_text())
    except Exception as exc:
        LOGGER.error("session_store.load_session error: %s", exc)
        return None


def append_message(email: str, session_id: str, role: str, content: str) -> None:
    """Read-modify-write: append a message and update title/updated_at."""
    _, bucket = _get_client()
    if bucket is None:
        return
    try:
        blob = bucket.blob(_blob_path(email, session_id))
        if blob.exists():
            session = json.loads(blob.download_as_text())
        else:
            session = {
                "session_id": session_id,
                "email": email,
                "title": "New conversation",
                "created_at": _now_iso(),
                "updated_at": _now_iso(),
                "messages": [],
            }

        session["messages"].append({
            "role": role,
            "content": content,
            "timestamp": _now_iso(),
        })

        # Set title from first user message
        if role == "user" and session.get("title") == "New conversation":
            session["title"] = content[:60]

        session["updated_at"] = _now_iso()

        blob.upload_from_string(
            json.dumps(session, indent=2, ensure_ascii=False),
            content_type="application/json",
        )
    except Exception as exc:
        LOGGER.error("session_store.append_message error: %s", exc)


def list_sessions(email: str) -> list[dict]:
    """List sessions for email sorted by updated_at desc, capped at 100."""
    client, bucket = _get_client()
    if client is None or bucket is None:
        return []
    try:
        prefix = f"{_SESSION_PREFIX}/{_email_safe(email)}/"
        blobs = list(client.list_blobs(_GCS_BUCKET, prefix=prefix))
        if not blobs:
            return []

        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _read_blob(blob):
            try:
                data = json.loads(blob.download_as_text())
                return {
                    "session_id": data.get("session_id", ""),
                    "title": data.get("title", "New conversation"),
                    "created_at": data.get("created_at", ""),
                    "updated_at": data.get("updated_at", ""),
                }
            except Exception:
                return None

        sessions = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(_read_blob, blob) for blob in blobs]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    sessions.append(result)

        sessions.sort(key=lambda s: s.get("updated_at", ""), reverse=True)
        return sessions[:_MAX_SESSIONS]
    except Exception as exc:
        LOGGER.error("session_store.list_sessions error: %s", exc)
        return []


def delete_session(email: str, session_id: str) -> None:
    """Delete the GCS blob for a session."""
    _, bucket = _get_client()
    if bucket is None:
        return
    try:
        blob = bucket.blob(_blob_path(email, session_id))
        if blob.exists():
            blob.delete()
    except Exception as exc:
        LOGGER.error("session_store.delete_session error: %s", exc)


def get_restore_context(email: str, session_id: str) -> str:
    """Return the last 20 messages as a plain-text block for cold-start context restore.

    Returns empty string if no prior history exists.
    """
    session = load_session(email, session_id)
    if not session or not session.get("messages"):
        return ""
    messages = session["messages"][-_RESTORE_MESSAGES:]
    lines = ["[Prior conversation context — continue naturally from here]"]
    for msg in messages:
        role_label = "User" if msg["role"] == "user" else "Assistant"
        lines.append(f"{role_label}: {msg['content']}")
    lines.append("[End of prior context]")
    return "\n".join(lines)
