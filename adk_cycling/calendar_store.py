"""
GCS-backed store for per-user Google OAuth2 credentials (Calendar scope).

Tokens are saved after the OAuth callback and loaded on demand by the
calendar FunctionTools in agent.py. If the credentials are expired they
are refreshed in-place and re-persisted automatically.
"""
from __future__ import annotations

import datetime
import json
import logging
import os
from typing import Any, Optional

LOGGER = logging.getLogger(__name__)

_GCS_BUCKET = os.environ.get("GCS_PROFILE_BUCKET", "")
_PREFIX = "cycling-coach/calendar-tokens"


def _key_for(email: str) -> str:
    safe = email.replace("@", "_").replace(".", "_")
    return f"{_PREFIX}/{safe}.json"


def save_tokens(email: str, credentials) -> None:
    """Serialise and persist OAuth2 credentials to GCS."""
    if not _GCS_BUCKET:
        LOGGER.warning("GCS_PROFILE_BUCKET not set — calendar tokens not persisted")
        return
    try:
        from google.cloud import storage
        data = {
            "token": credentials.token,
            "refresh_token": credentials.refresh_token,
            "token_uri": credentials.token_uri,
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
            "scopes": list(credentials.scopes) if credentials.scopes else [],
        }
        client = storage.Client()
        blob = client.bucket(_GCS_BUCKET).blob(_key_for(email))
        blob.upload_from_string(
            json.dumps(data, indent=2),
            content_type="application/json",
        )
        LOGGER.info("Calendar tokens saved to gs://%s/%s", _GCS_BUCKET, _key_for(email))
    except Exception as exc:
        LOGGER.error("Could not save calendar tokens: %s", exc)


def load_tokens(email: str) -> Optional[object]:
    """Load and return google.oauth2.credentials.Credentials, or None if not found.

    If the credentials are expired and a refresh token is available, they are
    refreshed automatically and re-saved to GCS.
    """
    if not _GCS_BUCKET:
        return None
    try:
        from google.cloud import storage
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request

        client = storage.Client()
        blob = client.bucket(_GCS_BUCKET).blob(_key_for(email))
        if not blob.exists():
            return None

        data = json.loads(blob.download_as_text())
        creds = Credentials(
            token=data.get("token"),
            refresh_token=data.get("refresh_token"),
            token_uri=data.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=data.get("client_id"),
            client_secret=data.get("client_secret"),
            scopes=data.get("scopes"),
        )

        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_tokens(email, creds)

        return creds
    except Exception as exc:
        LOGGER.error("Could not load calendar tokens for %s: %s", email, exc)
        return None


def create_event_for_session(email: str, session: dict[str, Any], start_time: str = "07:00") -> dict[str, Any]:
    """Create a Google Calendar event for one plan session (any source — AI-generated
    or Garmin Coach). Extracted out of agent.py's create_training_event chat tool so
    both that tool and app.py's bulk /api/plan/sync-calendar route share exactly one
    implementation of the actual Calendar API call.

    Args:
        email: The signed-in user's email (calendar tokens are keyed on this).
        session: A plan session dict — uses "title", "date", "target_duration_min"
            (defaults to 60 if missing), and "note" (used as the event description).
        start_time: HH:MM (24-hour) — plan sessions don't carry a time of day, so
            callers pick one (e.g. the user's saved training_reminder_time).

    Returns:
        {"ok": True, "event_id": str, "html_link": str} on success, or
        {"ok": False, "not_connected": bool, "error": str | None} otherwise —
        "not_connected" distinguishes "no calendar tokens on file" from a real
        API error so callers can surface the right message.
    """
    creds = load_tokens(email)
    if creds is None:
        return {"ok": False, "not_connected": True, "error": None}
    try:
        from googleapiclient.discovery import build
        service = build("calendar", "v3", credentials=creds)
        duration_minutes = session.get("target_duration_min") or 60
        start_dt = datetime.datetime.fromisoformat(f"{session['date']}T{start_time}:00")
        end_dt = start_dt + datetime.timedelta(minutes=duration_minutes)
        event_body = {
            "summary": session.get("title") or "Training session",
            "description": session.get("note", "") or "",
            "start": {"dateTime": start_dt.isoformat(), "timeZone": "Europe/London"},
            "end": {"dateTime": end_dt.isoformat(), "timeZone": "Europe/London"},
        }
        created = service.events().insert(calendarId="primary", body=event_body).execute()
        return {"ok": True, "event_id": created.get("id", ""), "html_link": created.get("htmlLink", "")}
    except Exception as exc:
        LOGGER.error("create_event_for_session error: %s", exc)
        return {"ok": False, "not_connected": False, "error": str(exc)}
