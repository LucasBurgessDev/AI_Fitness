"""
BigQuery-backed metadata store for skin-tracking lesions and photos.

Tables: garmin.skin_lesions, garmin.skin_photos (see terraform/bigquery.tf).
Follows the same insert_rows_json / parameterized-query pattern as
coaching_log.py — always filtered by email, even though this app is
single-tenant, for consistency with the rest of the codebase.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

LOGGER = logging.getLogger(__name__)


def _client(project_id: str):
    from google.cloud import bigquery
    return bigquery.Client(project=project_id)


def create_lesion(
    project_id: str,
    email: str,
    nickname: str,
    region_key: str,
    view: str,
    x_pct: float,
    y_pct: float,
) -> str:
    """Insert a new lesion and return its lesion_id."""
    lesion_id = str(uuid.uuid4())
    row = {
        "lesion_id": lesion_id,
        "email": email,
        "nickname": nickname,
        "region_key": region_key,
        "view": view,
        "x_pct": x_pct,
        "y_pct": y_pct,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "archived": False,
    }
    client = _client(project_id)
    errors = client.insert_rows_json(f"{project_id}.garmin.skin_lesions", [row])
    if errors:
        LOGGER.error("skin_lesions insert errors: %s", errors)
        raise RuntimeError(f"Could not save lesion: {errors}")
    return lesion_id


def list_lesions(project_id: str, email: str, include_archived: bool = False) -> list[dict]:
    """Return all lesions for a user, most-recently-created first."""
    from google.cloud import bigquery
    client = _client(project_id)
    archived_filter = "" if include_archived else "AND archived = FALSE"
    sql = f"""
        SELECT lesion_id, nickname, region_key, view, x_pct, y_pct, created_at, archived
        FROM `{project_id}.garmin.skin_lesions`
        WHERE email = @email {archived_filter}
        ORDER BY created_at DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("email", "STRING", email)]
    )
    rows = list(client.query(sql, job_config=job_config).result())
    return [dict(row) for row in rows]


def get_lesion(project_id: str, email: str, lesion_id: str) -> dict | None:
    from google.cloud import bigquery
    client = _client(project_id)
    sql = f"""
        SELECT lesion_id, nickname, region_key, view, x_pct, y_pct, created_at, archived
        FROM `{project_id}.garmin.skin_lesions`
        WHERE email = @email AND lesion_id = @lesion_id
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", email),
            bigquery.ScalarQueryParameter("lesion_id", "STRING", lesion_id),
        ]
    )
    rows = list(client.query(sql, job_config=job_config).result())
    return dict(rows[0]) if rows else None


def update_lesion(
    project_id: str,
    email: str,
    lesion_id: str,
    nickname: str | None = None,
    x_pct: float | None = None,
    y_pct: float | None = None,
    archived: bool | None = None,
) -> None:
    """Patch a lesion's mutable fields (rename, re-pin, archive)."""
    from google.cloud import bigquery
    sets = []
    params = [
        bigquery.ScalarQueryParameter("email", "STRING", email),
        bigquery.ScalarQueryParameter("lesion_id", "STRING", lesion_id),
    ]
    if nickname is not None:
        sets.append("nickname = @nickname")
        params.append(bigquery.ScalarQueryParameter("nickname", "STRING", nickname))
    if x_pct is not None:
        sets.append("x_pct = @x_pct")
        params.append(bigquery.ScalarQueryParameter("x_pct", "FLOAT64", x_pct))
    if y_pct is not None:
        sets.append("y_pct = @y_pct")
        params.append(bigquery.ScalarQueryParameter("y_pct", "FLOAT64", y_pct))
    if archived is not None:
        sets.append("archived = @archived")
        params.append(bigquery.ScalarQueryParameter("archived", "BOOL", archived))
    if not sets:
        return

    client = _client(project_id)
    sql = f"""
        UPDATE `{project_id}.garmin.skin_lesions`
        SET {', '.join(sets)}
        WHERE email = @email AND lesion_id = @lesion_id
    """
    client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


def add_photo(
    project_id: str,
    email: str,
    lesion_id: str,
    photo_id: str,
    gcs_path: str,
    thumb_gcs_path: str,
    note: str,
    ai_notes: str | None,
    width: int,
    height: int,
    file_size_bytes: int,
) -> str:
    """Insert a new photo row (photo_id must match the one used for its GCS path)."""
    row = {
        "photo_id": photo_id,
        "lesion_id": lesion_id,
        "email": email,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "gcs_path": gcs_path,
        "thumb_gcs_path": thumb_gcs_path,
        "note": note or None,
        "ai_notes": ai_notes or None,
        "width": width,
        "height": height,
        "file_size_bytes": file_size_bytes,
    }
    client = _client(project_id)
    errors = client.insert_rows_json(f"{project_id}.garmin.skin_photos", [row])
    if errors:
        LOGGER.error("skin_photos insert errors: %s", errors)
        raise RuntimeError(f"Could not save photo: {errors}")
    return photo_id


def list_photos(project_id: str, email: str, lesion_id: str) -> list[dict]:
    """Return all photos for a lesion, oldest first (natural timeline order)."""
    from google.cloud import bigquery
    client = _client(project_id)
    sql = f"""
        SELECT photo_id, lesion_id, captured_at, gcs_path, thumb_gcs_path,
               note, ai_notes, width, height, file_size_bytes
        FROM `{project_id}.garmin.skin_photos`
        WHERE email = @email AND lesion_id = @lesion_id
        ORDER BY captured_at ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", email),
            bigquery.ScalarQueryParameter("lesion_id", "STRING", lesion_id),
        ]
    )
    rows = list(client.query(sql, job_config=job_config).result())
    return [dict(row) for row in rows]


def get_photo(project_id: str, email: str, photo_id: str) -> dict | None:
    from google.cloud import bigquery
    client = _client(project_id)
    sql = f"""
        SELECT photo_id, lesion_id, gcs_path, thumb_gcs_path
        FROM `{project_id}.garmin.skin_photos`
        WHERE email = @email AND photo_id = @photo_id
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", email),
            bigquery.ScalarQueryParameter("photo_id", "STRING", photo_id),
        ]
    )
    rows = list(client.query(sql, job_config=job_config).result())
    return dict(rows[0]) if rows else None


def latest_photo(project_id: str, email: str, lesion_id: str) -> dict | None:
    """Return the most recent photo for a lesion, or None if it has none yet."""
    photos = list_photos(project_id, email, lesion_id)
    return photos[-1] if photos else None


def count_lesions_needing_check(project_id: str, email: str, days: int) -> int:
    """Count active lesions whose most recent photo is older than `days` (or has none)."""
    from google.cloud import bigquery
    client = _client(project_id)
    sql = f"""
        SELECT COUNT(*) AS n FROM (
          SELECT l.lesion_id, MAX(p.captured_at) AS last_captured
          FROM `{project_id}.garmin.skin_lesions` l
          LEFT JOIN `{project_id}.garmin.skin_photos` p USING (lesion_id)
          WHERE l.email = @email AND l.archived = FALSE
          GROUP BY l.lesion_id
        )
        WHERE last_captured IS NULL
           OR last_captured < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", email),
            bigquery.ScalarQueryParameter("days", "INT64", days),
        ]
    )
    rows = list(client.query(sql, job_config=job_config).result())
    return int(rows[0]["n"]) if rows else 0
