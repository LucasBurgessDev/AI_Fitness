from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
import io


LOGGER = logging.getLogger("drive_uploader")

# Use full Drive scope: folder permissions still constrain access
DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"


def _drive_service():
    creds, project_id = google.auth.default(scopes=[DRIVE_SCOPE])
    LOGGER.info("Drive auth ready, project_id=%s, scopes=%s", project_id, getattr(creds, "scopes", None))
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _folder_check(service, folder_id: str) -> None:
    try:
        meta = service.files().get(
            fileId=folder_id,
            fields="id,name,mimeType,driveId,parents",
            supportsAllDrives=True,
        ).execute()
        LOGGER.info(
            "Drive folder accessible: id=%s name=%s mimeType=%s driveId=%s",
            meta.get("id"), meta.get("name"), meta.get("mimeType"), meta.get("driveId")
        )
    except HttpError as e:
        LOGGER.exception("Drive folder check failed for folder_id=%s", folder_id)
        raise


def _escape_query_string(s: str) -> str:
    return (s or "").replace("'", "\\'")


def _find_file_in_folder(service, folder_id: str, filename: str) -> Optional[str]:
    q = (
        f"name = '{_escape_query_string(filename)}' "
        f"and '{folder_id}' in parents "
        f"and trashed = false"
    )

    resp = service.files().list(
        q=q,
        fields="files(id,name,modifiedTime,owners,driveId)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        corpora="allDrives",
        pageSize=10,
    ).execute()

    files = resp.get("files", [])
    if not files:
        LOGGER.info("Drive search: no match for %s in folder %s", filename, folder_id)
        return None

    f0 = files[0]
    LOGGER.info(
        "Drive search: found existing file: name=%s id=%s modified=%s driveId=%s",
        f0.get("name"), f0.get("id"), f0.get("modifiedTime"), f0.get("driveId")
    )
    return f0["id"]


def _list_folder(service, folder_id: str, limit: int = 50) -> None:
    try:
        resp = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id,name,mimeType,modifiedTime)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            corpora="allDrives",
            pageSize=limit,
        ).execute()

        files = resp.get("files", [])
        LOGGER.info("Drive folder listing: %s files (showing up to %s)", len(files), limit)
        for f in files[:limit]:
            LOGGER.info("  - %s | %s | %s", f.get("name"), f.get("id"), f.get("modifiedTime"))
    except HttpError:
        LOGGER.exception("Drive folder listing failed for folder_id=%s", folder_id)


def upload_or_replace_csv(folder_id: str, local_path: Path) -> str:
    service = _drive_service()
    _folder_check(service, folder_id)

    if not local_path.exists():
        raise FileNotFoundError(f"Missing local file: {local_path}")

    filename = local_path.name
    media = MediaFileUpload(str(local_path), mimetype="text/csv", resumable=True)

    LOGGER.info("Uploading CSV to Drive: %s size_bytes=%s", filename, local_path.stat().st_size)

    existing_id = _find_file_in_folder(service, folder_id, filename)

    try:
        if existing_id:
            LOGGER.info("Drive update starting: fileId=%s name=%s", existing_id, filename)
            updated = service.files().update(
                fileId=existing_id,
                media_body=media,
                supportsAllDrives=True,
            ).execute()
            LOGGER.info("Drive update complete: fileId=%s", updated.get("id"))
            return updated["id"]

        metadata = {"name": filename, "parents": [folder_id]}
        LOGGER.info("Drive create starting: name=%s parents=[%s]", filename, folder_id)
        created = service.files().create(
            body=metadata,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()
        LOGGER.info("Drive create complete: fileId=%s", created.get("id"))
        return created["id"]

    except HttpError:
        LOGGER.exception("Drive upload failed for %s", filename)
        raise


def upload_all_csvs(folder_id: str, save_path: Path) -> None:
    service = _drive_service()
    _folder_check(service, folder_id)

    LOGGER.info("Scanning for CSVs in %s", str(save_path))
    csvs = sorted(save_path.glob("*.csv"))

    LOGGER.info("Found %s CSVs", len(csvs))
    for p in csvs:
        LOGGER.info("  - %s (%s bytes)", p.name, p.stat().st_size)

    LOGGER.info("Folder listing before upload:")
    _list_folder(service, folder_id)

    for p in csvs:
        upload_or_replace_csv(folder_id, p)

    LOGGER.info("Folder listing after upload:")
    _list_folder(service, folder_id)


def download_file_if_exists(folder_id: str, filename: str, dest_path: Path) -> bool:
    service = _drive_service()
    _folder_check(service, folder_id)

    file_id = _find_file_in_folder(service, folder_id, filename)
    if not file_id:
        LOGGER.info("Drive download: %s not found in folder %s", filename, folder_id)
        return False

    LOGGER.info("Drive download starting: %s fileId=%s -> %s", filename, file_id, dest_path)
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(str(dest_path), "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            LOGGER.info("Drive download progress: %s%%", int(status.progress() * 100))

    LOGGER.info("Drive download complete: %s bytes=%s", filename, dest_path.stat().st_size)
    return True
