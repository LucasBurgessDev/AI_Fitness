from __future__ import annotations

import tarfile
import tempfile
from pathlib import Path
from typing import Tuple

from google.cloud import storage


def _parse_gcs_uri(uri: str) -> Tuple[str, str]:
    uri = uri.strip()
    if not uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {uri}")
    rest = uri[5:]
    parts = rest.split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid GCS URI: {uri}")
    return parts[0], parts[1]


def _tar_dir(src_dir: Path, tar_path: Path) -> None:
    if not src_dir.exists() or not src_dir.is_dir():
        raise FileNotFoundError(f"Token dir not found: {src_dir}")
    tar_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(src_dir, arcname=src_dir.name)


def _untar_to_parent(tar_path: Path, dest_parent: Path) -> Path:
    if not tar_path.exists():
        raise FileNotFoundError(f"Tarball not found: {tar_path}")
    dest_parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(path=dest_parent)
    return dest_parent / ".garth"


def upload_token_cache(gcs_uri: str, garth_dir: Path) -> None:
    bucket_name, blob_name = _parse_gcs_uri(gcs_uri)

    with tempfile.TemporaryDirectory() as td:
        tar_path = Path(td) / "token_cache.tar.gz"
        _tar_dir(garth_dir, tar_path)

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(tar_path))


def download_token_cache(gcs_uri: str, dest_parent: Path) -> Path:
    bucket_name, blob_name = _parse_gcs_uri(gcs_uri)

    with tempfile.TemporaryDirectory() as td:
        tar_path = Path(td) / "token_cache.tar.gz"

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if not blob.exists(client):
            raise FileNotFoundError(f"Token cache not found at: {gcs_uri}")

        blob.download_to_filename(str(tar_path))
        return _untar_to_parent(tar_path, dest_parent)
