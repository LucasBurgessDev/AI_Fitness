"""
Generate and persist VAPID keys for Web Push notifications.

Keys are generated once, stored in GCS, and cached in memory.
The public key is served to the browser; the private key signs push requests.
"""
from __future__ import annotations

import base64
import json
import logging
import os
from typing import Optional, Tuple

LOGGER = logging.getLogger(__name__)

_GCS_BUCKET = os.environ.get("GCS_PROFILE_BUCKET", "")
_GCS_OBJECT = "cycling-coach/vapid-keys.json"
_cache: Optional[dict] = None


def get_keys() -> Tuple[str, str]:
    """Return (public_key_urlsafe_b64, private_key_pem).

    Generated on first call and stored in GCS for reuse across cold-starts.
    """
    global _cache
    if _cache:
        return _cache["public"], _cache["private"]

    if _GCS_BUCKET:
        try:
            from google.cloud import storage
            client = storage.Client()
            blob = client.bucket(_GCS_BUCKET).blob(_GCS_OBJECT)
            if blob.exists():
                data = json.loads(blob.download_as_text())
                _cache = data
                return data["public"], data["private"]
        except Exception as exc:
            LOGGER.warning("Could not load VAPID keys from GCS: %s", exc)

    LOGGER.info("Generating new VAPID key pair")
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend

    private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
    public_bytes = private_key.public_key().public_bytes(
        serialization.Encoding.X962,
        serialization.PublicFormat.UncompressedPoint,
    )
    public_b64 = base64.urlsafe_b64encode(public_bytes).rstrip(b"=").decode()
    private_pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()

    data = {"public": public_b64, "private": private_pem}
    _cache = data

    if _GCS_BUCKET:
        try:
            from google.cloud import storage
            client = storage.Client()
            blob = client.bucket(_GCS_BUCKET).blob(_GCS_OBJECT)
            blob.upload_from_string(json.dumps(data), content_type="application/json")
            LOGGER.info("VAPID keys saved to GCS")
        except Exception as exc:
            LOGGER.warning("Could not save VAPID keys to GCS: %s", exc)

    return public_b64, private_pem
