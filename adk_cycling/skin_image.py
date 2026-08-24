"""
Image processing for skin-tracking photo uploads.

These are personal medical photos, not fitness data — this module exists
specifically to (a) strip EXIF metadata (especially GPS) before anything is
stored, and (b) cap resolution so the private photo archive doesn't balloon.
"""
from __future__ import annotations

import io

_MAX_DIMENSION = 2000
_THUMB_DIMENSION = 400
_JPEG_QUALITY = 88


def process_upload(raw_bytes: bytes) -> tuple[bytes, bytes, int, int]:
    """Re-encode an uploaded image as EXIF-stripped JPEG + thumbnail.

    Returns (full_jpeg_bytes, thumb_jpeg_bytes, width, height) of the full image.
    Raises ValueError if the bytes can't be decoded as an image.
    """
    from PIL import Image, ImageOps

    try:
        img = Image.open(io.BytesIO(raw_bytes))
        img = ImageOps.exif_transpose(img)  # apply rotation before dropping EXIF
        img = img.convert("RGB")
    except Exception as exc:
        raise ValueError(f"Could not read image: {exc}") from exc

    img.thumbnail((_MAX_DIMENSION, _MAX_DIMENSION), Image.LANCZOS)
    width, height = img.size

    full_buf = io.BytesIO()
    img.save(full_buf, format="JPEG", quality=_JPEG_QUALITY)  # no exif= kwarg -> stripped

    thumb = img.copy()
    thumb.thumbnail((_THUMB_DIMENSION, _THUMB_DIMENSION), Image.LANCZOS)
    thumb_buf = io.BytesIO()
    thumb.save(thumb_buf, format="JPEG", quality=_JPEG_QUALITY)

    return full_buf.getvalue(), thumb_buf.getvalue(), width, height
