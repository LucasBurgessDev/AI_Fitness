"""
Vertex AI Gemini-based photo comparison for the skin-tracking feature.

Mirrors the raw google.genai.Client() call already used in agent.py's
_auto_save_insights — same Vertex-backed ADC identity as the rest of the app
(GOOGLE_GENAI_USE_VERTEXAI=1 in deploy), so photos never leave the user's own
GCP project or reach any third-party service.

This is a narrow, one-shot call — it is NOT wired into the health-coach chat
agent's tools, so skin photos never enter the shared chat session state.
"""
from __future__ import annotations

import logging

LOGGER = logging.getLogger(__name__)

_MODEL = "gemini-2.5-flash"

_PROMPT = (
    "You are comparing two close-up photos of the same skin spot (mole, skin tag, "
    "or wart) on one person, taken at different times, for personal change-tracking. "
    "The first image is the earlier photo; the second is the newer one.\n\n"
    "Describe in 2-4 short sentences any visible differences in size, color, shape, "
    "border definition, or elevation. If nothing looks meaningfully different, say so "
    "plainly.\n\n"
    "IMPORTANT:\n"
    "- Do NOT diagnose, name a condition, or estimate cancer risk.\n"
    "- If the change looks notable (e.g. clearly larger, new color, irregular border), "
    "end with one short sentence recommending they get it checked by a dermatologist.\n"
    "- Plain text only, no markdown, no bullet points."
)


def compare_photos(prev_bytes: bytes, new_bytes: bytes) -> str | None:
    """Return a short AI-generated description of visible changes, or None on failure.

    Best-effort by design: callers should treat a None return as "no AI note this
    time" rather than fail the photo upload.
    """
    try:
        from google import genai
        from google.genai.types import Content, Part

        client = genai.Client()
        content = Content(parts=[
            Part(text=_PROMPT),
            Part(text="Earlier photo:"),
            Part.from_bytes(data=prev_bytes, mime_type="image/jpeg"),
            Part(text="Newer photo:"),
            Part.from_bytes(data=new_bytes, mime_type="image/jpeg"),
        ])
        resp = client.models.generate_content(model=_MODEL, contents=content)
        text = (resp.text or "").strip()
        return text or None
    except Exception as exc:
        LOGGER.warning("skin_compare.compare_photos error: %s", exc)
        return None
