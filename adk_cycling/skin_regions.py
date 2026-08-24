"""
Predefined body regions for skin-tracking pin placement.

Each region has a default (x_pct, y_pct) position — percentages of the body
diagram's viewBox — used two ways:
  - picking a region from the dropdown jumps the pin straight to its default
  - clicking directly on the diagram sets an exact pin position, and the
    client snaps to the nearest region here (by distance) to keep region_key
    meaningful for labeling/reminders

Coordinates assume a "front" and "back" silhouette sharing one 0-100 x 0-200
viewBox. Left/right are anatomical (the person's own left/right), so on the
front view the person's left arm appears on the right side of the image; on
the back view that flips back the other way since the person has turned
around relative to the viewer.
"""
from __future__ import annotations

REGIONS = [
    # ── Front ──
    {"key": "head", "label": "Head / face", "view": "front", "x": 50, "y": 7},
    {"key": "neck", "label": "Neck", "view": "front", "x": 50, "y": 15},
    {"key": "chest", "label": "Chest", "view": "front", "x": 50, "y": 25},
    {"key": "abdomen", "label": "Abdomen", "view": "front", "x": 50, "y": 35},
    {"key": "right_shoulder", "label": "Right shoulder", "view": "front", "x": 30, "y": 21},
    {"key": "left_shoulder", "label": "Left shoulder", "view": "front", "x": 70, "y": 21},
    {"key": "right_upper_arm", "label": "Right upper arm", "view": "front", "x": 22, "y": 30},
    {"key": "left_upper_arm", "label": "Left upper arm", "view": "front", "x": 78, "y": 30},
    {"key": "right_forearm", "label": "Right forearm", "view": "front", "x": 17, "y": 42},
    {"key": "left_forearm", "label": "Left forearm", "view": "front", "x": 83, "y": 42},
    {"key": "right_hand", "label": "Right hand", "view": "front", "x": 14, "y": 53},
    {"key": "left_hand", "label": "Left hand", "view": "front", "x": 86, "y": 53},
    {"key": "right_thigh", "label": "Right thigh", "view": "front", "x": 40, "y": 62},
    {"key": "left_thigh", "label": "Left thigh", "view": "front", "x": 60, "y": 62},
    {"key": "right_shin", "label": "Right shin", "view": "front", "x": 40, "y": 82},
    {"key": "left_shin", "label": "Left shin", "view": "front", "x": 60, "y": 82},
    {"key": "right_foot", "label": "Right foot", "view": "front", "x": 40, "y": 97},
    {"key": "left_foot", "label": "Left foot", "view": "front", "x": 60, "y": 97},
    # ── Back ──
    {"key": "head_back", "label": "Back of head", "view": "back", "x": 50, "y": 7},
    {"key": "neck_back", "label": "Back of neck", "view": "back", "x": 50, "y": 15},
    {"key": "upper_back", "label": "Upper back", "view": "back", "x": 50, "y": 26},
    {"key": "lower_back", "label": "Lower back", "view": "back", "x": 50, "y": 36},
    {"key": "right_shoulder_back", "label": "Right shoulder (back)", "view": "back", "x": 70, "y": 21},
    {"key": "left_shoulder_back", "label": "Left shoulder (back)", "view": "back", "x": 30, "y": 21},
    {"key": "right_upper_arm_back", "label": "Right upper arm (back)", "view": "back", "x": 78, "y": 30},
    {"key": "left_upper_arm_back", "label": "Left upper arm (back)", "view": "back", "x": 22, "y": 30},
    {"key": "right_forearm_back", "label": "Right forearm (back)", "view": "back", "x": 83, "y": 42},
    {"key": "left_forearm_back", "label": "Left forearm (back)", "view": "back", "x": 17, "y": 42},
    {"key": "right_hand_back", "label": "Right hand (back)", "view": "back", "x": 86, "y": 53},
    {"key": "left_hand_back", "label": "Left hand (back)", "view": "back", "x": 14, "y": 53},
    {"key": "right_glute", "label": "Right glute", "view": "back", "x": 60, "y": 58},
    {"key": "left_glute", "label": "Left glute", "view": "back", "x": 40, "y": 58},
    {"key": "right_hamstring", "label": "Right hamstring", "view": "back", "x": 60, "y": 72},
    {"key": "left_hamstring", "label": "Left hamstring", "view": "back", "x": 40, "y": 72},
    {"key": "right_calf", "label": "Right calf", "view": "back", "x": 60, "y": 86},
    {"key": "left_calf", "label": "Left calf", "view": "back", "x": 40, "y": 86},
    {"key": "right_heel", "label": "Right heel", "view": "back", "x": 60, "y": 97},
    {"key": "left_heel", "label": "Left heel", "view": "back", "x": 40, "y": 97},
]

_BY_KEY = {r["key"]: r for r in REGIONS}


def region_default(region_key: str) -> dict | None:
    """Return the {key, label, view, x, y} default entry for a region key."""
    return _BY_KEY.get(region_key)


def region_label(region_key: str) -> str:
    r = _BY_KEY.get(region_key)
    return r["label"] if r else region_key.replace("_", " ").title()
