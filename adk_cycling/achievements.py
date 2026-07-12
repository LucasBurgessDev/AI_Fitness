"""
Rule-based goal-hit detection for weekly KPI rings and streaks.

Pure logic, no I/O — the caller is responsible for loading/saving the
`achievement_state` blob (persisted in profile.json) and for writing the
returned achievements to the coaching log / push notifications.

State shape:
{
    "initialized": bool,
    "kpi_hits": {kpi_key: {"period": str, "hit": bool}},
    "streak_bests": {streak_key: int},
}
"""
from __future__ import annotations

from typing import Any

# weekly_hours is intentionally excluded — mirrors goals.html's KPI_META,
# which drops it because the user prefers not to see it as a KPI ring.
KPI_META: dict[str, dict[str, Any]] = {
    "weekly_cycling_km":   {"icon": "🚴", "label": "Weekly Cycling Goal", "unit": "km", "is_target": False},
    "weekly_running_km":   {"icon": "🏃", "label": "Weekly Running Goal", "unit": "km", "is_target": False},
    "weekly_active_days":  {"icon": "📅", "label": "Weekly Active Days Goal", "unit": "days", "is_target": False},
    "target_weight_kg":    {"icon": "⚖️", "label": "Weight Target", "unit": "kg", "is_target": True},
    "target_body_fat_pct": {"icon": "📉", "label": "Body Fat Target", "unit": "%", "is_target": True},
}

STREAK_META: dict[str, dict[str, str]] = {
    "active_days": {"icon": "🔥", "label": "Active Days Streak"},
    "sleep_goal":  {"icon": "🔥", "label": "Sleep Goal Streak"},
    "step_goal":   {"icon": "🔥", "label": "Steps Streak"},
    "checkin":     {"icon": "🔥", "label": "Check-in Streak"},
}

_ACTUALS_KEY = {
    "weekly_cycling_km": "cycling_km",
    "weekly_running_km": "running_km",
    "weekly_active_days": "active_days",
}


def _kpi_hit(key: str, meta: dict[str, Any], target: float, actual: float | None) -> bool:
    if target <= 0 or actual is None:
        return False
    if meta["is_target"]:
        return abs(actual - target) < 0.1
    return actual >= target


def evaluate(
    kpis: dict[str, Any],
    actuals: dict[str, Any],
    weight_latest: dict[str, Any] | None,
    streaks: dict[str, Any],
    week_start: str,
    today_iso: str,
    state: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Detect newly-crossed goal thresholds and new-best streaks.

    Returns (new_achievements, updated_state). `state` may be None/empty on
    first run — in that case bests are seeded silently (no achievements are
    emitted) so a fresh deploy doesn't instantly "celebrate" every
    already-existing streak/goal.
    """
    state = state or {}
    seeding = not state.get("initialized")
    kpi_hits = dict(state.get("kpi_hits", {}))
    streak_bests = dict(state.get("streak_bests", {}))
    achievements: list[dict[str, Any]] = []

    for key, meta in KPI_META.items():
        cfg = kpis.get(key) or {}
        if not cfg.get("enabled"):
            continue
        target = float(cfg.get("target") or 0)

        if meta["is_target"]:
            actual = (weight_latest or {}).get(
                "weight_kg" if key == "target_weight_kg" else "body_fat_pct"
            )
            period = today_iso
        else:
            actual = actuals.get(_ACTUALS_KEY[key])
            period = week_start

        hit = _kpi_hit(key, meta, target, actual)
        prev = kpi_hits.get(key) or {}
        prev_hit = bool(prev.get("hit")) if prev.get("period") == period else False

        if hit and not prev_hit and not seeding:
            achievements.append({
                "type": "kpi",
                "is_target": meta["is_target"],
                "key": key,
                "icon": meta["icon"],
                "label": meta["label"],
                "value": actual,
                "target_or_best": target,
                "unit": meta["unit"],
            })
        kpi_hits[key] = {"period": period, "hit": hit}

    for key, meta in STREAK_META.items():
        current = int((streaks.get(key) or {}).get("current") or 0)
        prev_best = int(streak_bests.get(key, 0))
        if current > prev_best:
            if not seeding and prev_best > 0:
                achievements.append({
                    "type": "streak",
                    "key": key,
                    "icon": meta["icon"],
                    "label": meta["label"],
                    "value": current,
                    "target_or_best": prev_best,
                    "unit": "days",
                })
            streak_bests[key] = current

    updated_state = {
        "initialized": True,
        "kpi_hits": kpi_hits,
        "streak_bests": streak_bests,
    }
    return achievements, updated_state
