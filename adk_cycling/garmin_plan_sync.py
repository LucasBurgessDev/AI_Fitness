"""
Translates the raw Garmin Coach plan (cached by garmin_plan_store.py) into the
exact session shape plan_generator.py already produces, then merges it into
plan_store — so every downstream consumer (plan_progress.py's matching/rollup/
weather-swap logic, mark_session_complete/adjust_session/
link_session_calendar_event, /api/plan/progress, plan.html) works unchanged
regardless of whether a plan came from the AI generator or Garmin.

Called from plan_progress.ensure_garmin_plan_synced() — see that module for
when/how often this runs.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Optional

import plan_generator
import plan_store
import garmin_plan_store

LOGGER = logging.getLogger(__name__)

# Garmin's own `taskWorkout.workoutPhrase` strings, mapped onto our internal
# session_type vocabulary (plan_generator.PLAIN_LABELS). Confirmed against a
# real Garmin Coach cycling plan pulled live during this feature's development
# (LONG_WORKOUT, EASY_WEEK_LOAD_BASE, LACTATE_THRESHOLD); the rest are
# best-effort guesses at plausible Garmin vocabulary. Garmin's full phrase set
# isn't documented anywhere we have access to, so anything not in this table
# falls back to a safe default rather than erroring — see _DEFAULT_SESSION_TYPE.
GARMIN_PHRASE_TO_SESSION_TYPE: dict[str, str] = {
    "TRAINING_READINESS_REST": "rest",
    "REST": "rest",
    "LONG_WORKOUT": "long_endurance",
    "EASY_WEEK_LOAD_BASE": "endurance",
    "RECOVERY": "recovery",
    "LACTATE_THRESHOLD": "sweet_spot",
    "THRESHOLD": "sweet_spot",
    "TEMPO": "tempo",
    "HILL_REPEATS": "hill_repeats",
    "INTERVALS": "intervals",
    "VO2MAX": "intervals",
    "RACE_PACE": "race_pace_long",
    "OPENERS": "openers",
}
_DEFAULT_SESSION_TYPE = "endurance"
_DEFAULT_RUNNING_SESSION_TYPE = "easy_run"

# Same running-vs-cycling session_type split plan_generator.py already defines,
# used to pick the right default/equipment-hint branch below.
_RUNNING_PHRASE_MAP: dict[str, str] = {
    "LONG_WORKOUT": "long_run",
    "EASY_WEEK_LOAD_BASE": "easy_run",
    "RECOVERY": "recovery_run",
    "LACTATE_THRESHOLD": "tempo_run",
    "THRESHOLD": "tempo_run",
    "TEMPO": "tempo_run",
    "INTERVALS": "intervals",
    "VO2MAX": "intervals",
    "RACE_PACE": "race_pace_long_run",
}


def _session_date(task: dict) -> Optional[str]:
    d = task.get("calendarDate")
    if d:
        return d
    scheduled = ((task.get("taskWorkout") or {}).get("scheduledDate") or "")
    return scheduled[:10] or None


def _equipment_hint(session_type: str, equipment_text: str) -> str:
    """Mirrors plan_generator._build_session's equipment heuristic. Duplicated
    rather than imported — that function is private and does a lot more than
    this one piece; matches the same duplication convention plan_progress.py
    already uses for CYCLING_ACTIVITY_TYPES (see its module docstring)."""
    if session_type == "rest":
        return ""
    if session_type in plan_generator.RUNNING_SESSION_TYPES:
        return "running shoes"
    equipment_lower = (equipment_text or "").lower()
    has_trainer = any(k in equipment_lower for k in ("zwift", "kickr", "trainer", "turbo"))
    if session_type in plan_generator.OUTDOOR_CYCLING_TYPES:
        return "outdoor bike" if any(
            k in equipment_lower for k in ("road", "gravel", "mountain", "domane", "triban")
        ) else ("indoor trainer" if has_trainer else "outdoor bike")
    return "indoor trainer" if has_trainer else ""


def _translate_task(task: dict, discipline: str, equipment_text: str) -> Optional[dict[str, Any]]:
    session_date = _session_date(task)
    if not session_date:
        return None

    tw = task.get("taskWorkout") or {}
    is_rest = bool(tw.get("restDay"))
    phrase = tw.get("workoutPhrase")

    if is_rest:
        session_type = "rest"
    elif discipline == "running":
        session_type = _RUNNING_PHRASE_MAP.get(phrase, _DEFAULT_RUNNING_SESSION_TYPE)
    else:
        session_type = GARMIN_PHRASE_TO_SESSION_TYPE.get(phrase, _DEFAULT_SESSION_TYPE)

    title, description = plan_generator.PLAIN_LABELS.get(
        session_type, (session_type.replace("_", " ").title(), ""),
    )

    duration_s = tw.get("estimatedDurationInSecs")
    target_duration_min = round(duration_s / 60) if duration_s else None

    # Garmin's raw workout name/target (e.g. "Threshold" / "15:00@275W") is kept
    # as secondary detail in `note`, never the headline — established
    # convention here is no raw power numbers in user-facing text
    # (feedback_power_stats.md): the plain-language title/description above are
    # what's shown by default.
    raw_bits = [b for b in (tw.get("workoutName"), tw.get("workoutDescription")) if b]
    note = f"Garmin: {' — '.join(raw_bits)}" if raw_bits else ""

    return {
        "id": f"garmin-{task.get('trainingPlanId', 'plan')}-{session_date}",
        "date": session_date,
        "week_number": task.get("weekId") or 0,
        "phase": "garmin_coach",
        "session_type": session_type,
        "title": title,
        "description": description,
        "target_duration_min": target_duration_min,
        "target_load": None,  # Garmin doesn't expose a TSS-equivalent here
        "equipment_hint": _equipment_hint(session_type, equipment_text),
        "status": "pending",
        "matched_activity_id": None,
        "calendar_event_id": None,
        "note": note,
    }


def _plan_goal(training_plan: dict) -> dict[str, Any]:
    type_key = ((training_plan.get("trainingType") or {}).get("typeKey") or "").lower()
    discipline = "running" if type_key == "running" else "cycling"
    name = training_plan.get("name") or "Garmin Coach Plan"
    desc = training_plan.get("description") or ""
    raw_text = f"{name} — {desc}" if desc else name
    return {
        "raw_text": raw_text,
        "goal_type": "garmin_coach",
        "discipline": discipline,
        "target_date": (training_plan.get("endDate") or "")[:10] or None,
        "projected_peak_ctl": None,
        "feasibility_note": None,
    }


def sync_from_garmin() -> dict[str, Any]:
    """Load the cached Garmin plan, translate + non-destructively merge it into
    plan_store, and return the resulting plan dict (same shape plan_store.load()
    returns). Safe to call often — it's a cheap, cached GCS read plus a save()
    only when something actually needs updating."""
    raw = garmin_plan_store.load_raw()
    current = plan_store.load()
    is_garmin_plan_already = (current.get("goal") or {}).get("goal_type") == "garmin_coach"

    training_plan = (raw or {}).get("training_plan")
    if not training_plan:
        # No Garmin plan synced yet (or none active on the account). If a Garmin
        # plan was previously active, mark it inactive rather than silently
        # leaving stale sessions around; leave a non-Garmin (custom) plan alone.
        if is_garmin_plan_already and current.get("active"):
            updated = {**current, "active": False}
            plan_store.save(updated)
            return updated
        return current

    task_list = training_plan.get("taskList") or []
    if not task_list:
        return current

    import profile as profile_store
    p = profile_store.load()
    equipment_text = p.get("equipment", "")

    goal = _plan_goal(training_plan)
    discipline = goal["discipline"]

    fresh_sessions = [
        s for s in (
            _translate_task(t, discipline, equipment_text) for t in task_list
        ) if s is not None
    ]
    if not fresh_sessions:
        return current
    fresh_by_date = {s["date"]: s for s in fresh_sessions}

    existing_by_date = (
        {s["date"]: s for s in (current.get("sessions") or [])}
        if is_garmin_plan_already else {}
    )

    merged: dict[str, dict] = {}
    for d, fresh in fresh_by_date.items():
        local = existing_by_date.get(d)
        # A date whose local session is already resolved (completed/skipped/
        # manually adjusted) is left untouched — mirrors the "manual completions
        # are left untouched" rule plan_progress._match_sessions already
        # documents. Only "pending" or missing dates pick up the fresh
        # translation, so Garmin adjusting an upcoming session is reflected,
        # but history is never clobbered.
        merged[d] = local if (local and local.get("status") != "pending") else fresh
    merged_sessions = [merged[d] for d in sorted(merged)]

    if is_garmin_plan_already and current.get("baseline", {}).get("captured_on"):
        baseline = current["baseline"]
    else:
        import plan_progress
        baseline = plan_progress.get_baseline_fitness(float(p.get("ftp") or 0))
        baseline = {**baseline, "captured_on": date.today().isoformat()}

    weeks_total = training_plan.get("durationInWeeks") or 0
    phase = {
        "name": "garmin_coach",
        "weeks": weeks_total,
        "load_start": 0,
        "load_end": 0,
    }

    plan = {
        **plan_store.DEFAULTS,
        "active": True,
        "plan_id": f"garmin-{training_plan.get('trainingPlanId')}",
        "created_at": current.get("created_at") if is_garmin_plan_already else datetime.utcnow().isoformat(),
        "goal": goal,
        "baseline": baseline,
        "phases": [phase],
        "sessions": merged_sessions,
        "progress": current.get("progress") if is_garmin_plan_already else plan_store.DEFAULTS["progress"],
        "milestone_state": current.get("milestone_state") if is_garmin_plan_already else plan_store.DEFAULTS["milestone_state"],
    }
    plan_store.save(plan)
    return plan
