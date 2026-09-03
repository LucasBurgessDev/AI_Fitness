"""
Read-only BigQuery progress computation + weather/equipment session-suggestion
logic for the training plan feature.

Synchronous throughout (see weather.py's module docstring for why) — safe to
call directly from agent.py's plain-`def` tool functions, and from app.py's
async routes via asyncio.to_thread(...) for the BQ-heavy calls, matching the
existing pattern in app.py's _compute_goals_data.

Session matching, weekly rollups, and milestone detection are pure functions
(no I/O) wherever practical, mirroring achievements.py's discipline — the
I/O-performing wrappers (get_baseline_fitness, get_plan_progress,
suggest_next_session) are thin shells around them.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from typing import Any, Optional

import plan_generator
import plan_store

LOGGER = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID", "health-data-482722")

# Garmin's own activityType.typeKey strings, exactly as already filtered on
# elsewhere in this app (app.py's _compute_goals_data). Duplicated here
# rather than imported, since app.py's copy is baked into raw SQL string
# fragments — not worth the risk of touching working, already-deployed code
# for a pure refactor.
CYCLING_ACTIVITY_TYPES = {
    "cycling", "road_cycling", "gravel_cycling", "mountain_biking",
    "indoor_cycling", "virtual_ride", "spinning",
}
RUNNING_ACTIVITY_TYPES = {"running", "treadmill_running", "trail_running"}


def _discipline_activity_types(discipline: str) -> set[str]:
    return CYCLING_ACTIVITY_TYPES if discipline == "cycling" else RUNNING_ACTIVITY_TYPES


def _session_discipline(session_type: str) -> str:
    return "running" if session_type in plan_generator.RUNNING_SESSION_TYPES else "cycling"


def get_baseline_fitness(ftp_watts: float) -> dict[str, Any]:
    """Fetch current CTL/ATL plus 4-week average weekly load, for plan generation
    and ongoing progress comparison. Reuses agent._build_training_load_cte so the
    ATL/CTL math is never duplicated."""
    from google.cloud import bigquery
    import agent as agent_mod
    import bq_cache

    client = bigquery.Client(project=PROJECT_ID)

    cte = agent_mod._build_training_load_cte(weeks=1, ftp_watts=ftp_watts)
    load_sql = f"{cte}\nSELECT ctl, atl FROM with_load ORDER BY date DESC LIMIT 1"
    load_rows = bq_cache.query(client, load_sql)
    ctl = load_rows[0]["ctl"] if load_rows else None
    atl = load_rows[0]["atl"] if load_rows else None

    cycling_list = ", ".join(f"'{t}'" for t in CYCLING_ACTIVITY_TYPES)
    running_list = ", ".join(f"'{t}'" for t in RUNNING_ACTIVITY_TYPES)
    vol_sql = f"""
        SELECT
            ROUND(SUM(CASE WHEN activity_type IN ({cycling_list}) THEN COALESCE(tss, 0) ELSE 0 END) / 4.0, 1)
                AS weekly_tss_avg_4wk,
            ROUND(SUM(CASE WHEN activity_type IN ({running_list}) THEN COALESCE(distance_m, 0) ELSE 0 END) / 4000.0, 1)
                AS weekly_km_avg_4wk
        FROM `{PROJECT_ID}.garmin.garmin_activities`
        WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY))
    """
    vol_rows = bq_cache.query(client, vol_sql)
    vol = vol_rows[0] if vol_rows else {}

    return {
        "ctl": ctl,
        "atl": atl,
        "ftp_w": ftp_watts,
        "weekly_tss_avg_4wk": vol.get("weekly_tss_avg_4wk") or 0,
        "weekly_km_avg_4wk": vol.get("weekly_km_avg_4wk") or 0,
    }


def _close_out_previous_plan(old_plan: dict, new_plan: dict, email: str) -> Optional[str]:
    """If an old plan is active, log a continuity note to coaching_log describing the
    transition, and return a plain-language summary the caller can weave into its own
    response — so switching goals never just silently erases the old one.

    Critical: the log entry must state that a NEW plan is now active, not just that the
    old one ended — a chat session's auto-injected coaching-log preamble (see agent.py's
    _prepare_session) reads entries like this out of context, and a message that only
    says "wrapped up X" reads as "no plan exists now" to a model skimming it, causing it
    to tell the user there's no active plan even though one was just created. (Root
    cause of a real incident: confirmed via the actual chat transcript and the exact
    coaching_log row it was reading — see git history for this comment for detail.)

    Mirrors the achievement/milestone logging convention (system-training-plan
    session_id, goal_progress category) already used elsewhere in this feature.
    """
    if not old_plan.get("active"):
        return None

    old_goal = old_plan.get("goal") or {}
    progress = old_plan.get("progress") or {}
    adherence = progress.get("overall_adherence_pct")
    trajectory_phrase = {
        "ahead": "ahead of pace", "on_track": "on track", "behind": "a bit behind pace",
    }.get(progress.get("trajectory_status"))

    stats_bits = []
    if adherence is not None:
        stats_bits.append(f"{int(adherence)}% of sessions completed")
    if trajectory_phrase:
        stats_bits.append(trajectory_phrase)
    stats = f" ({', '.join(stats_bits)})" if stats_bits else ""

    new_goal = new_plan.get("goal") or {}
    summary = (
        f'Replaced your goal of "{old_goal.get("raw_text", "your last plan")}"{stats} '
        f'with a new active plan: "{new_goal.get("raw_text", "")}" '
        f'(target {new_goal.get("target_date", "")}).'
    )

    try:
        import json as _json
        import coaching_log
        coaching_log.save_insight(
            PROJECT_ID, "system-training-plan", email,
            category="goal_progress",
            content=summary,
            context=_json.dumps({"old_goal": old_goal, "old_progress": progress, "new_goal": new_goal}),
        )
    except Exception as exc:
        LOGGER.warning("Could not log outgoing plan summary: %s", exc)
    return summary


def build_and_save_plan(goal_text: str, target_date: date, discipline: str = "cycling", email: str = "") -> dict:
    """Fetch profile + current fitness, generate a plan, and save it as the active plan.

    This is also how an existing plan gets adjusted/reformulated — a new goal always
    fully replaces the old one (re-baselined from current fitness, not the original
    plan's numbers), rather than trying to patch specific fields in place. If a plan
    was already active, a short continuity note is logged and returned in
    "outgoing_summary" so callers can tell the user what happened to it.

    Shared by the chat tool (agent.create_training_plan) and the /api/plan/create
    route so plan creation/adjustment has exactly one implementation.

    Returns:
        The full saved plan dict (goal/baseline/phases/sessions/progress/milestone_state)
        plus a transient "outgoing_summary" key (not persisted) — None if no plan was
        replaced.
    """
    import profile as profile_store

    discipline = discipline if discipline in ("cycling", "running") else "cycling"
    p = profile_store.load()
    ftp = float(p.get("ftp") or 0)
    baseline = get_baseline_fitness(ftp)

    old_plan = plan_store.load()

    goal_type = "ftp_target" if "ftp" in goal_text.lower() and discipline == "cycling" else "event_time"
    generated = plan_generator.generate_plan(
        goal_text=goal_text, goal_type=goal_type, discipline=discipline,
        target_date=target_date, start_date=date.today(), baseline=baseline,
        equipment_text=p.get("equipment", ""),
    )
    plan = {
        **plan_store.DEFAULTS,
        **generated,
        "active": True,
        "plan_id": f"plan-{date.today().isoformat()}",
        "created_at": datetime.utcnow().isoformat(),
    }

    # Log the transition (old goal's fate + new goal's identity) *after* the new plan
    # is built, so the one coaching_log entry unambiguously states a plan IS active now.
    outgoing_summary = _close_out_previous_plan(old_plan, plan, email)

    plan_store.save(plan)
    return {**plan, "outgoing_summary": outgoing_summary}


def _match_sessions(sessions: list[dict], activities: list[dict], today_iso: str) -> list[dict]:
    """Pure function: diff planned sessions against actual Garmin activities.

    Manual completions (status already "completed"/"skipped" with note="manual")
    are left untouched — this sweep only ever mutates sessions still "pending".
    """
    by_date: dict[str, list[dict]] = {}
    for a in activities:
        by_date.setdefault(a.get("date"), []).append(a)

    updated: list[dict] = []
    for s in sessions:
        if s.get("status") != "pending" or s.get("session_type") == "rest":
            updated.append(s)
            continue
        if s["date"] > today_iso:
            updated.append(s)
            continue

        candidates = by_date.get(s["date"], [])
        if not candidates:
            updated.append({**s, "status": "missed"} if s["date"] < today_iso else s)
            continue

        wanted_types = _discipline_activity_types(_session_discipline(s["session_type"]))
        exact = [a for a in candidates if a.get("activity_type") in wanted_types]
        pool = exact or candidates
        best = max(pool, key=lambda a: a.get("duration_s") or 0)
        new_s = {**s, "status": "completed", "matched_activity_id": best.get("activity_id")}
        if not exact:
            new_s["note"] = "substituted"
        updated.append(new_s)
    return updated


def _weekly_rollup(sessions: list[dict], baseline_ctl: Optional[float], projected_peak_ctl: Optional[float],
                    ctl_by_date: dict[str, float]) -> list[dict]:
    """Group sessions by ISO week (Monday start), compute planned-vs-actual load
    and a plain-language on-track/ahead/behind read against a linear CTL target
    line from baseline_ctl to projected_peak_ctl."""
    by_week: dict[str, dict[str, Any]] = {}
    dated_sessions = sorted((s for s in sessions if s.get("session_type") != "rest"), key=lambda s: s["date"])
    if not dated_sessions:
        return []

    first_date = date.fromisoformat(dated_sessions[0]["date"])
    last_date = date.fromisoformat(dated_sessions[-1]["date"])
    total_days = max((last_date - first_date).days, 1)

    for s in dated_sessions:
        d = date.fromisoformat(s["date"])
        week_start = (d - timedelta(days=d.weekday())).isoformat()
        wk = by_week.setdefault(week_start, {"planned_load": 0.0, "actual_load": 0.0, "planned": 0, "completed": 0})
        wk["planned_load"] += s.get("target_load") or 0
        wk["planned"] += 1
        if s.get("status") == "completed":
            wk["completed"] += 1
            wk["actual_load"] += s.get("target_load") or 0  # approximation: credit planned load when completed

    weeks_out = []
    for week_start in sorted(by_week):
        wk = by_week[week_start]
        adherence = round(100 * wk["completed"] / wk["planned"], 0) if wk["planned"] else None
        ctl_target = None
        if baseline_ctl is not None and projected_peak_ctl is not None:
            days_in = (date.fromisoformat(week_start) - first_date).days
            frac = min(max(days_in / total_days, 0), 1)
            ctl_target = round(baseline_ctl + (projected_peak_ctl - baseline_ctl) * frac, 1)
        weeks_out.append({
            "week_start": week_start,
            "planned_sessions": wk["planned"],
            "completed_sessions": wk["completed"],
            "planned_load": round(wk["planned_load"], 1),
            "actual_load": round(wk["actual_load"], 1),
            "adherence_pct": adherence,
            "ctl_actual": ctl_by_date.get(week_start),
            "ctl_target": ctl_target,
        })
    return weeks_out


def _trajectory_status(weeks: list[dict]) -> Optional[str]:
    recent = [w for w in weeks if w.get("ctl_actual") is not None and w.get("ctl_target") is not None]
    if not recent:
        return None
    latest = recent[-1]
    diff = latest["ctl_actual"] - latest["ctl_target"]
    if diff > 3:
        return "ahead"
    if diff < -3:
        return "behind"
    return "on_track"


def get_plan_progress() -> dict[str, Any]:
    """Load the active plan, match sessions against real activities, compute
    weekly adherence + fitness trajectory, persist the update, and return a
    plain dict ready for both the chat tool and the /plan page."""
    plan = plan_store.load()
    if not plan.get("active"):
        return {"active": False}

    from google.cloud import bigquery
    import agent as agent_mod
    import bq_cache

    client = bigquery.Client(project=PROJECT_ID)
    # Phases only store week-counts, not absolute dates (see _current_phase) —
    # the plan's real start date is the earliest session date, falling back to
    # when the baseline fitness snapshot was captured.
    session_dates = [s["date"] for s in plan.get("sessions", [])]
    start_date = min(session_dates) if session_dates else plan["baseline"]["captured_on"]
    activities_sql = f"""
        SELECT date, activity_id, activity_type, duration_s, tss
        FROM `{PROJECT_ID}.garmin.garmin_activities`
        WHERE date >= '{start_date}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY activity_id ORDER BY run_date DESC) = 1
    """
    activities = bq_cache.query(client, activities_sql)

    ftp_w = float(plan.get("baseline", {}).get("ftp_w") or 0)
    weeks_span = max((date.today() - date.fromisoformat(start_date)).days // 7 + 1, 1)
    cte = agent_mod._build_training_load_cte(weeks=weeks_span, ftp_watts=ftp_w)
    ctl_sql = f"{cte}\nSELECT date, ctl FROM with_load WHERE date >= '{start_date}' ORDER BY date"
    ctl_rows = bq_cache.query(client, ctl_sql)
    ctl_by_date = {r["date"]: r["ctl"] for r in ctl_rows}
    # Index CTL by week-start too, for the weekly rollup join.
    ctl_by_week: dict[str, float] = {}
    for r in ctl_rows:
        d = date.fromisoformat(r["date"])
        wk = (d - timedelta(days=d.weekday())).isoformat()
        ctl_by_week[wk] = r["ctl"]  # last value in the week wins (most recent)

    today_iso = date.today().isoformat()
    updated_sessions = _match_sessions(plan["sessions"], activities, today_iso)
    weeks = _weekly_rollup(
        updated_sessions,
        baseline_ctl=plan.get("baseline", {}).get("ctl"),
        projected_peak_ctl=plan.get("goal", {}).get("projected_peak_ctl"),
        ctl_by_date=ctl_by_week,
    )
    overall = None
    if weeks:
        pct_vals = [w["adherence_pct"] for w in weeks if w["adherence_pct"] is not None]
        overall = round(sum(pct_vals) / len(pct_vals), 0) if pct_vals else None

    progress = {
        "last_computed_at": datetime.utcnow().isoformat(),
        "weeks": weeks,
        "overall_adherence_pct": overall,
        "trajectory_status": _trajectory_status(weeks),
    }
    plan_store.save({**plan, "sessions": updated_sessions, "progress": progress})

    return {
        "active": True,
        "goal": plan["goal"],
        "current_phase": _current_phase(plan, today_iso),
        "progress": progress,
    }


def _current_phase(plan: dict, today_iso: str) -> Optional[dict]:
    """Find which phase 'today' falls into, using the plan's own session dates —
    the phases list only stores week-counts, not absolute start/end dates."""
    for phase in plan.get("phases", []):
        phase_dates = [s["date"] for s in plan["sessions"] if s["phase"] == phase["name"]]
        if phase_dates and min(phase_dates) <= today_iso <= max(phase_dates):
            return {"name": phase["name"], "label": plan_generator.PHASE_LABELS.get(phase["name"], phase["name"])}
    return None


def suggest_next_session(equipment_text: str, location: dict[str, Any]) -> dict[str, Any]:
    """Return today's (or the next pending) planned session, adjusted for the
    weather forecast and available equipment. Pure decision rules, no LLM call."""
    plan = plan_store.load()
    if not plan.get("active"):
        return {"active": False}

    today_iso = date.today().isoformat()
    pending = [s for s in plan["sessions"] if s["status"] == "pending" and s["date"] >= today_iso]
    if not pending:
        return {"active": True, "session": None, "note": "No upcoming sessions in the plan."}
    session = min(pending, key=lambda s: s["date"])

    if session["session_type"] == "rest":
        return {"active": True, "session": session, "adjusted": False}

    import weather as weather_mod
    lat, lon = location.get("lat"), location.get("lon")
    forecast = weather_mod.get_forecast(lat, lon, days=3) if lat is not None and lon is not None else {}
    day_forecast = (forecast.get("days") or {}).get(session["date"])

    adjusted = dict(session)
    swap_reason = None
    if day_forecast:
        precip = day_forecast.get("precip_prob_pct") or 0
        temp_min = day_forecast.get("temp_min_c")
        wind = day_forecast.get("wind_kph") or 0
        is_precision = session["session_type"] in (
            "sweet_spot", "hill_repeats", "intervals", "tempo", "tempo_run",
        )
        threshold = 40 if is_precision else 70
        bad_weather = (
            precip >= threshold
            or (temp_min is not None and temp_min < 2)
            or wind > 45
        )
        if bad_weather and session["session_type"] in plan_generator.OUTDOOR_CYCLING_TYPES:
            equipment_lower = (equipment_text or "").lower()
            has_trainer = any(k in equipment_lower for k in ("zwift", "kickr", "trainer", "turbo"))
            if has_trainer:
                adjusted["equipment_hint"] = "indoor trainer"
                adjusted["note"] = "Moved indoors — " + day_forecast["description"]
                swap_reason = f"weather: {day_forecast['description']}"
            else:
                adjusted["note"] = (
                    f"Forecast looks rough ({day_forecast['description']}) and there's no indoor "
                    "trainer on file — consider easing the effort or swapping to another day this week."
                )
                swap_reason = f"weather (no indoor option): {day_forecast['description']}"

    return {
        "active": True,
        "session": session,
        "adjusted_session": adjusted,
        "swapped": swap_reason is not None,
        "swap_reason": swap_reason,
    }


def evaluate_plan_milestones(plan: dict[str, Any], state: dict[str, Any] | None) -> tuple[list[dict], dict]:
    """Pure function, mirrors achievements.py's exact seeding discipline: on the
    first call (state not yet initialized) bests/hits are recorded silently with
    no events emitted, so shipping this feature never retroactively celebrates
    progress that already happened before it existed."""
    state = state or {}
    seeding = not state.get("initialized")
    phase_hits = dict(state.get("phase_hits", {}))
    events: list[dict] = []

    if not plan.get("active"):
        return [], {"initialized": True, "phase_hits": phase_hits}

    today_iso = date.today().isoformat()
    for phase in plan.get("phases", []):
        phase_sessions = [s for s in plan["sessions"] if s["phase"] == phase["name"] and s["session_type"] != "rest"]
        if not phase_sessions:
            continue
        all_past = all(s["date"] <= today_iso for s in phase_sessions)
        completed = sum(1 for s in phase_sessions if s["status"] == "completed")
        is_complete = all_past and completed >= max(1, int(0.6 * len(phase_sessions)))
        already_hit = phase_hits.get(phase["name"], False)
        if is_complete and not already_hit and not seeding:
            events.append({
                "type": "phase_complete",
                "phase": phase["name"],
                "label": plan_generator.PHASE_LABELS.get(phase["name"], phase["name"]),
                "completed": completed,
                "total": len(phase_sessions),
            })
        if is_complete:
            phase_hits[phase["name"]] = True

    return events, {"initialized": True, "phase_hits": phase_hits}
