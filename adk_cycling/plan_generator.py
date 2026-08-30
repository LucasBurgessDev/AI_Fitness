"""
Deterministic training-plan generator. Pure logic, no I/O — mirrors the
discipline of achievements.py. Callers fetch `baseline` (current fitness
from BigQuery) and pass it in; this module never touches BQ/GCS itself,
so it's trivially unit-testable with fixed inputs.

Produces a periodized plan: phase proportions and weekly-load ramp follow
standard endurance-training heuristics (the classic "10% rule" used
conservatively, a 3:1 build:recover microcycle, and a taper drop before
the goal date) — not sports science novel research, just the well-worn
defaults, applied consistently.

Everything phase/session-type related has a PLAIN_LABELS translation —
internal keys stay technical (base/build/tempo/hill_repeats/...) for the
ramp math, but any string shown to the user or the LLM must go through
PLAIN_LABELS / PHASE_LABELS, never the raw key. This is the app-wide rule
(see feedback_power_stats.md): no phase/TSS/CTL/FTP-zone jargon in output.
"""
from __future__ import annotations

import uuid
from datetime import date, timedelta
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Plain-language translation tables — the ONLY place these mappings live.
# Every other module (agent.py, plan_progress.py, plan.html) must import
# from here rather than re-deriving its own copy.
# ---------------------------------------------------------------------------

PHASE_LABELS: dict[str, str] = {
    "base": "foundation weeks",
    "build": "build weeks",
    "peak": "peak weeks",
    "taper": "taper weeks",
}

PLAIN_LABELS: dict[str, tuple[str, str]] = {
    # key: (title, description)
    "rest":               ("Rest day", "No structured session — full recovery."),
    "recovery":            ("Easy recovery spin", "Very easy, conversational pace. Legs-loosener only."),
    "recovery_run":        ("Easy recovery jog", "Very easy, conversational pace. Shakes out the legs."),
    "endurance":           ("Steady ride", "Comfortable, conversational-pace ride."),
    "easy_run":            ("Easy run", "Comfortable, conversational-pace run."),
    "cross_train":         ("Cross-training / strength", "Any non-cycling/running activity — swim, gym, yoga."),
    "tempo":               ("Moderate effort ride", "A bit of a push — sustainably hard, not all-out."),
    "tempo_run":           ("Moderate effort run", "A bit of a push — sustainably hard, not all-out."),
    "sweet_spot":          ("Hard effort ride", "Sustained hard effort — the main quality session of the week."),
    "hill_repeats":        ("Hill effort", "Repeated hard climbs with easy recovery between."),
    "intervals":           ("Interval session", "Short, hard efforts with recovery between."),
    "long_endurance":      ("Long ride", "The week's longest ride, at a comfortable, sustainable pace."),
    "long_run":            ("Long run", "The week's longest run, at a comfortable, sustainable pace."),
    "race_pace_long":      ("Goal-pace long ride", "Long ride with stretches at your target event pace."),
    "race_pace_long_run":  ("Goal-pace long run", "Long run with stretches at your target event pace."),
    "openers":             ("Easy sharpening spin", "Short and easy with a few brief quick efforts — keeps the legs sharp."),
    "short_long":          ("Shorter steady ride", "A shorter version of your long ride — easing the load."),
    "short_run":           ("Shorter steady run", "A shorter version of your long run — easing the load."),
}

# ---------------------------------------------------------------------------
# Weekly templates — index 0 = Monday ... index 6 = Sunday. Placement onto
# weekday vs weekend IS the "day-of-week assignment" step: long/hard
# sessions are already positioned at weekend slots, easy/rest on weekdays,
# consistent with the "Working Day" rules already in system_prompt.txt.
# ---------------------------------------------------------------------------

WEEKLY_TEMPLATES: dict[tuple[str, str], list[str]] = {
    ("cycling", "base"):  ["rest", "endurance", "recovery", "endurance", "rest", "long_endurance", "endurance"],
    ("cycling", "build"): ["rest", "sweet_spot", "endurance", "hill_repeats", "rest", "long_endurance", "recovery"],
    ("cycling", "peak"):  ["rest", "intervals", "endurance", "rest", "rest", "race_pace_long", "recovery"],
    ("cycling", "taper"): ["rest", "endurance", "recovery", "rest", "openers", "short_long", "rest"],

    ("running", "base"):  ["rest", "easy_run", "cross_train", "easy_run", "rest", "long_run", "easy_run"],
    ("running", "build"): ["rest", "tempo_run", "easy_run", "intervals", "rest", "long_run", "recovery_run"],
    ("running", "peak"):  ["rest", "intervals", "easy_run", "rest", "rest", "race_pace_long_run", "recovery_run"],
    ("running", "taper"): ["rest", "easy_run", "recovery_run", "rest", "openers", "short_run", "rest"],
}

# Relative share of the week's target load, per session type — normalized
# across whichever non-rest slots a given week's template actually has.
_LOAD_WEIGHTS: dict[str, float] = {
    "recovery": 0.05, "recovery_run": 0.05,
    "endurance": 0.20, "easy_run": 0.20, "cross_train": 0.10,
    "tempo": 0.20, "tempo_run": 0.20, "sweet_spot": 0.20,
    "hill_repeats": 0.20, "intervals": 0.20,
    "long_endurance": 0.35, "long_run": 0.35,
    "race_pace_long": 0.35, "race_pace_long_run": 0.35,
    "openers": 0.10, "short_long": 0.15, "short_run": 0.15,
}

# Intensity-factor assumption per session type, used to back-calculate a
# plausible duration from the session's share of the week's target load,
# via the same TSS-shaped formula the rest of the app already uses:
#   load = (duration_hr * IF^2) * 100  =>  duration_hr = load / (IF^2 * 100)
_INTENSITY_FACTOR: dict[str, float] = {
    "recovery": 0.55, "recovery_run": 0.55,
    "endurance": 0.65, "easy_run": 0.65, "cross_train": 0.60,
    "tempo": 0.75, "tempo_run": 0.75,
    "sweet_spot": 0.88, "hill_repeats": 0.85, "intervals": 0.95,
    "long_endurance": 0.68, "long_run": 0.68,
    "race_pace_long": 0.80, "race_pace_long_run": 0.80,
    "openers": 0.65, "short_long": 0.68, "short_run": 0.68,
}

RUNNING_SESSION_TYPES = {
    "easy_run", "tempo_run", "intervals", "long_run", "recovery_run",
    "race_pace_long_run", "short_run",
}

# Session types that imply outdoor riding — used by plan_progress's weather
# logic to decide whether a swap is even relevant.
OUTDOOR_CYCLING_TYPES = {"endurance", "sweet_spot", "hill_repeats", "long_endurance", "race_pace_long", "short_long"}

_RAMP_PCT_PER_WEEK = 0.08          # conservative "10% rule"
_RECOVERY_WEEK_FACTOR = 0.6        # every 4th week within base/build
_TAPER_END_FACTOR = 0.45           # taper drops to ~45% of peak load
_OVERALL_RAMP_CAP = 1.6            # whole-plan load never exceeds 1.6x the start


def _phase_proportions(weeks_total: int) -> list[tuple[str, int]]:
    """Return [(phase_name, weeks), ...] for the given total plan length."""
    if weeks_total <= 1:
        return [("peak", max(weeks_total, 1))]
    if weeks_total == 2:
        return [("build", 1), ("taper", 1)]
    if weeks_total <= 5:
        props = [("build", 0.45), ("peak", 0.30), ("taper", 0.25)]
    elif weeks_total <= 11:
        props = [("base", 0.30), ("build", 0.40), ("peak", 0.15), ("taper", 0.15)]
    else:
        props = [("base", 0.40), ("build", 0.35), ("peak", 0.10), ("taper", 0.15)]

    weeks = [max(1, round(weeks_total * p)) for _, p in props]
    diff = weeks_total - sum(weeks)
    if diff != 0:
        idx_largest = weeks.index(max(weeks))
        weeks[idx_largest] = max(1, weeks[idx_largest] + diff)
    return [(name, w) for (name, _), w in zip(props, weeks)]


def _phase_load_bounds(
    phase_names_weeks: list[tuple[str, int]], starting_load: float,
) -> list[dict[str, Any]]:
    """Compute load_start/load_end per phase, capped at the overall ramp ceiling."""
    cap = starting_load * _OVERALL_RAMP_CAP
    phases: list[dict[str, Any]] = []
    prev_end = starting_load
    for name, weeks in phase_names_weeks:
        if name in ("base", "build"):
            load_start = prev_end
            load_end = min(load_start * ((1 + _RAMP_PCT_PER_WEEK) ** weeks), cap)
        elif name == "peak":
            load_start = prev_end
            load_end = load_start  # hold flat — intensity shifts via template, not volume
        else:  # taper
            load_start = prev_end
            load_end = load_start * _TAPER_END_FACTOR
        phases.append({"name": name, "weeks": weeks, "load_start": round(load_start, 1), "load_end": round(load_end, 1)})
        prev_end = load_end
    return phases


def _week_target_load(phase: dict, week_idx_in_phase: int, global_week_number: int) -> float:
    """Linear interpolation of this week's target load within its phase, with a
    down-week (recovery) applied every 4th global week inside base/build."""
    weeks = phase["weeks"]
    frac = week_idx_in_phase / max(weeks - 1, 1)
    load = phase["load_start"] + (phase["load_end"] - phase["load_start"]) * frac
    if phase["name"] in ("base", "build") and global_week_number % 4 == 0:
        load *= _RECOVERY_WEEK_FACTOR
    return round(load, 1)


def _build_session(
    session_date: date, phase_name: str, week_number: int, session_type: str,
    week_load: float, load_weight_total: float, discipline: str, equipment_text: str,
) -> dict[str, Any]:
    title, description = PLAIN_LABELS.get(session_type, (session_type.replace("_", " ").title(), ""))
    if session_type == "rest":
        return {
            "id": f"s-{session_date.isoformat()}-{uuid.uuid4().hex[:6]}",
            "date": session_date.isoformat(),
            "week_number": week_number,
            "phase": phase_name,
            "session_type": "rest",
            "title": title,
            "description": description,
            "target_duration_min": None,
            "target_load": None,
            "equipment_hint": "",
            "status": "pending",
            "matched_activity_id": None,
            "calendar_event_id": None,
            "note": "",
        }

    share = _LOAD_WEIGHTS.get(session_type, 0.15) / load_weight_total if load_weight_total > 0 else 0
    session_load = round(week_load * share, 1)
    intensity = _INTENSITY_FACTOR.get(session_type, 0.7)
    duration_hr = session_load / (intensity ** 2 * 100) if session_load > 0 else 0.5
    duration_min = max(20, round(duration_hr * 60))

    equipment_lower = (equipment_text or "").lower()
    has_trainer = any(k in equipment_lower for k in ("zwift", "kickr", "trainer", "turbo"))
    if session_type in OUTDOOR_CYCLING_TYPES:
        equipment_hint = "outdoor bike" if any(
            k in equipment_lower for k in ("road", "gravel", "mountain", "domane", "triban")
        ) else ("indoor trainer" if has_trainer else "outdoor bike")
    elif session_type in RUNNING_SESSION_TYPES:
        equipment_hint = "running shoes"
    else:
        equipment_hint = ""

    return {
        "id": f"s-{session_date.isoformat()}-{uuid.uuid4().hex[:6]}",
        "date": session_date.isoformat(),
        "week_number": week_number,
        "phase": phase_name,
        "session_type": session_type,
        "title": title,
        "description": description,
        "target_duration_min": duration_min,
        "target_load": session_load,
        "equipment_hint": equipment_hint,
        "status": "pending",
        "matched_activity_id": None,
        "calendar_event_id": None,
        "note": "",
    }


def generate_plan(
    goal_text: str,
    goal_type: str,
    discipline: str,
    target_date: date,
    start_date: date,
    baseline: dict[str, Any],
    equipment_text: str = "",
) -> dict[str, Any]:
    """Build a full periodized plan from a goal + current fitness baseline.

    Args:
        goal_text: Free text as the user stated it, e.g. "sub-5:00 century".
        goal_type: "event_time" | "ftp_target" | "distance_pb" | "general_fitness".
        discipline: "cycling" | "running".
        target_date: The event/deadline date.
        start_date: Usually today.
        baseline: {"ctl": float, "atl": float, "ftp_w": float,
                   "weekly_tss_avg_4wk": float, "weekly_km_avg_4wk": float}
                  — any missing key is treated as 0.
        equipment_text: profile.equipment free text, for coarse gear matching.

    Returns:
        {"goal": {...}, "baseline": {...}, "phases": [...], "sessions": [...]}
        ready for plan_store.save(). Never raises for an unrealistic/tight
        timeline — instead attaches goal["feasibility_note"].
    """
    discipline = discipline if discipline in ("cycling", "running") else "cycling"
    weeks_total = max((target_date - start_date).days // 7, 0)

    feasibility_note = None
    if weeks_total < 3:
        feasibility_note = (
            "That's a tight timeline — I've built the most realistic short plan I can, "
            "but temper expectations on hitting an ambitious target this fast."
        )

    starting_load = (
        max(float(baseline.get("weekly_tss_avg_4wk") or 0), 150.0)
        if discipline == "cycling"
        else max(float(baseline.get("weekly_km_avg_4wk") or 0), 15.0)
    )

    phase_weeks = _phase_proportions(weeks_total)
    phases = _phase_load_bounds(phase_weeks, starting_load)

    # Flag (rather than silently ignore) an unrealistic ask that hit the ramp cap.
    if phases and phases[-2 if len(phases) > 1 else -1]["load_end"] >= starting_load * _OVERALL_RAMP_CAP - 0.1:
        feasibility_note = feasibility_note or (
            "Your target looks ambitious given your current fitness — I've built the "
            "steepest safe progression I can, but be ready to adjust expectations."
        )

    sessions: list[dict[str, Any]] = []
    global_week_number = 0
    cursor = start_date
    for phase in phases:
        template = WEEKLY_TEMPLATES.get((discipline, phase["name"]), WEEKLY_TEMPLATES[("cycling", "build")])
        load_weight_total = sum(_LOAD_WEIGHTS.get(t, 0.15) for t in template if t != "rest") or 1.0

        for week_idx in range(phase["weeks"]):
            global_week_number += 1
            week_load = _week_target_load(phase, week_idx, global_week_number)
            week_start = cursor
            for day_offset in range(7):
                session_date = week_start + timedelta(days=day_offset)
                if session_date > target_date:
                    break
                session_type = template[session_date.weekday()]
                sessions.append(_build_session(
                    session_date, phase["name"], global_week_number, session_type,
                    week_load, load_weight_total, discipline, equipment_text,
                ))
            cursor = week_start + timedelta(days=7)

    projected_peak_ctl = None
    if discipline == "cycling" and phases:
        # The highest sustained load reached, not the tapered-down end value —
        # matters for short plans that have no explicit "peak" phase, where a
        # naive "last phase" lookup would land on taper's deliberately-reduced load.
        non_taper = [p for p in phases if p["name"] != "taper"]
        peak_load = max((p["load_end"] for p in non_taper), default=phases[-1]["load_end"])
        projected_peak_ctl = round(peak_load / 7, 1)

    goal = {
        "raw_text": goal_text,
        "goal_type": goal_type,
        "discipline": discipline,
        "target_date": target_date.isoformat(),
        "projected_peak_ctl": projected_peak_ctl,
        "feasibility_note": feasibility_note,
    }
    baseline_out = {
        "captured_on": start_date.isoformat(),
        "ctl": baseline.get("ctl"),
        "atl": baseline.get("atl"),
        "ftp_w": baseline.get("ftp_w"),
        "weekly_tss_avg_4wk": baseline.get("weekly_tss_avg_4wk"),
        "weekly_km_avg_4wk": baseline.get("weekly_km_avg_4wk"),
    }
    return {"goal": goal, "baseline": baseline_out, "phases": phases, "sessions": sessions}
