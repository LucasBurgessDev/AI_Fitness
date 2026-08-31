from __future__ import annotations

import logging
import os
import threading
import time

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools import FunctionTool
from google.cloud import bigquery
from google.genai.types import Content, Part

import bq_cache
import profile as profile_store

LOGGER = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID", "health-data-482722")

_APP_NAME = "health_coach"
_session_service = InMemorySessionService()

# Per-session runner cache: session_id → (runner, profile_snapshot)
_runners: dict[str, tuple[Runner, dict]] = {}

_SYSTEM_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "system_prompt.txt")


# ---------------------------------------------------------------------------
# BigQuery function tools — used instead of MCP to avoid asyncio scope issues
# ---------------------------------------------------------------------------

def query_garmin_data(sql: str, safe: bool = False) -> str:
    """Execute a SQL query against the garmin BigQuery dataset and return results.

    Args:
        sql: A valid BigQuery SQL query. Available tables:
             - `garmin.garmin_stats`: daily biometrics (weight, sleep, HRV, VO2 max, steps, etc.)
             - `garmin.garmin_activities`: activities (distance, duration, HR, calories). Power,
               normalized power, TSS, and FTP columns only have values for indoor cycling/Zwift
               rides — don't select or surface them unless the user is specifically asking about
               a cycling workout.
             Both tables are in project health-data-482722 and partitioned on run_date (DATE).
        safe: if True, retries with NULL substituted for any column BigQuery reports
              as not existing yet (see bq_cache.patch_missing_column) instead of
              failing outright. Only pass this for fixed, single-table SELECT
              queries we control — not arbitrary caller-authored SQL.

    Returns:
        Query results as a formatted string, or an error message.
    """
    cached = bq_cache.get(sql)
    if cached is not None:
        LOGGER.debug("BQ cache hit")
        return cached

    client = bigquery.Client(project=PROJECT_ID)
    attempt_sql = sql
    for _ in range(12 if safe else 1):
        try:
            results = client.query(attempt_sql).result()
            rows = [dict(row) for row in results]
            if not rows:
                result = "Query returned no results."
            else:
                # Format as a readable table summary
                result = "\n".join(str(row) for row in rows)
            bq_cache.put(sql, result)
            return result
        except Exception as e:
            patched = bq_cache.patch_missing_column(attempt_sql, e) if safe else None
            if not patched:
                LOGGER.error("BigQuery query error: %s | SQL: %s", e, attempt_sql)
                return f"Query error: {e}"
            LOGGER.warning("garmin_stats column not backfilled yet, using NULL: %s", e)
            attempt_sql = patched
    return "Query error: too many unrecognized columns in query"


def get_recent_activities(days: int = 30, activity_type: str = "") -> str:
    """Fetch recent activities from BigQuery.

    Args:
        days: Number of days to look back (default 30).
        activity_type: Optional filter e.g. 'cycling', 'road_cycling', 'virtual_ride'.
                       Leave empty for all activity types.

    Returns:
        Recent activities with key metrics as a formatted string.
    """
    type_filter = f"AND activity_type = '{activity_type}'" if activity_type else ""
    sql = f"""
        SELECT date, title, activity_type, duration_s, distance_m,
               avg_power_w, normalized_power_w, tss, ftp_watts,
               avg_hr, max_hr, elevation_gain_m, calories
        FROM `{PROJECT_ID}.garmin.garmin_activities`
        WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY))
        {type_filter}
        ORDER BY date DESC
        LIMIT 50
    """
    return query_garmin_data(sql)


def get_recent_stats(days: int = 30) -> str:
    """Fetch recent daily biometric stats from BigQuery.

    Args:
        days: Number of days to look back (default 30).

    Returns:
        Daily stats including weight, body composition, sleep, HRV, stress, hydration,
        overnight recovery (HR range, stress, Body Battery change, naps), and more as a formatted string.
    """
    sql = f"""
        SELECT date, timestamp, weight_lbs, muscle_mass_lbs, body_fat_pct, water_pct,
               sleep_total_hr, sleep_deep_hr, sleep_rem_hr, sleep_score,
               rhr, min_hr, max_hr, avg_stress, body_battery, respiration, spo2,
               vo2_max, training_status, hrv_status, hrv_avg,
               steps, step_goal, cals_total, cals_active, cals_goal, activities,
               hydration_ml, hydration_goal_ml, bb_change_overnight,
               overnight_hr_avg, overnight_hr_min, overnight_hr_max, overnight_stress_avg,
               nap_bb_gain, nap_count
        FROM `{PROJECT_ID}.garmin.garmin_stats`
        WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY timestamp DESC) = 1
        ORDER BY date DESC
        LIMIT 60
    """
    return query_garmin_data(sql, safe=True)


def get_intraday_stats(date: str = "") -> str:
    """Fetch all intra-day readings for a specific date to track how metrics evolved through the day.

    The pipeline runs every 30 minutes, so there can be up to ~35 rows per day. This tool
    returns every row in timestamp order — useful for seeing body battery drain through the day,
    stress spikes after specific events, step accumulation, or how HRV/respiration changed.

    Args:
        date: Date in YYYY-MM-DD format. Defaults to today if empty.

    Returns:
        All intra-day readings ordered by time, showing progression of body battery, stress,
        steps, HRV, and other metrics through the day.
    """
    date_expr = f"'{date}'" if date else "FORMAT_DATE('%Y-%m-%d', CURRENT_DATE())"
    sql = f"""
        SELECT timestamp, avg_stress, body_battery, steps, cals_total, cals_active,
               rhr, min_hr, max_hr, respiration, spo2, hrv_status, hrv_avg,
               sleep_total_hr, sleep_score, weight_lbs, activities
        FROM `{PROJECT_ID}.garmin.garmin_stats`
        WHERE date = {date_expr}
        ORDER BY timestamp ASC
    """
    return query_garmin_data(sql)


def _build_training_load_cte(weeks: int, ftp_watts: float) -> str:
    """Return a `WITH ... with_load AS (...)` CTE (no trailing SELECT) computing
    date/tss/atl/ctl for a date-spine covering `weeks` plus a 42-day seed buffer.

    Shared by get_training_load() and plan_progress.py's plan-progress query so
    both build byte-identical ATL/CTL math — see module docstring in plan_progress.py.
    """
    lookback_days = weeks * 7 + 42
    ftp_safe = max(float(ftp_watts), 0.0)

    # When FTP is known, compute TSS from stored power for activities the pipeline missed.
    # The BETWEEN 30 AND 3000 guard rejects corrupt rows where a timestamp/ID was
    # mistakenly stored as watts (values like 21983723521).
    if ftp_safe > 0:
        tss_expr = (
            f"COALESCE(\n"
            f"                tss,\n"
            f"                CASE\n"
            f"                    WHEN COALESCE(normalized_power_w, avg_power_w) BETWEEN 30 AND 3000\n"
            f"                         AND duration_s > 0\n"
            f"                    THEN ROUND(\n"
            f"                        (duration_s * POWER(COALESCE(normalized_power_w, avg_power_w) / {ftp_safe}, 2))\n"
            f"                        / 3600.0 * 100.0, 1)\n"
            f"                    ELSE NULL\n"
            f"                END,\n"
            f"                0\n"
            f"            )"
        )
    else:
        tss_expr = "COALESCE(tss, 0)"

    return f"""
        WITH date_spine AS (
            SELECT d AS date
            FROM UNNEST(GENERATE_DATE_ARRAY(
                DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY),
                CURRENT_DATE()
            )) AS d
        ),
        daily_tss AS (
            SELECT
                DATE(date) AS date,
                SUM({tss_expr}) AS total_tss
            FROM `{PROJECT_ID}.garmin.garmin_activities`
            WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY))
            GROUP BY 1
        ),
        filled AS (
            SELECT
                ds.date,
                COALESCE(dt.total_tss, 0) AS tss
            FROM date_spine ds
            LEFT JOIN daily_tss dt ON ds.date = dt.date
        ),
        with_load AS (
            SELECT
                date,
                tss,
                AVG(tss) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS atl,
                AVG(tss) OVER (ORDER BY date ROWS BETWEEN 41 PRECEDING AND CURRENT ROW) AS ctl
            FROM filled
        )
    """


def get_training_load(weeks: int = 8, ftp_watts: float = 0) -> str:
    """Compute daily ATL, CTL, and TSB (cycling training-load metrics) from activity TSS data.

    This is a cycling-specialty tool — only call it when the user explicitly asks about their
    cycling training load, fitness/fatigue balance, or FTP progress. Don't volunteer ATL/CTL/TSB
    jargon in general health conversations.

    ATL (Acute Training Load) = 7-day rolling average TSS — represents short-term fatigue.
    CTL (Chronic Training Load) = 42-day rolling average TSS — represents long-term fitness base.
    TSB (Training Stress Balance) = CTL − ATL — positive means fresh, negative means fatigued.

    Always pass ftp_watts from the user's current profile so that TSS can be computed on
    the fly for any activities where it was not pre-calculated by the pipeline.

    Args:
        weeks: Number of weeks of results to return (default 8). An extra 42-day buffer is
               fetched automatically to seed the CTL window accurately.
        ftp_watts: User's current cycling FTP in watts (e.g. 191). When provided, activities that
                   have stored power data but a NULL tss column will have TSS computed as
                   (duration_s × (NP or avg_power / FTP)²) / 3600 × 100. Pass 0 to disable.

    Returns:
        Daily training load table (date, tss, atl, ctl, tsb) as a formatted string.
    """
    cte = _build_training_load_cte(weeks, ftp_watts)
    sql = f"""
        {cte}
        SELECT
            date,
            ROUND(tss, 1) AS tss,
            ROUND(atl, 1) AS atl,
            ROUND(ctl, 1) AS ctl,
            ROUND(ctl - atl, 1) AS tsb
        FROM with_load
        WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL {weeks * 7} DAY))
        ORDER BY date DESC
    """
    return query_garmin_data(sql)


def get_weekly_summary(weeks: int = 8) -> str:
    """Fetch a week-by-week training and recovery summary.

    Per week shows: number of activities, total TSS, total hours, total km, dominant
    activity type, plus average RHR, HRV, sleep, sleep score, and body battery from
    garmin_stats.

    Args:
        weeks: Number of weeks to return (default 8).

    Returns:
        Weekly summary table as a formatted string, newest week first.
    """
    lookback_days = weeks * 7
    sql = f"""
        WITH deduped_stats AS (
            SELECT date, rhr, hrv_avg, sleep_total_hr, sleep_score, body_battery
            FROM `{PROJECT_ID}.garmin.garmin_stats`
            WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY))
            QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY timestamp DESC) = 1
        ),
        weekly_stats AS (
            SELECT
                DATE_TRUNC(date, WEEK(MONDAY)) AS week_start,
                ROUND(AVG(rhr), 1) AS avg_rhr,
                ROUND(AVG(hrv_avg), 1) AS avg_hrv,
                ROUND(AVG(sleep_total_hr), 2) AS avg_sleep_hr,
                ROUND(AVG(sleep_score), 1) AS avg_sleep_score,
                ROUND(AVG(body_battery), 1) AS avg_body_battery
            FROM deduped_stats
            GROUP BY 1
        ),
        weekly_activities AS (
            SELECT
                DATE_TRUNC(DATE(date), WEEK(MONDAY)) AS week_start,
                COUNT(*) AS num_activities,
                ROUND(SUM(COALESCE(tss, 0)), 1) AS total_tss,
                ROUND(SUM(duration_s) / 3600.0, 1) AS total_hours,
                ROUND(SUM(COALESCE(distance_m, 0)) / 1000.0, 1) AS total_km,
                APPROX_TOP_COUNT(activity_type, 1)[OFFSET(0)].value AS dominant_type
            FROM `{PROJECT_ID}.garmin.garmin_activities`
            WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY))
            GROUP BY 1
        )
        SELECT
            wa.week_start,
            wa.num_activities,
            wa.total_tss,
            wa.total_hours,
            wa.total_km,
            wa.dominant_type,
            ws.avg_rhr,
            ws.avg_hrv,
            ws.avg_sleep_hr,
            ws.avg_sleep_score,
            ws.avg_body_battery
        FROM weekly_activities wa
        LEFT JOIN weekly_stats ws ON wa.week_start = ws.week_start
        ORDER BY wa.week_start DESC
    """
    return query_garmin_data(sql)


def get_body_composition_trend(weeks: int = 12) -> str:
    """Fetch body composition trend over time from garmin_stats.

    Returns one row per day (latest reading) including weight, body fat %, muscle mass,
    water %, VO2 max, and training status. Useful for tracking changes in physique and
    aerobic fitness over a training block.

    Args:
        weeks: Number of weeks to look back (default 12).

    Returns:
        Body composition trend table as a formatted string, newest first.
    """
    sql = f"""
        SELECT
            date,
            weight_lbs,
            ROUND(weight_lbs / 2.20462, 1) AS weight_kg,
            body_fat_pct,
            muscle_mass_lbs,
            ROUND(muscle_mass_lbs / 2.20462, 1) AS muscle_mass_kg,
            water_pct,
            vo2_max,
            training_status
        FROM `{PROJECT_ID}.garmin.garmin_stats`
        WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL {weeks * 7} DAY))
          AND (weight_lbs IS NOT NULL OR body_fat_pct IS NOT NULL)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY timestamp DESC) = 1
        ORDER BY date DESC
    """
    return query_garmin_data(sql)


# ---------------------------------------------------------------------------
# Training plan tools — plain sync functions like everything above (not async:
# see weather.py's module docstring for why). Every string returned here must
# stay in plain language — no "phase"/"TSS"/"CTL"/"FTP zone" — mirroring the
# existing rule against power jargon everywhere else in this file's tools.
# ---------------------------------------------------------------------------

_TRAJECTORY_PHRASES = {
    "ahead": "ahead of pace for your goal",
    "on_track": "on track for your goal",
    "behind": "a bit behind pace — here's a good chance to catch up",
}


def get_training_plan() -> str:
    """Return the user's current training plan: goal, target date, and this week's sessions.

    Call this whenever the user asks about their training plan, "what's my plan", what
    stage they're in, or before suggesting a specific session. If no plan is active, say
    so plainly and offer to build one with create_training_plan.

    Returns:
        Formatted plan summary, or "No active training plan." if none exists.
    """
    from datetime import date, timedelta

    import plan_store

    plan = plan_store.load()
    if not plan.get("active"):
        return "No active training plan."

    goal = plan["goal"]
    lines = [f"Goal: {goal['raw_text']} — target date {goal['target_date']}"]
    if goal.get("feasibility_note"):
        lines.append(f"Note: {goal['feasibility_note']}")

    today = date.today()
    week_start = (today - timedelta(days=today.weekday())).isoformat()
    week_end = (today + timedelta(days=6 - today.weekday())).isoformat()
    this_week = sorted(
        (s for s in plan["sessions"] if week_start <= s["date"] <= week_end),
        key=lambda s: s["date"],
    )
    if this_week:
        lines.append("\nThis week:")
        for s in this_week:
            icon = {"completed": "✅", "missed": "⚠️", "pending": "•"}.get(s["status"], "•")
            dur = f" ({s['target_duration_min']} min)" if s.get("target_duration_min") else ""
            lines.append(f"{icon} {s['date']}: {s['title']}{dur} (id: {s['id']})")
    return "\n".join(lines)


def get_plan_progress() -> str:
    """Return how the user is doing against their active training plan.

    Call when the user asks "how am I doing", "am I on track", or before suggesting
    adjustments to the plan.

    Returns:
        Plain-language weekly progress summary, or "No active training plan." if none exists.
    """
    import plan_progress

    result = plan_progress.get_plan_progress()
    if not result.get("active"):
        return "No active training plan."

    progress = result["progress"]
    status = progress.get("trajectory_status")
    phrase = _TRAJECTORY_PHRASES.get(status, "still building a fitness picture — check back after a session or two")
    lines = [f"You're {phrase}."]
    if progress.get("overall_adherence_pct") is not None:
        lines.append(f"You've completed about {int(progress['overall_adherence_pct'])}% of your planned sessions so far.")

    from datetime import date
    today_iso = date.today().isoformat()
    # Prioritize weeks that have actually started (so the current week is never
    # dropped in favour of future weeks for a plan that spans a week boundary).
    started = [w for w in progress["weeks"] if w["week_start"] <= today_iso]
    for w in (started or progress["weeks"])[-4:]:
        lines.append(f"Week of {w['week_start']}: {w['completed_sessions']}/{w['planned_sessions']} sessions done")
    return "\n".join(lines)


def suggest_next_session() -> str:
    """Return today's (or the next upcoming) planned session, adjusted for weather
    forecast and available equipment.

    Always call this instead of inventing a session yourself when the user asks "what
    should I do today/next" and a plan is active.

    Returns:
        The recommended session with any weather/equipment adjustment explained in
        plain language, or "No active training plan." if none exists.
    """
    import plan_progress

    p = profile_store.load()
    result = plan_progress.suggest_next_session(p.get("equipment", ""), p.get("location") or {})
    if not result.get("active"):
        return "No active training plan."
    if result.get("session") is None:
        return result.get("note", "No upcoming sessions in the plan.")

    session = result.get("adjusted_session") or result["session"]
    dur = f" — about {session['target_duration_min']} minutes" if session.get("target_duration_min") else ""
    lines = [f"{session['title']}{dur}. {session['description']}"]
    if result.get("swapped") and session.get("note"):
        lines.append(f"Adjusted: {session['note']}")
    lines.append(f"(session id: {session['id']})")
    return "\n".join(lines)


def get_weather_forecast(days: int = 3) -> str:
    """Fetch a short-range weather forecast for the user's saved location.

    Call before suggesting outdoor sessions, or when the user asks about conditions
    for an upcoming ride/run.

    Args:
        days: Number of days ahead (default 3, max 7).

    Returns:
        Formatted daily forecast (temp range, rain chance, wind), or an error message
        if no location is set in the profile.
    """
    import weather as weather_mod

    p = profile_store.load()
    loc = p.get("location") or {}
    lat, lon = loc.get("lat"), loc.get("lon")
    if lat is None or lon is None:
        return "No location set — add one in Settings to get weather-aware suggestions."

    forecast = weather_mod.get_forecast(lat, lon, days=days)
    if forecast.get("error"):
        return f"Couldn't fetch a forecast right now: {forecast['error']}"

    lines = [f"Forecast for {loc.get('place_name', 'your area')}:"]
    for d, info in forecast.get("days", {}).items():
        lines.append(
            f"{d}: {info['description']}, {info['temp_min_c']}–{info['temp_max_c']}°C, "
            f"{info['precip_prob_pct']}% chance of rain, wind {info['wind_kph']} km/h"
        )
    return "\n".join(lines)


def mark_session_complete(session_id: str, note: str = "") -> str:
    """Mark a specific planned session as completed. Only call this after the user
    explicitly confirms they did it (or wants to log it done).

    Use get_training_plan or suggest_next_session first to find the session_id.

    Args:
        session_id: From get_training_plan/suggest_next_session output.
        note: Optional note, e.g. "felt great" or "cut it short, tired".

    Returns:
        Confirmation, or an error message.
    """
    import plan_store

    plan = plan_store.load()
    if not plan.get("active"):
        return "No active training plan."
    updated, found = [], False
    for s in plan["sessions"]:
        if s["id"] == session_id:
            found = True
            s = {**s, "status": "completed", "note": note or "manual"}
        updated.append(s)
    if not found:
        return f"Couldn't find a session with id {session_id}."
    plan_store.save({**plan, "sessions": updated})
    return "Logged — nice work."


def link_session_calendar_event(session_id: str, event_id: str) -> str:
    """Record that a plan session already has a Google Calendar event, so it doesn't
    get duplicated or orphaned later. Call this right after create_training_event
    succeeds for a session that came from the training plan.

    Args:
        session_id: The plan session's id.
        event_id: The calendar event id returned by create_training_event.

    Returns:
        Confirmation, or an error message.
    """
    import plan_store

    plan = plan_store.load()
    if not plan.get("active"):
        return "No active training plan."
    updated, found = [], False
    for s in plan["sessions"]:
        if s["id"] == session_id:
            found = True
            s = {**s, "calendar_event_id": event_id}
        updated.append(s)
    if not found:
        return f"Couldn't find a session with id {session_id}."
    plan_store.save({**plan, "sessions": updated})
    return "Linked to calendar."


# ---------------------------------------------------------------------------
# Runner construction
# ---------------------------------------------------------------------------

def _build_instruction(p: dict) -> str:
    with open(_SYSTEM_PROMPT_PATH) as f:
        template = f.read()
    ftp = float(p.get("ftp") or 0)
    weight = float(p.get("weight_kg") or 1)
    wpkg = round(ftp / weight, 2) if weight > 0 else "N/A"
    return template.format(
        stats_date=p.get("stats_date", ""),
        ftp=p.get("ftp", ""),
        weight_kg=p.get("weight_kg", ""),
        height_cm=p.get("height_cm", ""),
        age=p.get("age", ""),
        wpkg=wpkg,
        goals=p.get("goals", ""),
        equipment=p.get("equipment", ""),
        location_name=(p.get("location") or {}).get("place_name", "Putney, London"),
    )


_NOT_CONNECTED = "Calendar not connected — ask the user to sign out and sign back in to grant calendar access."


def _auto_save_insights(
    user_message: str,
    response_text: str,
    user_email: str,
    session_id: str,
) -> None:
    """Background: use Gemini Flash to extract coaching insights from the exchange and save them.

    Runs after every response so insights are captured reliably without depending on
    the main model remembering to call a tool.
    """
    try:
        import json
        from google import genai

        prompt = (
            "Review this health coach AI conversation and extract any insights worth saving "
            "to a long-term coaching log. Return JSON only — no markdown, no explanation.\n\n"
            f"USER: {user_message[:600]}\n\n"
            f"COACH: {response_text[:2500]}\n\n"
            'If nothing save-worthy, return: {"insights": []}\n\n'
            'Format: {"insights": [{"category": "milestone|recommendation|observation|goal_progress", '
            '"content": "1-2 sentence self-contained insight", "context": "optional data snippet or empty string"}]}\n\n'
            "SAVE when the coach:\n"
            "- Noted a fitness milestone (longest run/ride, new personal best, consistency streak, "
            "weight goal hit, event completed)\n"
            "- Made a specific recommendation the user agreed to act on (activity, sleep, nutrition, rest)\n"
            "- Created or confirmed a plan (summarise it in 1-2 sentences)\n"
            "- Spotted a significant trend (weight direction, sleep pattern, HRV pattern, RHR shift, stress)\n"
            "- Recorded illness, injury, or a major life event affecting activity or wellbeing\n\n"
            "DO NOT save: routine Q&A, data lookups with no new finding, general advice, "
            "or any response that doesn't contain a specific memorable insight about this person."
        )

        client = genai.Client()
        resp = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        text = resp.text.strip()

        # Strip markdown code fences if the model wraps the JSON
        if "```" in text:
            parts = text.split("```")
            text = parts[1] if len(parts) > 1 else text
            if text.startswith("json\n"):
                text = text[5:]
        text = text.strip()

        data = json.loads(text)
        insights = data.get("insights", [])
        if not insights:
            return

        import coaching_log as cl
        for item in insights:
            category = item.get("category", "observation")
            content = item.get("content", "").strip()
            context = item.get("context", "").strip()
            if content:
                result = cl.save_insight(
                    project_id=PROJECT_ID,
                    session_id=session_id,
                    email=user_email,
                    category=category,
                    content=content,
                    context=context,
                )
                LOGGER.info("Auto-insight [%s]: %.80s → %s", category, content, result)
    except Exception as exc:
        LOGGER.warning("_auto_save_insights error: %s", exc)


def _make_runner(instruction: str, user_email: str = "", session_id: str = "") -> Runner:
    # --- Google Calendar tools (closures capturing user_email) ---

    def list_calendar_events(days_ahead: int = 14) -> str:
        """List upcoming Google Calendar events.

        Args:
            days_ahead: Number of days ahead to look (default 14).

        Returns:
            Formatted list of events with date, time, title, and description.
            Returns a message if calendar is not connected.
        """
        import calendar_store
        creds = calendar_store.load_tokens(user_email)
        if creds is None:
            return _NOT_CONNECTED
        try:
            import datetime
            from googleapiclient.discovery import build
            service = build("calendar", "v3", credentials=creds)
            now = datetime.datetime.utcnow()
            time_min = now.isoformat() + "Z"
            time_max = (now + datetime.timedelta(days=days_ahead)).isoformat() + "Z"
            result = service.events().list(
                calendarId="primary",
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy="startTime",
                maxResults=50,
            ).execute()
            events = result.get("items", [])
            if not events:
                return f"No events found in the next {days_ahead} days."
            lines = []
            for ev in events:
                start = ev["start"].get("dateTime", ev["start"].get("date", ""))
                title = ev.get("summary", "(no title)")
                desc = ev.get("description", "")
                event_id = ev.get("id", "")
                line = f"[{start}] {title} (id: {event_id})"
                if desc:
                    line += f"\n  {desc}"
                lines.append(line)
            return "\n".join(lines)
        except Exception as exc:
            LOGGER.error("list_calendar_events error: %s", exc)
            return f"Error fetching calendar events: {exc}"

    def create_training_event(
        title: str,
        date: str,
        start_time: str,
        duration_minutes: int,
        description: str = "",
    ) -> str:
        """Create a Google Calendar event for a training session or rest day.

        Args:
            title: e.g. "Z2 Endurance Ride — 2hr" or "Rest Day"
            date: YYYY-MM-DD
            start_time: HH:MM (24-hour)
            duration_minutes: Duration as an integer number of minutes.
            description: Optional notes e.g. "Target 120–140W, RPE 6"

        Returns:
            Confirmation with event ID, or error message.
        """
        import calendar_store
        creds = calendar_store.load_tokens(user_email)
        if creds is None:
            return _NOT_CONNECTED
        try:
            import datetime
            from googleapiclient.discovery import build
            service = build("calendar", "v3", credentials=creds)
            start_dt = datetime.datetime.fromisoformat(f"{date}T{start_time}:00")
            end_dt = start_dt + datetime.timedelta(minutes=duration_minutes)
            event_body = {
                "summary": title,
                "description": description,
                "start": {"dateTime": start_dt.isoformat(), "timeZone": "Europe/London"},
                "end": {"dateTime": end_dt.isoformat(), "timeZone": "Europe/London"},
            }
            created = service.events().insert(calendarId="primary", body=event_body).execute()
            event_id = created.get("id", "")
            html_link = created.get("htmlLink", "")
            return f"Event created: '{title}' on {date} at {start_time} for {duration_minutes} min. ID: {event_id}. Link: {html_link}"
        except Exception as exc:
            LOGGER.error("create_training_event error: %s", exc)
            return f"Error creating calendar event: {exc}"

    def delete_calendar_event(event_id: str) -> str:
        """Delete a Google Calendar event by ID.

        Args:
            event_id: ID from list_calendar_events or create_training_event.

        Returns:
            Confirmation or error message.
        """
        import calendar_store
        creds = calendar_store.load_tokens(user_email)
        if creds is None:
            return _NOT_CONNECTED
        try:
            from googleapiclient.discovery import build
            service = build("calendar", "v3", credentials=creds)
            service.events().delete(calendarId="primary", eventId=event_id).execute()
            return f"Event {event_id} deleted successfully."
        except Exception as exc:
            LOGGER.error("delete_calendar_event error: %s", exc)
            return f"Error deleting calendar event: {exc}"

    # --- Training plan creation/adjustment (closure capturing user_email, needed
    # to log a continuity note to coaching_log when replacing an existing plan) ---

    def create_training_plan(goal_text: str, target_date: str, discipline: str = "cycling") -> str:
        """Generate a training plan from a stated goal and save it as the active plan.

        This is also how you adjust or reformulate an existing plan — e.g. the user
        wants a different target date, a harder/easier goal, or to switch discipline.
        There's no separate "edit" tool: just call this again with the new goal and it
        replaces the old plan, re-baselined from their current fitness (not the old
        plan's numbers). If a plan is already active, call get_training_plan or
        get_plan_progress first, mention where they left off, and confirm they want to
        replace it before calling this.

        Always restate the parsed goal and target date back to the user for
        confirmation before calling this. Pulls the user's current fitness (recent
        training load) and equipment from their profile automatically; you don't need
        to ask for those separately.

        Args:
            goal_text: The user's goal in their own words, e.g. "sub-5:00 century".
            target_date: Event/goal date in YYYY-MM-DD format.
            discipline: "cycling" | "running" (default "cycling").

        Returns:
            A plain-language confirmation with the plan's shape (and, if this replaced
            an existing plan, a short note on how that one wrapped up), or an error message.
        """
        from datetime import date

        import plan_progress

        try:
            target = date.fromisoformat(target_date)
        except ValueError:
            return f"'{target_date}' isn't a valid date — please give it as YYYY-MM-DD."
        if target <= date.today():
            return "The target date needs to be in the future."

        plan = plan_progress.build_and_save_plan(goal_text, target, discipline, email=user_email)

        lines = []
        if plan.get("outgoing_summary"):
            lines.append(plan["outgoing_summary"])
        lines.append(f"Plan created: {goal_text} by {target_date}.")
        if plan["goal"].get("feasibility_note"):
            lines.append(plan["goal"]["feasibility_note"])
        session_count = len([s for s in plan["sessions"] if s["session_type"] != "rest"])
        lines.append(f"{len(plan['phases'])} training stages, {session_count} sessions planned.")
        first_week = sorted(
            (s for s in plan["sessions"] if s["week_number"] == 1), key=lambda s: s["date"],
        )
        lines.append("First week:")
        for s in first_week:
            dur = f" ({s['target_duration_min']} min)" if s.get("target_duration_min") else ""
            lines.append(f"- {s['date']}: {s['title']}{dur}")
        return "\n".join(lines)

    # --- Coaching log tool (closure capturing user_email) ---
    # Insights are saved automatically after every response via _auto_save_insights.
    # get_coaching_log is kept as a tool so the model can query the log on demand
    # (e.g. "what did you recommend last month?").

    def get_coaching_log(weeks: int = 52, category: str = "") -> str:
        """Retrieve past coaching insights from the persistent coaching log.

        Call at the start of a new conversation, when the user asks about past advice or
        progress, or before repeating a recommendation. Use the log for continuity.

        Args:
            weeks: How many weeks back to look (default 52 = one year).
            category: Optional filter — "milestone", "recommendation", "observation", "goal_progress".

        Returns:
            Formatted log entries or "No coaching log entries found."
        """
        import coaching_log
        return coaching_log.get_insights(
            project_id=PROJECT_ID,
            email=user_email,
            weeks=weeks,
            category=category,
        )

    agent = LlmAgent(
        model="gemini-2.5-flash",
        name="health_coach",
        instruction=instruction,
        tools=[
            FunctionTool(func=query_garmin_data),
            FunctionTool(func=get_recent_activities),
            FunctionTool(func=get_recent_stats),
            FunctionTool(func=get_intraday_stats),
            FunctionTool(func=get_training_load),
            FunctionTool(func=get_weekly_summary),
            FunctionTool(func=get_body_composition_trend),
            FunctionTool(func=list_calendar_events),
            FunctionTool(func=create_training_event),
            FunctionTool(func=delete_calendar_event),
            FunctionTool(func=get_coaching_log),
            FunctionTool(func=get_training_plan),
            FunctionTool(func=create_training_plan),
            FunctionTool(func=get_plan_progress),
            FunctionTool(func=suggest_next_session),
            FunctionTool(func=get_weather_forecast),
            FunctionTool(func=mark_session_complete),
            FunctionTool(func=link_session_calendar_event),
        ],
    )
    return Runner(agent=agent, app_name=_APP_NAME, session_service=_session_service)


def _get_runner(session_id: str, user_email: str = "") -> Runner:
    """Return a cached runner for this session, rebuilding if the profile changed."""
    current_profile = profile_store.load()

    if session_id in _runners:
        runner, cached_profile = _runners[session_id]
        if cached_profile == current_profile:
            return runner

    instruction = _build_instruction(current_profile)
    runner = _make_runner(instruction, user_email=user_email, session_id=session_id)
    _runners[session_id] = (runner, dict(current_profile))
    return runner


def warm_bq_cache(delay_seconds: int = 0) -> None:
    """Pre-populate the BQ cache with the most commonly used queries.

    Intended to be called in a background thread after a Garmin sync completes.
    Pass delay_seconds to wait for the pipeline to finish before fetching.
    """
    if delay_seconds:
        time.sleep(delay_seconds)
    import bq_cache as _bq_cache
    _bq_cache.clear()
    LOGGER.info("Warming BQ cache...")
    p = profile_store.load()
    ftp = float(p.get("ftp") or 0)
    try:
        get_recent_stats(30)
        get_recent_activities(30)
        get_weekly_summary(8)
        if ftp > 0:
            get_training_load(8, ftp)
        LOGGER.info("BQ cache warm complete")
    except Exception as exc:
        LOGGER.warning("BQ cache warm error: %s", exc)


def invalidate_sessions() -> None:
    """Evict all cached runners so the next request rebuilds with the latest profile."""
    _runners.clear()
    profile_store.invalidate_cache()
    LOGGER.info("All agent sessions invalidated; will rebuild on next request")


def evict_session(session_id: str) -> None:
    """Evict a single runner from the cache (e.g. after session deletion)."""
    _runners.pop(session_id, None)


async def _prepare_session(session_id: str, user_email: str, message: str) -> str:
    """Ensure ADK session exists; return message with cold-start preamble if new session."""
    session = await _session_service.get_session(
        app_name=_APP_NAME, user_id="user", session_id=session_id
    )
    is_new_session = session is None
    if is_new_session:
        await _session_service.create_session(
            app_name=_APP_NAME, user_id="user", session_id=session_id
        )

    if not (is_new_session and user_email):
        return message

    import session_store
    import coaching_log as coaching_log_mod

    preamble_parts: list[str] = []
    restore_ctx = session_store.get_restore_context(user_email, session_id)
    if restore_ctx:
        preamble_parts.append(restore_ctx)
    try:
        log_ctx = coaching_log_mod.get_insights(
            project_id=PROJECT_ID, email=user_email, weeks=8
        )
        if log_ctx and "No coaching log entries found" not in log_ctx and "Error" not in log_ctx:
            preamble_parts.append(log_ctx)
    except Exception as exc:
        LOGGER.warning("Could not fetch coaching log for preamble: %s", exc)

    if preamble_parts:
        return "\n\n".join(preamble_parts) + "\n\n" + message
    return message


async def run_agent_stream(
    message: str,
    session_id: str = "default",
    user_email: str = "",
):
    """Async generator: yields tool_start/tool_done dicts, then a final done dict with the response."""
    runner = _get_runner(session_id, user_email=user_email)
    actual_message = await _prepare_session(session_id, user_email, message)

    content = Content(parts=[Part(text=actual_message)])
    response_parts: list[str] = []

    async for event in runner.run_async(
        user_id="user", session_id=session_id, new_message=content
    ):
        if event.content and event.content.parts:
            for part in event.content.parts:
                fc = getattr(part, "function_call", None)
                if fc and getattr(fc, "name", None):
                    yield {"type": "tool_start", "name": fc.name}
                fr = getattr(part, "function_response", None)
                if fr and getattr(fr, "name", None):
                    yield {"type": "tool_done", "name": fr.name}

        if hasattr(event, "is_final_response") and event.is_final_response():
            if event.content and event.content.parts:
                for part in event.content.parts:
                    if hasattr(part, "text") and part.text:
                        response_parts.append(part.text)

    response_text = "".join(response_parts) or "I was unable to generate a response. Please try again."

    if user_email:
        def _persist() -> None:
            import session_store
            session_store.append_message(user_email, session_id, "user", message)
            session_store.append_message(user_email, session_id, "assistant", response_text)
            _auto_save_insights(message, response_text, user_email, session_id)
        threading.Thread(target=_persist, daemon=True).start()

    yield {"type": "done", "response": response_text}


async def run_agent(
    message: str,
    session_id: str = "default",
    user_email: str = "",
) -> str:
    """Run the agent and return the final response text (non-streaming)."""
    async for evt in run_agent_stream(message, session_id=session_id, user_email=user_email):
        if evt.get("type") == "done":
            return evt["response"]
    return "I was unable to generate a response. Please try again."
