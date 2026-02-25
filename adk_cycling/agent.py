from __future__ import annotations

import logging
import os
import threading

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools import FunctionTool
from google.cloud import bigquery
from google.genai.types import Content, Part

import profile as profile_store

LOGGER = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID", "health-data-482722")

_APP_NAME = "cycling_coach"
_session_service = InMemorySessionService()

# Per-session runner cache: session_id → (runner, profile_snapshot)
_runners: dict[str, tuple[Runner, dict]] = {}

_SYSTEM_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "system_prompt.txt")


# ---------------------------------------------------------------------------
# BigQuery function tools — used instead of MCP to avoid asyncio scope issues
# ---------------------------------------------------------------------------

def query_garmin_data(sql: str) -> str:
    """Execute a SQL query against the garmin BigQuery dataset and return results.

    Args:
        sql: A valid BigQuery SQL query. Available tables:
             - `garmin.garmin_stats`: daily biometrics (weight, sleep, HRV, VO2 max, steps, etc.)
             - `garmin.garmin_activities`: activities with power, HR, TSS, FTP, cadence, distance.
             Both tables are in project health-data-482722 and partitioned on run_date (DATE).

    Returns:
        Query results as a formatted string, or an error message.
    """
    try:
        client = bigquery.Client(project=PROJECT_ID)
        results = client.query(sql).result()
        rows = [dict(row) for row in results]
        if not rows:
            return "Query returned no results."
        # Format as a readable table summary
        return "\n".join(str(row) for row in rows)
    except Exception as e:
        LOGGER.error("BigQuery query error: %s | SQL: %s", e, sql)
        return f"Query error: {e}"


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
        Daily stats including weight, body composition, sleep, HRV, stress, and more as a formatted string.
    """
    sql = f"""
        SELECT date, timestamp, weight_lbs, muscle_mass_lbs, body_fat_pct, water_pct,
               sleep_total_hr, sleep_deep_hr, sleep_rem_hr, sleep_score,
               rhr, min_hr, max_hr, avg_stress, body_battery, respiration, spo2,
               vo2_max, training_status, hrv_status, hrv_avg,
               steps, step_goal, cals_total, cals_active, activities
        FROM `{PROJECT_ID}.garmin.garmin_stats`
        WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY timestamp DESC) = 1
        ORDER BY date DESC
        LIMIT 60
    """
    return query_garmin_data(sql)


def get_training_load(weeks: int = 8) -> str:
    """Compute daily ATL, CTL, and TSB (training load metrics) from activity TSS data.

    ATL (Acute Training Load) = 7-day rolling average TSS — represents short-term fatigue.
    CTL (Chronic Training Load) = 42-day rolling average TSS — represents long-term fitness base.
    TSB (Training Stress Balance) = CTL − ATL — positive means fresh, negative means fatigued.

    Args:
        weeks: Number of weeks of results to return (default 8). An extra 42-day buffer is
               fetched automatically to seed the CTL window accurately.

    Returns:
        Daily training load table (date, tss, atl, ctl, tsb) as a formatted string.
    """
    lookback_days = weeks * 7 + 42
    sql = f"""
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
                SUM(COALESCE(tss, 0)) AS total_tss
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
        SELECT
            date,
            ROUND(tss, 1) AS tss,
            ROUND(atl, 1) AS atl,
            ROUND(ctl, 1) AS ctl,
            ROUND(ctl - atl, 1) AS tsb
        FROM with_load
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {weeks * 7} DAY)
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
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
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
    )


_NOT_CONNECTED = "Calendar not connected — ask the user to sign out and sign back in to grant calendar access."


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

    # --- Coaching log tools (closures capturing session_id + user_email) ---

    def save_coaching_insight(category: str, content: str, context: str = "") -> str:
        """Save a coaching insight to the persistent coaching log.

        Call proactively when you identify a PR, make a recommendation, observe a notable
        trend, or track goal progress. This ensures continuity across sessions.

        Args:
            category: One of "PR", "recommendation", "observation", "goal_progress".
            content: The insight text (concise, self-contained, 1–3 sentences).
            context: Optional supporting data or context snippet.

        Returns:
            "Insight saved." or an error string.
        """
        import coaching_log
        return coaching_log.save_insight(
            project_id=PROJECT_ID,
            session_id=session_id,
            email=user_email,
            category=category,
            content=content,
            context=context,
        )

    def get_coaching_log(weeks: int = 52, category: str = "") -> str:
        """Retrieve past coaching insights from the persistent coaching log.

        Call at the start of a new conversation, when the user asks about past advice or
        progress, or before repeating a recommendation. Use the log for continuity.

        Args:
            weeks: How many weeks back to look (default 52 = one year).
            category: Optional filter — "PR", "recommendation", "observation", "goal_progress".

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
        model="gemini-2.0-flash",
        name="cycling_expert",
        instruction=instruction,
        tools=[
            FunctionTool(func=query_garmin_data),
            FunctionTool(func=get_recent_activities),
            FunctionTool(func=get_recent_stats),
            FunctionTool(func=get_training_load),
            FunctionTool(func=get_weekly_summary),
            FunctionTool(func=get_body_composition_trend),
            FunctionTool(func=list_calendar_events),
            FunctionTool(func=create_training_event),
            FunctionTool(func=delete_calendar_event),
            FunctionTool(func=save_coaching_insight),
            FunctionTool(func=get_coaching_log),
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


def invalidate_sessions() -> None:
    """Evict all cached runners so the next request rebuilds with the latest profile."""
    _runners.clear()
    profile_store.invalidate_cache()
    LOGGER.info("All agent sessions invalidated; will rebuild on next request")


def evict_session(session_id: str) -> None:
    """Evict a single runner from the cache (e.g. after session deletion)."""
    _runners.pop(session_id, None)


async def run_agent(
    message: str,
    session_id: str = "default",
    user_email: str = "",
) -> str:
    """Run the cycling agent for a single user message and return the response text."""
    runner = _get_runner(session_id, user_email=user_email)

    # Create session on first use; get_session returns None (not an exception) when not found.
    session = await _session_service.get_session(
        app_name=_APP_NAME, user_id="user", session_id=session_id
    )
    is_new_session = session is None
    if is_new_session:
        await _session_service.create_session(
            app_name=_APP_NAME, user_id="user", session_id=session_id
        )

    # Cold-start context restore: prepend prior conversation history
    actual_message = message
    if is_new_session and user_email:
        import session_store
        restore_ctx = session_store.get_restore_context(user_email, session_id)
        if restore_ctx:
            actual_message = f"{restore_ctx}\n\n{message}"

    content = Content(parts=[Part(text=actual_message)])
    response_parts: list[str] = []

    async for event in runner.run_async(
        user_id="user",
        session_id=session_id,
        new_message=content,
    ):
        if hasattr(event, "is_final_response") and event.is_final_response():
            if event.content and event.content.parts:
                for part in event.content.parts:
                    if hasattr(part, "text") and part.text:
                        response_parts.append(part.text)

    response_text = "".join(response_parts) or "I was unable to generate a response. Please try again."

    # Background GCS persist: append user message then assistant response
    if user_email:
        def _persist() -> None:
            import session_store
            session_store.append_message(user_email, session_id, "user", message)
            session_store.append_message(user_email, session_id, "assistant", response_text)
        threading.Thread(target=_persist, daemon=True).start()

    return response_text
