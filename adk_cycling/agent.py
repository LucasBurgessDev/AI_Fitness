from __future__ import annotations

import logging
import os

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
        Daily stats including weight, sleep, HRV, body battery, VO2 max as a formatted string.
    """
    sql = f"""
        SELECT date, weight_lbs, sleep_total_hr, sleep_score, rhr,
               hrv_avg, hrv_status, body_battery, vo2_max,
               training_status, steps, cals_total
        FROM `{PROJECT_ID}.garmin.garmin_stats`
        WHERE date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY))
        ORDER BY date DESC
        LIMIT 60
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


def _make_runner(instruction: str) -> Runner:
    agent = LlmAgent(
        model="gemini-2.0-flash",
        name="cycling_expert",
        instruction=instruction,
        tools=[
            FunctionTool(func=query_garmin_data),
            FunctionTool(func=get_recent_activities),
            FunctionTool(func=get_recent_stats),
        ],
    )
    return Runner(agent=agent, app_name=_APP_NAME, session_service=_session_service)


def _get_runner(session_id: str) -> Runner:
    """Return a cached runner for this session, rebuilding if the profile changed."""
    current_profile = profile_store.load()

    if session_id in _runners:
        runner, cached_profile = _runners[session_id]
        if cached_profile == current_profile:
            return runner

    instruction = _build_instruction(current_profile)
    runner = _make_runner(instruction)
    _runners[session_id] = (runner, dict(current_profile))
    return runner


def invalidate_sessions() -> None:
    """Evict all cached runners so the next request rebuilds with the latest profile."""
    _runners.clear()
    profile_store.invalidate_cache()
    LOGGER.info("All agent sessions invalidated; will rebuild on next request")


async def run_agent(message: str, session_id: str = "default") -> str:
    """Run the cycling agent for a single user message and return the response text."""
    runner = _get_runner(session_id)

    # Create session on first use; get_session returns None (not an exception) when not found.
    session = await _session_service.get_session(
        app_name=_APP_NAME, user_id="user", session_id=session_id
    )
    if session is None:
        await _session_service.create_session(
            app_name=_APP_NAME, user_id="user", session_id=session_id
        )

    content = Content(parts=[Part(text=message)])
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

    return "".join(response_parts) or "I was unable to generate a response. Please try again."
