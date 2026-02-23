from __future__ import annotations

import logging
import os
from typing import Optional

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools.mcp_tool import McpToolset, StdioConnectionParams
from mcp import StdioServerParameters
from google.genai.types import Content, Part

import profile as profile_store

LOGGER = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID", "health-data-482722")

_APP_NAME = "cycling_coach"
_session_service = InMemorySessionService()

# Per-session runner cache: session_id → (runner, profile_snapshot)
# When the profile changes, old runners are evicted so the new prompt is used.
_runners: dict[str, tuple[Runner, dict]] = {}

_SYSTEM_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "system_prompt.txt")


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
    bq_mcp = McpToolset(
        connection_params=StdioConnectionParams(
            server_params=StdioServerParameters(
                command="npx",
                args=[
                    "-y",
                    "@modelcontextprotocol/server-bigquery",
                    "--project",
                    PROJECT_ID,
                    "--dataset",
                    "garmin",
                ],
            )
        )
    )
    drive_mcp = McpToolset(
        connection_params=StdioConnectionParams(
            server_params=StdioServerParameters(
                command="npx",
                args=["-y", "@modelcontextprotocol/server-gdrive"],
            )
        )
    )
    agent = LlmAgent(
        model="gemini-2.0-flash",
        name="cycling_expert",
        instruction=instruction,
        tools=[bq_mcp, drive_mcp],
    )
    return Runner(agent=agent, app_name=_APP_NAME, session_service=_session_service)


def _get_runner(session_id: str) -> Runner:
    """Return a cached runner for this session, rebuilding if the profile changed."""
    current_profile = profile_store.load()

    if session_id in _runners:
        runner, cached_profile = _runners[session_id]
        # Rebuild if any profile key has changed
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

    # Create session on first use; reuse on subsequent calls.
    # get_session returns None (not an exception) when not found.
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
