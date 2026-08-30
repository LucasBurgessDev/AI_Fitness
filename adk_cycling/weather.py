"""
Forward-looking weather forecast + geocoding via Open-Meteo (no API key required).

Used by the training-plan feature to adjust session suggestions (e.g. swap an
outdoor hill session for an indoor one if heavy rain is forecast). This is
distinct from `pipeline/garmin_activities_daily.py`'s `fetch_activity_weather`,
which records *past* per-activity conditions from the Garmin device — this
module looks *forward* from a lat/lon.

Synchronous (httpx.Client, not AsyncClient) to match agent.py's tool functions,
which are all plain `def` — an async call from inside one of those risks a
nested-event-loop error since ADK's function-calling isn't guaranteed to run
tools off the main loop's thread. app.py's async routes wrap these in
asyncio.to_thread(...) instead, the same pattern _compute_goals_data uses.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

LOGGER = logging.getLogger(__name__)

_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
_GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"

# WMO weather codes → short human description (subset covering common cases).
_WEATHER_CODE_DESC = {
    0: "clear sky", 1: "mainly clear", 2: "partly cloudy", 3: "overcast",
    45: "fog", 48: "depositing rime fog",
    51: "light drizzle", 53: "drizzle", 55: "dense drizzle",
    61: "light rain", 63: "rain", 65: "heavy rain",
    71: "light snow", 73: "snow", 75: "heavy snow",
    80: "light rain showers", 81: "rain showers", 82: "violent rain showers",
    95: "thunderstorm",
}


def describe_code(code: Optional[int]) -> str:
    if code is None:
        return "unknown conditions"
    return _WEATHER_CODE_DESC.get(int(code), "mixed conditions")


def get_forecast(lat: float, lon: float, days: int = 3) -> dict[str, Any]:
    """Fetch a daily forecast for the given coordinates.

    Returns:
        {"days": {"YYYY-MM-DD": {"temp_max_c", "temp_min_c", "precip_prob_pct",
         "wind_kph", "code", "description"}, ...}}
        or {"error": "..."} on failure — callers should treat a missing/empty
        result as "no forecast available" rather than crash.
    """
    days = max(1, min(int(days), 7))
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": ",".join([
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_probability_max",
            "windspeed_10m_max",
            "weathercode",
        ]),
        "forecast_days": days,
        "timezone": "auto",
    }
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(_FORECAST_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        LOGGER.warning("weather.get_forecast failed for (%s, %s): %s", lat, lon, exc)
        return {"error": str(exc)}

    daily = data.get("daily") or {}
    dates = daily.get("time") or []
    out: dict[str, Any] = {}
    for i, date_str in enumerate(dates):
        code = (daily.get("weathercode") or [None] * len(dates))[i]
        out[date_str] = {
            "temp_max_c": (daily.get("temperature_2m_max") or [None] * len(dates))[i],
            "temp_min_c": (daily.get("temperature_2m_min") or [None] * len(dates))[i],
            "precip_prob_pct": (daily.get("precipitation_probability_max") or [None] * len(dates))[i],
            "wind_kph": (daily.get("windspeed_10m_max") or [None] * len(dates))[i],
            "code": code,
            "description": describe_code(code),
        }
    return {"days": out}


def geocode(place_name: str) -> Optional[dict[str, Any]]:
    """Resolve a free-text place name to coordinates.

    Returns {"place_name": "<resolved name, country>", "lat": float, "lon": float}
    or None if nothing matched / the request failed.
    """
    place_name = (place_name or "").strip()
    if not place_name:
        return None
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(
                _GEOCODE_URL,
                params={"name": place_name, "count": 1, "language": "en", "format": "json"},
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        LOGGER.warning("weather.geocode failed for %r: %s", place_name, exc)
        return None

    results = data.get("results") or []
    if not results:
        return None
    r = results[0]
    name_parts = [r.get("name", place_name)]
    if r.get("admin1"):
        name_parts.append(r["admin1"])
    if r.get("country"):
        name_parts.append(r["country"])
    return {
        "place_name": ", ".join(name_parts),
        "lat": r.get("latitude"),
        "lon": r.get("longitude"),
    }
