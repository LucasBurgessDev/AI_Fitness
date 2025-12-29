from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, Optional, Tuple
import time


@dataclass(frozen=True)
class FtpResult:
    ftp_watts: Optional[float]
    source: str  # "garmin_settings" | "virtual_ride_best_20m" | "missing"
    best_20m_watts: Optional[float]


def _try_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        x = float(v)
        return x if x > 0 else None
    except Exception:
        return None


def _scan_for_keys(obj: Any, key_hints: Tuple[str, ...]) -> Optional[float]:
    """
    Recursively scan dict/list payloads for numeric values under keys that match hints.
    """
    if obj is None:
        return None

    if isinstance(obj, (int, float)):
        return _try_float(obj)

    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower()
            if any(h in kl for h in key_hints):
                val = _try_float(v)
                if val:
                    return val
            found = _scan_for_keys(v, key_hints)
            if found:
                return found

    if isinstance(obj, list):
        for item in obj:
            found = _scan_for_keys(item, key_hints)
            if found:
                return found

    return None


def get_cycling_ftp_from_settings(api: Any) -> Optional[float]:
    """
    Garmin settings first:
    - Use api.get_cycling_ftp() if available (newer python-garminconnect)
    - Else try connectapi endpoint if api exposes connectapi or if garth is present
    """
    # 1) Prefer library method if present
    fn = getattr(api, "get_cycling_ftp", None)
    if callable(fn):
        try:
            data = fn()
            ftp = _scan_for_keys(
                data,
                (
                    "functionalthresholdpower",
                    "thresholdpower",
                    "ftp",
                    "ftpwatts",
                ),
            )
            return ftp
        except Exception:
            pass

    # 2) Try connectapi via api.connectapi if exposed
    fn2 = getattr(api, "connectapi", None)
    if callable(fn2):
        try:
            # Based on python-garminconnect implementation of get_cycling_ftp
            data = fn2(f"{api.garmin_connect_biometric_url}/latestFunctionalThresholdPower/CYCLING")
            ftp = _scan_for_keys(
                data,
                (
                    "functionalthresholdpower",
                    "thresholdpower",
                    "ftp",
                    "ftpwatts",
                ),
            )
            return ftp
        except Exception:
            pass

    # 3) Last resort, try garth.connectapi directly if garth is loaded in this repo
    try:
        import garth  # local dependency already used in our scripts

        data = garth.connectapi(
            "biometric-service/biometric/latestFunctionalThresholdPower/CYCLING",
            params={},
        )
        ftp = _scan_for_keys(
            data,
            (
                "functionalthresholdpower",
                "thresholdpower",
                "ftp",
                "ftpwatts",
            ),
        )
        return ftp
    except Exception:
        return None


def _activity_type_key(act: Dict[str, Any]) -> str:
    t = act.get("activityType")
    if isinstance(t, dict):
        return (t.get("typeKey") or t.get("typeName") or "").strip().lower().replace(" ", "_")
    return str(act.get("activityType") or act.get("activityTypeName") or act.get("activity_type") or "").strip().lower().replace(" ", "_")


def extract_best_20m_power_w(activity_detail: Dict[str, Any]) -> Optional[float]:
    """
    We look for Garmin fields like: max avg power (20 mins).
    Payload key names vary, so we scan for hints.
    """
    return _scan_for_keys(
        activity_detail,
        (
            "20min",
            "20_min",
            "20-min",
            "maxavgpower",
            "max_avg_power",
            "maxaveragepower",
            "best20",
            "best_20",
        ),
    )


def fetch_activity_detail(api: Any, activity_id: str) -> Dict[str, Any]:
    """
    Uses api.get_activity_details if present, otherwise falls back to garth.connectapi.
    """
    fn = getattr(api, "get_activity_details", None)
    if callable(fn):
        try:
            d = fn(activity_id)
            if isinstance(d, dict):
                return d
        except Exception:
            pass

    # fallback using garth
    try:
        import garth
        d = garth.connectapi(f"activity-service/activity/{activity_id}", params={})
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def ftp_from_virtual_rides(api: Any, lookback_days: int = 60, sleep_s: float = 0.1) -> FtpResult:
    """
    Looks at recent virtual rides first, grabs the best 'max avg power 20 min' we can find,
    then estimates FTP as 95% of that.
    """
    today = date.today()
    start = today - timedelta(days=lookback_days)

    # We try to fetch a broad list and filter locally, similar to our daily script behavior
    activities = None
    try:
        try:
            activities = api.get_activities_by_date(start.isoformat(), today.isoformat(), "")
        except Exception:
            activities = api.get_activities_by_date(start.isoformat(), today.isoformat(), None)
    except Exception:
        activities = None

    if not activities:
        return FtpResult(None, "missing", None)

    best_20m = None

    for act in activities:
        t = _activity_type_key(act)
        if t not in {"virtual_ride", "indoor_cycling", "spinning"}:
            continue

        act_id = act.get("activityId") or act.get("activity_id") or act.get("id")
        if not act_id:
            continue

        detail = fetch_activity_detail(api, str(act_id))
        v = extract_best_20m_power_w(detail)
        if v and (best_20m is None or v > best_20m):
            best_20m = v

        if sleep_s:
            time.sleep(sleep_s)

    if not best_20m:
        return FtpResult(None, "missing", None)

    ftp = 0.95 * best_20m
    return FtpResult(ftp, "virtual_ride_best_20m", best_20m)


def resolve_ftp(api: Any) -> FtpResult:
    """
    Garmin settings first, then virtual ride fallback.
    """
    ftp = get_cycling_ftp_from_settings(api)
    if ftp:
        return FtpResult(ftp, "garmin_settings", None)

    return ftp_from_virtual_rides(api)
