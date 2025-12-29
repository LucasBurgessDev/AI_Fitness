#!/usr/bin/env python3
"""
garmin_activities_history.py

Backfill Garmin activities (running + cycling) into one CSV: garmin_activities.csv
Includes Step 4 enrichment:
- FTP resolved once per run (Garmin settings first, virtual ride best 20m fallback)
- Cycling activities pull details to extract best_20m and normalized power when available
- IF and TSS computed when FTP exists

Assumptions:
- direct_login.py has created .garth session folder (default ./.garth)
- SAVE_PATH set in .env (or env var)
- activity_filter.py + activity_filters.yaml exist at repo root
"""

import csv
import os
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional, Set, Tuple

import garth
from dotenv import load_dotenv
from garminconnect import Garmin

from activity_filter import load_activity_filter

# -----------------------------
# CONFIG
# -----------------------------
load_dotenv()

SAVE_PATH = os.getenv("SAVE_PATH")
if SAVE_PATH:
    CSV_FILE = os.path.join(SAVE_PATH, "garmin_activities.csv")
else:
    print("WARNING: SAVE_PATH not set in .env. Using current folder.")
    CSV_FILE = "garmin_activities.csv"

TOKEN_DIR = os.getenv("GARTH_DIR", ".garth")

START_DATE = os.getenv("START_DATE", "2023-01-01")  # how far back to go
CHUNK_DAYS = int(os.getenv("CHUNK_DAYS", "30"))      # range chunk size
SLEEP_BETWEEN_CHUNKS_S = float(os.getenv("SLEEP_BETWEEN_CHUNKS_S", "0.75"))

DETAIL_SLEEP_S = float(os.getenv("DETAIL_SLEEP_S", "0.15"))

FTP_LOOKBACK_DAYS = int(os.getenv("FTP_LOOKBACK_DAYS", "60"))
FTP_DETAIL_SLEEP_S = float(os.getenv("FTP_DETAIL_SLEEP_S", "0.10"))

# Common Garmin activity type keys we treat as cycling for detail pulls
CYCLING_TYPE_KEYS = {
    "cycling",
    "road_cycling",
    "gravel_cycling",
    "mountain_biking",
    "indoor_cycling",
    "virtual_ride",
    "spinning",
}

# -----------------------------
# CSV SCHEMA
# -----------------------------
DESIRED_FIELDS = [
    "activity_id",
    "date",
    "time",
    "start_time_local",
    "title",
    "activity_type",
    "distance_m",
    "duration_s",
    "calories",
    "avg_speed_mps",
    "max_speed_mps",
    "avg_pace_min_mile",
    "avg_hr",
    "max_hr",
    "running_cadence_spm",
    "cycling_cadence_rpm",
    "avg_power_w",
    "max_power_w",
    "elevation_gain_m",
    "aerobic_te",
    "anaerobic_te",
    "best_20m_watts",
    "ftp_watts",
    "ftp_source",
    "normalized_power_w",
    "intensity_factor",
    "tss",
]

# -----------------------------
# BASIC HELPERS
# -----------------------------
def ensure_folder(path: str) -> None:
    folder_path = os.path.dirname(path)
    if folder_path and not os.path.exists(folder_path):
        os.makedirs(folder_path)


def normalize_key(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "_")


def _norm_key(k: str) -> str:
    return "".join(ch for ch in (k or "").lower() if ch.isalnum())


def safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        x = float(v)
        return x if x > 0 else None
    except Exception:
        return None


def format_pace_min_mile(speed_mps: Optional[float]) -> Optional[str]:
    if not speed_mps or speed_mps <= 0:
        return None
    mins_per_mile = 26.8224 / float(speed_mps)
    minutes = int(mins_per_mile)
    seconds = int((mins_per_mile - minutes) * 60)
    return f"{minutes}:{seconds:02d}"


def intensity_factor(power_w: Optional[float], ftp_watts: Optional[float]) -> Optional[float]:
    try:
        if not power_w or not ftp_watts:
            return None
        return round(float(power_w) / float(ftp_watts), 3)
    except Exception:
        return None


def tss(duration_s: Optional[float], power_w: Optional[float], ftp_watts: Optional[float]) -> Optional[float]:
    try:
        if not duration_s or not power_w or not ftp_watts:
            return None
        if_val = float(power_w) / float(ftp_watts)
        return round((float(duration_s) * float(power_w) * if_val) / (float(ftp_watts) * 3600.0) * 100.0, 1)
    except Exception:
        return None


def parse_iso_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


# -----------------------------
# CSV HELPERS
# -----------------------------
def read_csv_header(csv_path: str) -> Optional[list]:
    if not os.path.isfile(csv_path):
        return None
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            return next(reader, None)
    except Exception:
        return None


def migrate_csv_to_schema(csv_path: str, desired_fields: list) -> None:
    header = read_csv_header(csv_path)
    if not header:
        return
    if header == desired_fields:
        return

    print("CSV header differs from desired schema: migrating file in place.")
    tmp_path = csv_path + ".tmp"

    existing_rows = []
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_rows.append(row)
    except Exception as e:
        print(f"WARNING: Could not read existing CSV for migration: {e}")
        return

    try:
        with open(tmp_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=desired_fields)
            writer.writeheader()
            for row in existing_rows:
                out = {k: None for k in desired_fields}
                for k in desired_fields:
                    if k in row:
                        out[k] = row.get(k)
                writer.writerow(out)

        os.replace(tmp_path, csv_path)
        print("Migration complete.")
    except Exception as e:
        print(f"WARNING: Migration failed: {e}")
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def load_existing_activity_ids(csv_path: str) -> Set[str]:
    existing_ids: Set[str] = set()
    if not os.path.isfile(csv_path):
        return existing_ids
    try:
        with open(csv_path, mode="r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                aid = row.get("activity_id")
                if aid:
                    existing_ids.add(str(aid))
    except Exception:
        pass
    return existing_ids


def append_rows(csv_path: str, rows: list) -> None:
    if not rows:
        return
    file_exists = os.path.isfile(csv_path)
    with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=DESIRED_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


# -----------------------------
# GARMIN PAYLOAD HELPERS
# -----------------------------
def normalize_activity_type(act: Dict[str, Any]) -> str:
    t = act.get("activityType")
    if isinstance(t, dict):
        return str(t.get("typeKey") or t.get("typeName") or "")
    return str(act.get("activityType") or act.get("activityTypeName") or act.get("activity_type") or "")


def coerce_activity_id(act: Dict[str, Any]) -> Optional[str]:
    v = act.get("activityId") or act.get("activity_id") or act.get("id")
    if v is None:
        return None
    return str(v)


def scan_for_keys(obj: Any, key_hints: Tuple[str, ...]) -> Optional[float]:
    if obj is None:
        return None

    if isinstance(obj, (int, float)):
        return safe_float(obj)

    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower()
            if any(h in kl for h in key_hints):
                val = safe_float(v)
                if val:
                    return val
            found = scan_for_keys(v, key_hints)
            if found:
                return found

    if isinstance(obj, list):
        for item in obj:
            found = scan_for_keys(item, key_hints)
            if found:
                return found

    return None


# -----------------------------
# FTP: STRICT EXTRACTOR + SANITY
# -----------------------------
def extract_ftp_watts_strict(obj: Any) -> Optional[float]:
    allowed = {
        "functionalthresholdpower",
        "latestfunctionalthresholdpower",
        "thresholdpower",
        "ftp",
        "ftpwatts",
        "cyclingftp",
    }

    def walk(x: Any) -> Optional[float]:
        if x is None:
            return None

        if isinstance(x, dict):
            for k, v in x.items():
                nk = _norm_key(str(k))
                if nk in allowed:
                    val = safe_float(v)
                    if val and 50 <= val <= 1200:
                        return val
                    if isinstance(v, dict):
                        vv = safe_float(v.get("value"))
                        if vv and 50 <= vv <= 1200:
                            return vv

                found = walk(v)
                if found:
                    return found

        if isinstance(x, list):
            for item in x:
                found = walk(item)
                if found:
                    return found

        return None

    return walk(obj)


def get_cycling_ftp_from_settings(api: Garmin) -> Optional[float]:
    fn = getattr(api, "get_cycling_ftp", None)
    if callable(fn):
        try:
            data = fn()
            if os.getenv("DEBUG_FTP") == "1":
                print("DEBUG_FTP method payload type:", type(data))
                if isinstance(data, dict):
                    print("DEBUG_FTP method keys:", list(data.keys())[:50])
            ftp = extract_ftp_watts_strict(data)
            if ftp:
                return ftp
        except Exception:
            pass

    try:
        data = garth.connectapi(
            "biometric-service/biometric/latestFunctionalThresholdPower/CYCLING",
            params={},
        )
        if os.getenv("DEBUG_FTP") == "1":
            print("DEBUG_FTP endpoint payload type:", type(data))
            if isinstance(data, dict):
                print("DEBUG_FTP endpoint keys:", list(data.keys())[:50])
        ftp = extract_ftp_watts_strict(data)
        if ftp:
            return ftp
    except Exception:
        pass

    return None


def fetch_activity_detail(api: Garmin, activity_id: str) -> Dict[str, Any]:
    fn = getattr(api, "get_activity_details", None)
    if callable(fn):
        try:
            d = fn(activity_id)
            if isinstance(d, dict):
                return d
        except Exception:
            pass

    try:
        d = garth.connectapi(f"activity-service/activity/{activity_id}", params={})
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def extract_best_20m_power_w(detail: Dict[str, Any]) -> Optional[float]:
    return scan_for_keys(
        detail,
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


def extract_normalized_power_w(detail: Dict[str, Any]) -> Optional[float]:
    return scan_for_keys(
        detail,
        (
            "normalizedpower",
            "weightedmeanpower",
            "weighted_power",
        ),
    )


def resolve_ftp(api: Garmin) -> Tuple[Optional[float], str, Optional[float]]:
    ftp = get_cycling_ftp_from_settings(api)
    if ftp:
        return ftp, "garmin_settings", None

    today = date.today()
    start = today - timedelta(days=FTP_LOOKBACK_DAYS)

    activities = None
    try:
        try:
            activities = api.get_activities_by_date(start.isoformat(), today.isoformat(), "")
        except Exception:
            activities = api.get_activities_by_date(start.isoformat(), today.isoformat(), None)
    except Exception:
        activities = None

    if not activities:
        return None, "missing", None

    best_20m = None
    checked = 0

    for act in activities:
        tkey = normalize_key(normalize_activity_type(act))
        if tkey not in {"virtual_ride", "indoor_cycling", "spinning"}:
            continue

        act_id = coerce_activity_id(act)
        if not act_id:
            continue

        detail = fetch_activity_detail(api, act_id)
        v = extract_best_20m_power_w(detail)
        if v and (best_20m is None or v > best_20m):
            best_20m = v

        checked += 1
        if FTP_DETAIL_SLEEP_S:
            time.sleep(FTP_DETAIL_SLEEP_S)
        if checked >= 60:
            break

    if not best_20m:
        return None, "missing", None

    ftp_est = 0.95 * best_20m
    if not (50 <= ftp_est <= 1200):
        return None, "missing", None

    return ftp_est, "virtual_ride_best_20m", best_20m


# -----------------------------
# ACTIVITY FETCH
# -----------------------------
def fetch_activities(api: Garmin, start_iso: str, end_iso: str) -> list:
    """
    Prefer broad fetch then filter locally.
    If broad fetch fails, fall back to common types and merge.
    """
    try:
        try:
            acts = api.get_activities_by_date(start_iso, end_iso, "")
            if acts is not None:
                return acts
        except Exception:
            pass

        acts = api.get_activities_by_date(start_iso, end_iso, None)
        if acts is not None:
            return acts
    except Exception:
        pass

    merged = []
    for t in ["running", "cycling", "indoor_cycling", "virtual_ride"]:
        try:
            a = api.get_activities_by_date(start_iso, end_iso, t)
            if a:
                merged.extend(a)
        except Exception:
            continue

    out = []
    seen: Set[str] = set()
    for act in merged:
        aid = coerce_activity_id(act)
        if not aid:
            continue
        if aid in seen:
            continue
        seen.add(aid)
        out.append(act)
    return out


# -----------------------------
# ROW BUILD
# -----------------------------
def to_row(
    act: Dict[str, Any],
    detail: Optional[Dict[str, Any]],
    ftp_watts: Optional[float],
    ftp_source: str,
) -> Dict[str, Any]:
    start_local = str(act.get("startTimeLocal", "") or "")
    date_str = start_local[:10] if len(start_local) >= 10 else ""
    time_str = start_local[11:] if len(start_local) > 11 else ""

    activity_id = coerce_activity_id(act)
    activity_type = normalize_activity_type(act)
    title = str(act.get("activityName") or "")

    dist_m = safe_float(act.get("distance"))
    dur_s = safe_float(act.get("duration"))
    calories = safe_float(act.get("calories"))

    avg_speed_mps = safe_float(act.get("averageSpeed"))
    max_speed_mps = safe_float(act.get("maxSpeed"))

    elev_gain_m = safe_float(act.get("totalElevationGain") or act.get("elevationGain"))

    avg_hr = safe_float(act.get("averageHR"))
    max_hr = safe_float(act.get("maxHR"))

    run_cad = safe_float(act.get("averageRunningCadenceInStepsPerMinute"))
    bike_cad = safe_float(act.get("averageCadence"))

    avg_power = safe_float(act.get("averagePower"))
    max_power = safe_float(act.get("maxPower"))

    aero_te = safe_float(act.get("aerobicTrainingEffect"))
    ana_te = safe_float(act.get("anaerobicTrainingEffect"))

    avg_pace = format_pace_min_mile(avg_speed_mps) if avg_speed_mps else None

    detail = detail or {}
    best_20m = extract_best_20m_power_w(detail) if detail else None
    np_w = extract_normalized_power_w(detail) if detail else None

    np_or_avg = np_w or avg_power

    if_val = intensity_factor(np_or_avg, ftp_watts) if ftp_watts else None
    tss_val = tss(dur_s, np_or_avg, ftp_watts) if ftp_watts else None

    row = {
        "activity_id": activity_id,
        "date": date_str,
        "time": time_str,
        "start_time_local": start_local,
        "title": title,
        "activity_type": activity_type,
        "distance_m": dist_m,
        "duration_s": dur_s,
        "calories": calories,
        "avg_speed_mps": avg_speed_mps,
        "max_speed_mps": max_speed_mps,
        "avg_pace_min_mile": avg_pace,
        "avg_hr": avg_hr,
        "max_hr": max_hr,
        "running_cadence_spm": run_cad,
        "cycling_cadence_rpm": bike_cad,
        "avg_power_w": avg_power,
        "max_power_w": max_power,
        "elevation_gain_m": elev_gain_m,
        "aerobic_te": aero_te,
        "anaerobic_te": ana_te,
        "best_20m_watts": best_20m,
        "ftp_watts": ftp_watts,
        "ftp_source": ftp_source,
        "normalized_power_w": np_w,
        "intensity_factor": if_val,
        "tss": tss_val,
    }
    return {k: row.get(k) for k in DESIRED_FIELDS}


# -----------------------------
# MAIN
# -----------------------------
def main() -> None:
    ensure_folder(CSV_FILE)

    if os.path.isfile(CSV_FILE):
        migrate_csv_to_schema(CSV_FILE, DESIRED_FIELDS)

    flt = load_activity_filter()
    existing_ids = load_existing_activity_ids(CSV_FILE)

    # Login using saved token session
    try:
        garth.resume(TOKEN_DIR)
        api = Garmin("dummy", "dummy")
        api.garth = garth.client
    except Exception as e:
        print(f"Login Error: {e}")
        return

    # Resolve FTP once
    ftp_watts, ftp_source, ftp_best_20m_used = resolve_ftp(api)
    print(f"FTP resolved: {ftp_watts} source={ftp_source} best_20m_used={ftp_best_20m_used}")

    start = parse_iso_date(START_DATE)
    end = date.today()

    print(f"Backfilling activities from {start} to {end} in {CHUNK_DAYS}-day chunks")
    print(f"Writing to: {CSV_FILE}")

    current = start
    total_written = 0
    total_skipped_dup = 0
    total_skipped_type = 0

    while current <= end:
        chunk_end = current + timedelta(days=CHUNK_DAYS)
        if chunk_end > end:
            chunk_end = end

        print(f"Processing {current.isoformat()} to {chunk_end.isoformat()} ...", end="", flush=True)

        activities = fetch_activities(api, current.isoformat(), chunk_end.isoformat())
        if not activities:
            print(" none.")
            current = chunk_end + timedelta(days=1)
            if SLEEP_BETWEEN_CHUNKS_S:
                time.sleep(SLEEP_BETWEEN_CHUNKS_S)
            continue

        new_rows = []
        for act in activities:
            act_id = coerce_activity_id(act)
            if not act_id:
                continue

            if act_id in existing_ids:
                total_skipped_dup += 1
                continue

            activity_type = normalize_activity_type(act)
            if not flt.allows(activity_type):
                total_skipped_type += 1
                continue

            tkey = normalize_key(activity_type)
            needs_detail = tkey in CYCLING_TYPE_KEYS

            detail = {}
            if needs_detail:
                detail = fetch_activity_detail(api, act_id)
                if DETAIL_SLEEP_S:
                    time.sleep(DETAIL_SLEEP_S)

            row = to_row(act, detail, ftp_watts, ftp_source)
            new_rows.append(row)
            existing_ids.add(act_id)

        if new_rows:
            new_rows.sort(key=lambda r: ((r.get("date") or ""), (r.get("time") or "")))
            append_rows(CSV_FILE, new_rows)
            total_written += len(new_rows)
            print(f" wrote {len(new_rows)}.")
        else:
            print(" wrote 0.")

        current = chunk_end + timedelta(days=1)

        if SLEEP_BETWEEN_CHUNKS_S:
            time.sleep(SLEEP_BETWEEN_CHUNKS_S)

    print("--- HISTORY PULL COMPLETE ---")
    print(f"--- WROTE {total_written} NEW ACTIVITIES ---")
    print(f"--- SKIPPED DUPES {total_skipped_dup}, SKIPPED TYPE {total_skipped_type} ---")
    print(f"--- FIND DATA IN {CSV_FILE} ---")


if __name__ == "__main__":
    main()
