#!/usr/bin/env python3
import garth
from garminconnect import Garmin
from datetime import date, timedelta
import csv
import os
import time
from dotenv import load_dotenv
from activity_filter import load_activity_filter

# --- CONFIGURATION VIA ENVIRONMENT ---
load_dotenv()

SAVE_PATH = os.getenv("SAVE_PATH")
if SAVE_PATH:
    CSV_FILE = os.path.join(SAVE_PATH, "garmin_activities.csv")
else:
    print("WARNING: SAVE_PATH not set in .env. Using current folder.")
    CSV_FILE = "garmin_activities.csv"

TOKEN_DIR = os.getenv("GARTH_DIR", ".garth")

# How many days back to check each run, keep a bit of overlap for late sync
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "14"))

# Optional throttle between detail calls
DETAIL_SLEEP_S = float(os.getenv("DETAIL_SLEEP_S", "0.15"))
# -------------------------------------


def get_safe(data, *keys):
    try:
        for key in keys:
            data = data[key]
        return data
    except (KeyError, TypeError, AttributeError):
        return None


def format_pace_min_mile(speed_mps):
    """Converts meters/sec to Min/Mile (e.g., 8:30). Useful for runs."""
    if not speed_mps or speed_mps <= 0:
        return None
    mins_per_mile = 26.8224 / speed_mps
    minutes = int(mins_per_mile)
    seconds = int((mins_per_mile - minutes) * 60)
    return f"{minutes}:{seconds:02d}"


def ensure_folder(path):
    folder_path = os.path.dirname(path)
    if folder_path and not os.path.exists(folder_path):
        os.makedirs(folder_path)


def load_existing_activity_ids(csv_path):
    """
    Prefer de-dupe by Garmin activity_id. Falls back to date+time signature
    if older files exist without activity_id (should not happen once we switch).
    """
    existing_ids = set()
    existing_sig = set()

    if not os.path.isfile(csv_path):
        return existing_ids, existing_sig

    try:
        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return existing_ids, existing_sig

            has_activity_id = "activity_id" in reader.fieldnames
            has_date_time = ("date" in reader.fieldnames) and ("time" in reader.fieldnames)

            for row in reader:
                if has_activity_id and row.get("activity_id"):
                    existing_ids.add(str(row["activity_id"]))
                elif has_date_time:
                    ds = row.get("date", "")
                    ts = row.get("time", "")
                    if ds and ts:
                        existing_sig.add(f"{ds}_{ts}")
    except Exception:
        pass

    return existing_ids, existing_sig


def normalize_activity_type(act):
    """
    Garminconnect sometimes returns activityType as dict, sometimes string.
    We return a string label.
    """
    t = act.get("activityType")
    if isinstance(t, dict):
        # common shape: {"typeKey":"running","typeName":"Running",...}
        return t.get("typeKey") or t.get("typeName") or ""
    return act.get("activityType") or act.get("activityTypeName") or act.get("activity_type") or ""


def to_row(act):
    """
    Build a unified activity row, leaving blanks for non-applicable fields.
    We write in meters, seconds, mps, watts, rpm to keep it consistent.
    """
    start_local = act.get("startTimeLocal", "") or ""
    date_str = start_local[:10] if len(start_local) >= 10 else ""
    time_str = start_local[11:] if len(start_local) > 11 else ""

    activity_id = act.get("activityId") or act.get("activity_id") or act.get("id")
    activity_type = normalize_activity_type(act)
    title = act.get("activityName") or ""

    dist_m = act.get("distance")
    dur_s = act.get("duration")
    calories = act.get("calories")

    avg_speed_mps = act.get("averageSpeed")
    max_speed_mps = act.get("maxSpeed")

    elev_gain_m = act.get("totalElevationGain") or act.get("elevationGain")

    avg_hr = act.get("averageHR")
    max_hr = act.get("maxHR")

    # Running cadence key in your current script:
    run_cad = act.get("averageRunningCadenceInStepsPerMinute")

    # Cycling cadence and power often differ by key:
    bike_cad = act.get("averageCadence")
    avg_power = act.get("averagePower")
    max_power = act.get("maxPower")

    # Training effect:
    aero_te = act.get("aerobicTrainingEffect")
    ana_te = act.get("anaerobicTrainingEffect")

    # Pace for runs (min/mile) from avg_speed_mps
    avg_pace = format_pace_min_mile(avg_speed_mps) if avg_speed_mps else None

    return {
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

        # placeholders for step 4
        "ftp_watts": None,
        "ftp_source": None,
        "normalized_power_w": None,
        "intensity_factor": None,
        "tss": None,
    }


def main():
    # Ensure output folder exists
    ensure_folder(CSV_FILE)

    # Load filter config
    flt = load_activity_filter()

    # Load existing IDs for de-dupe
    existing_ids, existing_sig = load_existing_activity_ids(CSV_FILE)

    # Login
    try:
        garth.resume(TOKEN_DIR)
        api = Garmin("dummy", "dummy")
        api.garth = garth.client
    except Exception as e:
        print(f"Login Error: {e}")
        return

    # Lookback window
    today = date.today()
    start_check = today - timedelta(days=LOOKBACK_DAYS)

    print(f"Checking activities from {start_check} to {today} (lookback {LOOKBACK_DAYS} days)...")
    print(f"Writing to: {CSV_FILE}")

    # Pull activities: we fetch without restricting to "running" so we can include cycling too.
    # get_activities_by_date signature in garminconnect: (start, end, activityType)
    # Some versions allow None/"" for activityType, others require a string.
    activities = None
    try:
        try:
            activities = api.get_activities_by_date(start_check.isoformat(), today.isoformat(), "")
        except Exception:
            activities = api.get_activities_by_date(start_check.isoformat(), today.isoformat(), None)
    except Exception as e:
        print(f"Fetch Error: {e}")
        return

    if not activities:
        print("No activities returned.")
        return

    new_rows = []
    skipped_dupe = 0
    skipped_type = 0

    for act in activities:
        activity_type = normalize_activity_type(act)

        if not flt.allows(activity_type):
            skipped_type += 1
            continue

        # Prefer de-dupe by activity_id
        activity_id = act.get("activityId") or act.get("activity_id") or act.get("id")
        if activity_id is not None and str(activity_id) in existing_ids:
            skipped_dupe += 1
            continue

        # Fallback: date_time signature
        start_local = act.get("startTimeLocal", "") or ""
        date_str = start_local[:10]
        time_str = start_local[11:]
        sig = f"{date_str}_{time_str}"
        if sig in existing_sig:
            skipped_dupe += 1
            continue

        row = to_row(act)
        new_rows.append(row)

        if activity_id is not None:
            existing_ids.add(str(activity_id))
        existing_sig.add(sig)

    if not new_rows:
        print(f"No new filtered activities found. skipped_type={skipped_type}, skipped_dupe={skipped_dupe}")
        return

    # Sort by start time when possible
    def sort_key(r):
        return (r.get("date") or "", r.get("time") or "")

    new_rows.sort(key=sort_key)

    # Write
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(new_rows[0].keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)

    print(
        f"SUCCESS: Added {len(new_rows)} new activities. "
        f"skipped_type={skipped_type}, skipped_dupe={skipped_dupe}"
    )


if __name__ == "__main__":
    main()
