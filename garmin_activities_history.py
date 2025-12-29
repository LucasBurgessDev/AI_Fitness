import garth
from garminconnect import Garmin
from datetime import date, timedelta
import csv
import os
import time
from dotenv import load_dotenv

from activity_filter import load_activity_filter

# --- CONFIGURATION ---
load_dotenv()
TOKEN_DIR = ".garth"
SAVE_PATH = os.getenv("SAVE_PATH", "").strip()
START_DATE = os.getenv("START_DATE", "2023-01-01").strip()  # How far back to go
CHUNK_DAYS = int(os.getenv("CHUNK_DAYS", "30"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "1.0"))

CSV_NAME = "garmin_activities.csv"
CSV_FILE = os.path.join(SAVE_PATH, CSV_NAME) if SAVE_PATH else CSV_NAME

print(f"--- SAVE PATH: {CSV_FILE} ---")
print(f"--- START DATE: {START_DATE} ---")
# ---------------------


def get_safe(data, *keys):
    try:
        for key in keys:
            if data is None:
                return None
            data = data[key]
        return data
    except (KeyError, TypeError, AttributeError, IndexError):
        return None


def pick(summary: dict, *keys, default=None):
    for k in keys:
        v = summary.get(k)
        if v is not None:
            return v
    return default


def norm_type(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "_")


def get_activity_type_key(act: dict) -> str:
    # Common shapes from garminconnect activity lists
    t = get_safe(act, "activityType", "typeKey")
    if t:
        return str(t)

    # Sometimes already flattened
    t = act.get("activityType")
    if isinstance(t, str) and t:
        return t

    # Another fallback
    t = act.get("activityTypeName")
    if t:
        return str(t)

    return ""


def format_pace_min_per_mile(speed_mps):
    # meters per second to min per mile
    if not speed_mps or speed_mps <= 0:
        return None
    mins_per_mile = 26.8224 / speed_mps
    minutes = int(mins_per_mile)
    seconds = int((mins_per_mile - minutes) * 60)
    return f"{minutes}:{seconds:02d}"


def ensure_folder(path: str):
    folder_path = os.path.dirname(path)
    if folder_path and not os.path.exists(folder_path):
        os.makedirs(folder_path, exist_ok=True)


def load_existing_ids(csv_path: str) -> set:
    if not os.path.isfile(csv_path):
        return set()
    ids = set()
    with open(csv_path, mode="r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("activity_id"):
                ids.add(row["activity_id"])
    return ids


def write_header_if_missing(csv_path: str, fieldnames: list):
    if os.path.isfile(csv_path):
        return
    ensure_folder(csv_path)
    with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()


def append_rows(csv_path: str, fieldnames: list, rows: list[dict]):
    if not rows:
        return
    ensure_folder(csv_path)
    with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerows(rows)


def main():
    flt = load_activity_filter()

    print("1. Loading tokens...")
    garth.resume(TOKEN_DIR)
    api = Garmin("dummy", "dummy")
    api.garth = garth.client
    try:
        api.display_name = api.garth.profile["displayName"]
    except Exception:
        pass

    print(f"2. Fetching activities from {START_DATE}...")
    start = date.fromisoformat(START_DATE)
    end = date.today()

    fieldnames = [
        "activity_id",
        "start_time_local",
        "date",
        "time",
        "title",
        "activity_type",
        "distance_m",
        "distance_km",
        "distance_mi",
        "duration_s",
        "duration_min",
        "avg_speed_mps",
        "max_speed_mps",
        "avg_pace_min_mi",
        "avg_hr",
        "max_hr",
        "elevation_gain_m",
        "elevation_gain_ft",
        "calories",
        "avg_cadence_rpm",
        "avg_power_w",
        "max_power_w",
        "aerobic_te",
        "anaerobic_te",
    ]

    write_header_if_missing(CSV_FILE, fieldnames)
    existing_ids = load_existing_ids(CSV_FILE)
    print(f"   Existing rows: {len(existing_ids)}")

    current = start
    total_added = 0
    total_seen = 0
    total_kept = 0

    while current <= end:
        chunk_end = current + timedelta(days=CHUNK_DAYS)
        if chunk_end > end:
            chunk_end = end

        print(f"   Processing {current} to {chunk_end}:", end=" ", flush=True)

        try:
            # Preferred: fetch all, then filter by metadata
            try:
                activities = api.get_activities_by_date(current.isoformat(), chunk_end.isoformat())
            except TypeError:
                # Fallback if library requires a type: fetch broad types then filter
                activities = []
                for broad in ["running", "cycling"]:
                    try:
                        activities.extend(
                            api.get_activities_by_date(current.isoformat(), chunk_end.isoformat(), broad) or []
                        )
                    except Exception:
                        pass

            activities = activities or []
            total_seen += len(activities)

            new_rows = []
            for act in activities:
                act_type = get_activity_type_key(act)
                if not flt.allows(act_type):
                    continue

                total_kept += 1

                activity_id = pick(act, "activityId", "activity_id", "id")
                if activity_id is None:
                    continue
                activity_id = str(activity_id)

                if activity_id in existing_ids:
                    continue

                start_local = pick(act, "startTimeLocal", "start_time_local", default="")
                date_str = start_local[:10] if start_local else ""
                time_str = start_local[11:] if len(start_local) > 11 else ""

                title = pick(act, "activityName", "title", default="Activity")

                dist_m = pick(act, "distance", default=0) or 0
                dist_km = round(dist_m / 1000.0, 3) if dist_m else 0
                dist_mi = round(dist_m * 0.000621371, 3) if dist_m else 0

                dur_s = pick(act, "duration", default=0) or 0
                dur_min = round(dur_s / 60.0, 2) if dur_s else 0

                avg_speed_mps = pick(act, "averageSpeed", "avg_speed_mps")
                max_speed_mps = pick(act, "maxSpeed", "max_speed_mps")
                avg_pace = format_pace_min_per_mile(avg_speed_mps) if norm_type(act_type).find("running") != -1 else None

                avg_hr = pick(act, "averageHR", "avg_hr")
                max_hr = pick(act, "maxHR", "max_hr")

                elev_m = pick(act, "totalElevationGain", "elevationGain", "elevation_gain_m", default=0) or 0
                elev_ft = round(elev_m * 3.28084, 0) if elev_m else 0

                calories = pick(act, "calories")

                # Cadence: running cadence or cycling cadence
                cadence = pick(
                    act,
                    "averageRunningCadenceInStepsPerMinute",
                    "averageCadence",
                    "avg_cadence_rpm",
                )

                # Power (often present for cycling if power meter or trainer)
                avg_power = pick(act, "averagePower", "avg_power_w")
                max_power = pick(act, "maxPower", "max_power_w")

                aerobic_te = pick(act, "aerobicTrainingEffect", "aerobic_te")
                anaerobic_te = pick(act, "anaerobicTrainingEffect", "anaerobic_te")

                row = {
                    "activity_id": activity_id,
                    "start_time_local": start_local,
                    "date": date_str,
                    "time": time_str,
                    "title": title,
                    "activity_type": act_type,
                    "distance_m": dist_m,
                    "distance_km": dist_km,
                    "distance_mi": dist_mi,
                    "duration_s": dur_s,
                    "duration_min": dur_min,
                    "avg_speed_mps": avg_speed_mps,
                    "max_speed_mps": max_speed_mps,
                    "avg_pace_min_mi": avg_pace,
                    "avg_hr": avg_hr,
                    "max_hr": max_hr,
                    "elevation_gain_m": elev_m,
                    "elevation_gain_ft": elev_ft,
                    "calories": calories,
                    "avg_cadence_rpm": cadence,
                    "avg_power_w": avg_power,
                    "max_power_w": max_power,
                    "aerobic_te": aerobic_te,
                    "anaerobic_te": anaerobic_te,
                }

                new_rows.append(row)
                existing_ids.add(activity_id)

            if new_rows:
                new_rows.sort(key=lambda r: (r.get("date") or "", r.get("time") or ""))
                append_rows(CSV_FILE, fieldnames, new_rows)
                total_added += len(new_rows)
                print(f"Saved {len(new_rows)} activities.")
            else:
                print("No new activities.")

        except Exception as e:
            print(f"Error: {e}")

        current = chunk_end + timedelta(days=1)
        time.sleep(SLEEP_SECONDS)

    print("--- HISTORY PULL COMPLETE ---")
    print(f"--- TOTAL SEEN: {total_seen} ---")
    print(f"--- TOTAL KEPT BY FILTER: {total_kept} ---")
    print(f"--- TOTAL NEW WRITTEN: {total_added} ---")
    print(f"--- FIND DATA IN {CSV_FILE} ---")


if __name__ == "__main__":
    main()
