import garth
from garminconnect import Garmin
from datetime import date, timedelta
import csv
import os
import time
import random
from dotenv import load_dotenv

from activity_filter import load_activity_filter

# --- CONFIGURATION VIA ENVIRONMENT ---
load_dotenv()
SAVE_PATH = os.getenv("SAVE_PATH")

if SAVE_PATH:
    CSV_FILE = os.path.join(SAVE_PATH, "garmin_stats.csv")
else:
    print("WARNING: SAVE_PATH not set in .env. Using current folder.")
    CSV_FILE = "garmin_stats.csv"

TOKEN_DIR = os.getenv("GARTH_DIR", ".garth")
START_DATE = os.getenv("START_DATE", "2023-01-01")
# -------------------------------------


HEADERS = [
    "Date",
    "Weight (lbs)", "Muscle Mass (lbs)", "Body Fat %", "Water %",
    "Sleep Total (hr)", "Sleep Deep (hr)", "Sleep REM (hr)", "Sleep Score",
    "RHR", "Min HR", "Max HR", "Avg Stress", "Respiration", "SpO2",
    "VO2 Max", "Training Status", "HRV Status", "HRV Avg",
    "Steps", "Step Goal", "Cals Total", "Cals Active",
    "Activities",
]


def get_safe(data, *keys):
    try:
        for key in keys:
            data = data[key]
        return data
    except (KeyError, TypeError, AttributeError):
        return None


def ensure_folder(path):
    folder_path = os.path.dirname(path)
    if folder_path and not os.path.exists(folder_path):
        os.makedirs(folder_path)


def load_existing_dates(csv_path):
    dates = set()
    if not os.path.isfile(csv_path):
        return dates
    try:
        with open(csv_path, mode="r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if row and row[0]:
                    dates.add(row[0])
    except Exception:
        pass
    return dates


def activity_type_key(act):
    t = act.get("activityType")
    if isinstance(t, dict):
        return (t.get("typeKey") or t.get("typeName") or "")
    return str(act.get("activityType") or act.get("activityTypeName") or act.get("activity_type") or "")


def build_activity_str(api, day_str, flt):
    out = ""
    try:
        acts = None
        try:
            acts = api.get_activities_by_date(day_str, day_str, "")
        except Exception:
            acts = api.get_activities_by_date(day_str, day_str, None)

        if acts:
            names = []
            for act in acts:
                tkey = activity_type_key(act)
                if flt.allows(tkey):
                    names.append(f"{act.get('activityName', 'Activity')} ({str(tkey).lower().replace(' ', '_')})")
            out = "; ".join(names)
    except Exception:
        pass
    return out


def main():
    ensure_folder(CSV_FILE)

    # Ensure header exists
    if not os.path.isfile(CSV_FILE):
        with open(CSV_FILE, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(HEADERS)

    existing_dates = load_existing_dates(CSV_FILE)

    # Login
    try:
        garth.resume(TOKEN_DIR)
        api = Garmin("dummy", "dummy")
        api.garth = garth.client
        try:
            api.display_name = api.garth.profile["displayName"]
        except Exception:
            pass
    except Exception as e:
        print(f"Login failed: {e}")
        return

    flt = load_activity_filter()

    start = date.fromisoformat(START_DATE)
    end = date.today() - timedelta(days=1)  # leave today for daily script

    print("--- STARTING HISTORY PULL ---")
    print(f"From {start} to {end}")
    print("Press Ctrl+C to stop at any time.")

    current_date = start
    while current_date <= end:
        day_str = current_date.isoformat()

        if day_str in existing_dates:
            print(f"Processing {day_str}... skipped (already in CSV).")
            current_date += timedelta(days=1)
            continue

        print(f"Processing {day_str}...", end="", flush=True)

        try:
            # Core
            try:
                user_stats = api.get_user_summary(day_str)
                rhr = get_safe(user_stats, "restingHeartRate")
                min_hr = get_safe(user_stats, "minHeartRate")
                max_hr = get_safe(user_stats, "maxHeartRate")
                stress = get_safe(user_stats, "averageStressLevel")
                steps = get_safe(user_stats, "totalSteps")
                vo2 = get_safe(user_stats, "vo2Max")
                spo2 = get_safe(user_stats, "averageSpO2")
                resp = get_safe(user_stats, "averageRespirationValue")
                cals_tot = get_safe(user_stats, "totalKilocalories")
                cals_act = get_safe(user_stats, "activeKilocalories")
                step_goal = get_safe(user_stats, "dailyStepGoal")
            except Exception:
                rhr = min_hr = max_hr = stress = steps = vo2 = spo2 = resp = cals_tot = cals_act = step_goal = None

            # Sleep
            try:
                sleep_data = api.get_sleep_data(day_str)
                s_tot = get_safe(sleep_data, "dailySleepDTO", "sleepTimeSeconds")
                s_deep = get_safe(sleep_data, "dailySleepDTO", "deepSleepSeconds")
                s_rem = get_safe(sleep_data, "dailySleepDTO", "remSleepSeconds")
                s_score = get_safe(sleep_data, "dailySleepDTO", "sleepScores", "overall", "value")
                if s_tot:
                    s_tot = round(s_tot / 3600, 2)
                if s_deep:
                    s_deep = round(s_deep / 3600, 2)
                if s_rem:
                    s_rem = round(s_rem / 3600, 2)
            except Exception:
                s_tot = s_deep = s_rem = s_score = None

            # Training Status
            t_status = None
            try:
                if hasattr(api, "get_training_status"):
                    ts = api.get_training_status(day_str)
                    t_status = get_safe(ts, "mostRecentTerminatedTrainingStatus", "status")
            except Exception:
                pass

            # Body Comp
            wt = mus = fat = h2o = None
            try:
                bc = api.get_body_composition(day_str)
                if bc and "totalAverage" in bc:
                    avg = bc["totalAverage"]
                    if avg.get("weight"):
                        wt = round(avg.get("weight") / 453.592, 1)
                    if avg.get("muscleMass"):
                        mus = round(avg.get("muscleMass") / 453.592, 1)
                    fat = avg.get("bodyFat")
                    h2o = avg.get("bodyWater")
            except Exception:
                pass

            # HRV
            hrv_s = hrv_a = None
            try:
                if hasattr(api, "get_hrv_data"):
                    h = api.get_hrv_data(day_str)
                else:
                    h = api.connectapi(f"/hrv-service/hrv/daily/{day_str}")
                hrv_s = get_safe(h, "hrvSummary", "status")
                hrv_a = get_safe(h, "hrvSummary", "weeklyAverage")
            except Exception:
                pass

            # Activities (filtered)
            act_str = build_activity_str(api, day_str, flt)

            row = [
                day_str, wt, mus, fat, h2o, s_tot, s_deep, s_rem, s_score,
                rhr, min_hr, max_hr, stress, resp, spo2, vo2, t_status, hrv_s, hrv_a,
                steps, step_goal, cals_tot, cals_act, act_str
            ]

            with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(row)

            existing_dates.add(day_str)
            print(" done.")

        except Exception as e:
            print(f" failed ({e})")

        current_date += timedelta(days=1)
        time.sleep(random.uniform(0.8, 1.8))

    print("--- HISTORY PULL COMPLETE ---")


if __name__ == "__main__":
    main()
