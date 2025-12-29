import garth
from garminconnect import Garmin
from datetime import date
import csv
import os
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


def activity_type_key(act):
    t = act.get("activityType")
    if isinstance(t, dict):
        return (t.get("typeKey") or t.get("typeName") or "")
    return str(act.get("activityType") or act.get("activityTypeName") or act.get("activity_type") or "")


def build_activity_str(api, day_str, flt):
    """
    Pulls activities for the day, then filters using activity_filters.yaml.
    """
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

    try:
        print("1. Loading tokens...")
        garth.resume(TOKEN_DIR)

        api = Garmin("dummy", "dummy")
        api.garth = garth.client
        try:
            api.display_name = api.garth.profile["displayName"]
        except Exception:
            pass

        flt = load_activity_filter()

        today = date.today().isoformat()
        print(f"2. Pulling data for {today}...")

        # 1) Core Biometrics
        try:
            user_stats = api.get_user_summary(today)
            rhr = get_safe(user_stats, "restingHeartRate")
            min_hr = get_safe(user_stats, "minHeartRate")
            max_hr = get_safe(user_stats, "maxHeartRate")
            stress_avg = get_safe(user_stats, "averageStressLevel")
            steps = get_safe(user_stats, "totalSteps")
            vo2_max = get_safe(user_stats, "vo2Max")
            spo2_avg = get_safe(user_stats, "averageSpO2")
            respiration_avg = get_safe(user_stats, "averageRespirationValue")
            cals_total = get_safe(user_stats, "totalKilocalories")
            cals_active = get_safe(user_stats, "activeKilocalories")
            step_goal = get_safe(user_stats, "dailyStepGoal")
        except Exception:
            rhr = min_hr = max_hr = stress_avg = steps = vo2_max = spo2_avg = respiration_avg = cals_total = cals_active = step_goal = None

        # 2) Sleep
        try:
            sleep_data = api.get_sleep_data(today)
            sleep_total = get_safe(sleep_data, "dailySleepDTO", "sleepTimeSeconds")
            sleep_deep = get_safe(sleep_data, "dailySleepDTO", "deepSleepSeconds")
            sleep_rem = get_safe(sleep_data, "dailySleepDTO", "remSleepSeconds")
            sleep_score = get_safe(sleep_data, "dailySleepDTO", "sleepScores", "overall", "value")

            if sleep_total:
                sleep_total = round(sleep_total / 3600, 2)
            if sleep_deep:
                sleep_deep = round(sleep_deep / 3600, 2)
            if sleep_rem:
                sleep_rem = round(sleep_rem / 3600, 2)
        except Exception:
            sleep_total = sleep_deep = sleep_rem = sleep_score = None

        # 3) Training Status
        training_status = None
        try:
            if hasattr(api, "get_training_status"):
                t_status = api.get_training_status(today)
                training_status = get_safe(t_status, "mostRecentTerminatedTrainingStatus", "status")
        except Exception:
            pass

        # 4) Body Comp
        weight = muscle_mass = fat_pct = water_pct = None
        try:
            body_comp = api.get_body_composition(today)
            if body_comp and "totalAverage" in body_comp:
                avg = body_comp["totalAverage"]
                w_g = avg.get("weight")
                if w_g:
                    weight = round(w_g / 453.592, 1)
                m_g = avg.get("muscleMass")
                if m_g:
                    muscle_mass = round(m_g / 453.592, 1)
                fat_pct = avg.get("bodyFat")
                water_pct = avg.get("bodyWater")
        except Exception:
            pass

        # 5) HRV
        hrv_status = hrv_avg = None
        try:
            if hasattr(api, "get_hrv_data"):
                h = api.get_hrv_data(today)
            else:
                h = api.connectapi(f"/hrv-service/hrv/daily/{today}")
            hrv_status = get_safe(h, "hrvSummary", "status")
            hrv_avg = get_safe(h, "hrvSummary", "weeklyAverage")
        except Exception:
            pass

        # 6) Activities (filtered)
        activity_str = build_activity_str(api, today, flt)

        new_row = [
            today,
            weight, muscle_mass, fat_pct, water_pct,
            sleep_total, sleep_deep, sleep_rem, sleep_score,
            rhr, min_hr, max_hr, stress_avg, respiration_avg, spo2_avg,
            vo2_max, training_status, hrv_status, hrv_avg,
            steps, step_goal, cals_total, cals_active,
            activity_str,
        ]

        # --- SMART SAVE: replace today's row if already present ---
        rows = []
        file_exists = os.path.isfile(CSV_FILE)

        if file_exists:
            try:
                with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    all_data = list(reader)
                    if all_data:
                        rows = [row for row in all_data[1:] if row and row[0] != today]
            except Exception as e:
                print(f"Warning reading existing CSV: {e}")

        rows.append(new_row)
        rows.sort(key=lambda x: x[0])

        with open(CSV_FILE, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(HEADERS)
            writer.writerows(rows)

        print(f"SUCCESS: Saved data for {today} to {CSV_FILE}")

    except Exception as e:
        print(f"Global Error: {e}")


if __name__ == "__main__":
    main()
