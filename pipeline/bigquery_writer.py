from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import pandas as pd
from google.cloud import bigquery

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

_STATS_SCHEMA = [
    bigquery.SchemaField("run_date", "DATE"),
    bigquery.SchemaField("batch_id", "STRING"),
    bigquery.SchemaField("date", "STRING"),
    bigquery.SchemaField("timestamp", "STRING"),
    bigquery.SchemaField("weight_lbs", "FLOAT64"),
    bigquery.SchemaField("muscle_mass_lbs", "FLOAT64"),
    bigquery.SchemaField("body_fat_pct", "FLOAT64"),
    bigquery.SchemaField("water_pct", "FLOAT64"),
    bigquery.SchemaField("sleep_total_hr", "FLOAT64"),
    bigquery.SchemaField("sleep_deep_hr", "FLOAT64"),
    bigquery.SchemaField("sleep_rem_hr", "FLOAT64"),
    bigquery.SchemaField("sleep_light_hr", "FLOAT64"),
    bigquery.SchemaField("sleep_awake_hr", "FLOAT64"),
    bigquery.SchemaField("sleep_score", "INT64"),
    bigquery.SchemaField("rhr", "INT64"),
    bigquery.SchemaField("min_hr", "INT64"),
    bigquery.SchemaField("max_hr", "INT64"),
    bigquery.SchemaField("avg_stress", "INT64"),
    bigquery.SchemaField("body_battery", "INT64"),
    bigquery.SchemaField("body_battery_high", "INT64"),
    bigquery.SchemaField("body_battery_low", "INT64"),
    bigquery.SchemaField("respiration", "FLOAT64"),
    bigquery.SchemaField("spo2", "FLOAT64"),
    bigquery.SchemaField("vo2_max", "FLOAT64"),
    bigquery.SchemaField("training_status", "STRING"),
    bigquery.SchemaField("hrv_status", "STRING"),
    bigquery.SchemaField("hrv_avg", "FLOAT64"),
    bigquery.SchemaField("training_readiness", "INT64"),
    bigquery.SchemaField("fitness_age", "INT64"),
    bigquery.SchemaField("steps", "INT64"),
    bigquery.SchemaField("step_goal", "INT64"),
    bigquery.SchemaField("floors_climbed", "INT64"),
    bigquery.SchemaField("cals_total", "INT64"),
    bigquery.SchemaField("cals_active", "INT64"),
    bigquery.SchemaField("intensity_moderate_mins", "INT64"),
    bigquery.SchemaField("intensity_vigorous_mins", "INT64"),
    bigquery.SchemaField("race_5k_secs", "INT64"),
    bigquery.SchemaField("race_10k_secs", "INT64"),
    bigquery.SchemaField("race_half_secs", "INT64"),
    bigquery.SchemaField("race_full_secs", "INT64"),
    # Training load (ATL/CTL/TSB)
    bigquery.SchemaField("atl", "FLOAT64"),
    bigquery.SchemaField("ctl", "FLOAT64"),
    bigquery.SchemaField("tsb", "FLOAT64"),
    bigquery.SchemaField("tl_aerobic_pct", "FLOAT64"),
    # Lactate threshold
    bigquery.SchemaField("lactate_threshold_hr", "INT64"),
    bigquery.SchemaField("lactate_threshold_pace", "FLOAT64"),
    bigquery.SchemaField("activities", "STRING"),
]

_ACTIVITIES_SCHEMA = [
    bigquery.SchemaField("run_date", "DATE"),
    bigquery.SchemaField("batch_id", "STRING"),
    bigquery.SchemaField("activity_id", "STRING"),
    bigquery.SchemaField("date", "STRING"),
    bigquery.SchemaField("time", "STRING"),
    bigquery.SchemaField("start_time_local", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("activity_type", "STRING"),
    bigquery.SchemaField("distance_m", "FLOAT64"),
    bigquery.SchemaField("duration_s", "FLOAT64"),
    bigquery.SchemaField("calories", "INT64"),
    bigquery.SchemaField("avg_speed_mps", "FLOAT64"),
    bigquery.SchemaField("max_speed_mps", "FLOAT64"),
    bigquery.SchemaField("avg_pace_min_mile", "FLOAT64"),
    bigquery.SchemaField("avg_hr", "INT64"),
    bigquery.SchemaField("max_hr", "INT64"),
    bigquery.SchemaField("running_cadence_spm", "INT64"),
    bigquery.SchemaField("cycling_cadence_rpm", "INT64"),
    bigquery.SchemaField("avg_power_w", "FLOAT64"),
    bigquery.SchemaField("max_power_w", "FLOAT64"),
    bigquery.SchemaField("elevation_gain_m", "FLOAT64"),
    bigquery.SchemaField("aerobic_te", "FLOAT64"),
    bigquery.SchemaField("anaerobic_te", "FLOAT64"),
    bigquery.SchemaField("best_20m_watts", "FLOAT64"),
    bigquery.SchemaField("ftp_watts", "FLOAT64"),
    bigquery.SchemaField("ftp_source", "STRING"),
    bigquery.SchemaField("normalized_power_w", "FLOAT64"),
    bigquery.SchemaField("intensity_factor", "FLOAT64"),
    bigquery.SchemaField("tss", "FLOAT64"),
    # Recovery & readiness
    bigquery.SchemaField("recovery_time_s", "FLOAT64"),
    bigquery.SchemaField("vo2max_activity", "FLOAT64"),
    bigquery.SchemaField("performance_condition", "FLOAT64"),
    # Heart rate zones
    bigquery.SchemaField("hr_zone_1_secs", "FLOAT64"),
    bigquery.SchemaField("hr_zone_2_secs", "FLOAT64"),
    bigquery.SchemaField("hr_zone_3_secs", "FLOAT64"),
    bigquery.SchemaField("hr_zone_4_secs", "FLOAT64"),
    bigquery.SchemaField("hr_zone_5_secs", "FLOAT64"),
    # Running dynamics
    bigquery.SchemaField("ground_contact_time_ms", "FLOAT64"),
    bigquery.SchemaField("vertical_oscillation_mm", "FLOAT64"),
    bigquery.SchemaField("stride_length_m", "FLOAT64"),
    bigquery.SchemaField("vertical_ratio_pct", "FLOAT64"),
    # Environment
    bigquery.SchemaField("avg_temp_c", "FLOAT64"),
    bigquery.SchemaField("humidity_pct", "FLOAT64"),
    # Training effect labels
    bigquery.SchemaField("aerobic_te_label", "STRING"),
    bigquery.SchemaField("anaerobic_te_label", "STRING"),
    # Cardiac efficiency
    bigquery.SchemaField("aerobic_decoupling_pct", "FLOAT64"),
    # Power zones (cycling)
    bigquery.SchemaField("power_zone_1_secs", "FLOAT64"),
    bigquery.SchemaField("power_zone_2_secs", "FLOAT64"),
    bigquery.SchemaField("power_zone_3_secs", "FLOAT64"),
    bigquery.SchemaField("power_zone_4_secs", "FLOAT64"),
    bigquery.SchemaField("power_zone_5_secs", "FLOAT64"),
]

# Mapping from garmin_stats CSV headers (human-readable) → BQ column names
_CSV_TO_STATS_COLS = {
    "Date": "date",
    "Timestamp": "timestamp",
    "Weight (lbs)": "weight_lbs",
    "Muscle Mass (lbs)": "muscle_mass_lbs",
    "Body Fat %": "body_fat_pct",
    "Water %": "water_pct",
    "Sleep Total (hr)": "sleep_total_hr",
    "Sleep Deep (hr)": "sleep_deep_hr",
    "Sleep REM (hr)": "sleep_rem_hr",
    "Sleep Light (hr)": "sleep_light_hr",
    "Sleep Awake (hr)": "sleep_awake_hr",
    "Sleep Score": "sleep_score",
    "RHR": "rhr",
    "Min HR": "min_hr",
    "Max HR": "max_hr",
    "Avg Stress": "avg_stress",
    "Body Battery": "body_battery",
    "Body Battery High": "body_battery_high",
    "Body Battery Low": "body_battery_low",
    "Respiration": "respiration",
    "SpO2": "spo2",
    "VO2 Max": "vo2_max",
    "Training Status": "training_status",
    "HRV Status": "hrv_status",
    "HRV Avg": "hrv_avg",
    "Training Readiness": "training_readiness",
    "Fitness Age": "fitness_age",
    "Steps": "steps",
    "Step Goal": "step_goal",
    "Floors Climbed": "floors_climbed",
    "Cals Total": "cals_total",
    "Cals Active": "cals_active",
    "Intensity Moderate Mins": "intensity_moderate_mins",
    "Intensity Vigorous Mins": "intensity_vigorous_mins",
    "Race 5K Secs": "race_5k_secs",
    "Race 10K Secs": "race_10k_secs",
    "Race Half Secs": "race_half_secs",
    "Race Full Secs": "race_full_secs",
    "ATL": "atl",
    "CTL": "ctl",
    "TSB": "tsb",
    "Training Load Focus Aerobic %": "tl_aerobic_pct",
    "Lactate Threshold HR": "lactate_threshold_hr",
    "Lactate Threshold Pace": "lactate_threshold_pace",
    "Activities": "activities",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pace_str_to_float(val) -> Optional[float]:
    """Convert pace string '5:30' → 5.5 (minutes/mile as float)."""
    if val is None or str(val).strip() == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        pass
    try:
        parts = str(val).split(":")
        if len(parts) == 2:
            return float(parts[0]) + float(parts[1]) / 60.0
    except Exception:
        pass
    return None


def _coerce_int_cols(df: pd.DataFrame, cols: set) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def _coerce_float_cols(df: pd.DataFrame, cols: set) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _ensure_schema_cols(df: pd.DataFrame, schema: list) -> pd.DataFrame:
    """Add any missing schema columns as None and reorder to match schema."""
    for field in schema:
        if field.name not in df.columns:
            df[field.name] = None
    return df[[f.name for f in schema]]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _existing_date_timestamps(client: bigquery.Client, table_id: str, dates: list[str]) -> set[str]:
    """Return existing (date, timestamp) pairs as 'date|timestamp' strings for the given dates."""
    date_list = ", ".join(f"'{d}'" for d in dates)
    try:
        rows = client.query(
            f"SELECT date, timestamp FROM `{table_id}` WHERE date IN ({date_list})"
        ).result()
        return {f"{row['date']}|{row['timestamp']}" for row in rows}
    except Exception:
        return set()


def _existing_activity_ids(client: bigquery.Client, table_id: str, dates: list[str]) -> set[str]:
    """Return activity_ids already in the table for the given dates."""
    date_list = ", ".join(f"'{d}'" for d in dates)
    try:
        rows = client.query(
            f"SELECT DISTINCT activity_id FROM `{table_id}` WHERE date IN ({date_list})"
        ).result()
        return {row["activity_id"] for row in rows}
    except Exception:
        return set()


def write_stats_range(
    df: pd.DataFrame,
    project_id: str,
    dates: list[str],
    batch_id: str = "",
) -> int:
    """Write stats rows for the given dates, skipping any already present in BQ."""
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.garmin.garmin_stats"

    out = df.rename(columns=_CSV_TO_STATS_COLS).copy()
    if "date" not in out.columns:
        return 0

    # Only keep rows in the requested date window
    out = out[out["date"].astype(str).isin(dates)]
    if out.empty:
        LOGGER.info("No stats rows in date range %s–%s", dates[-1], dates[0])
        return 0

    # Skip rows already in BQ (dedup on date+timestamp to allow multiple rows per day)
    if "timestamp" in out.columns:
        existing = _existing_date_timestamps(client, table_id, dates)
        if existing:
            mask = (out["date"].astype(str) + "|" + out["timestamp"].astype(str)).isin(existing)
            out = out[~mask]
            LOGGER.info("Skipped %d already-present stats rows", mask.sum())
    if out.empty:
        LOGGER.info("All stats rows for range already in BQ")
        return 0

    out.insert(0, "run_date", out["date"].astype(str))
    out.insert(1, "batch_id", batch_id)

    out = _coerce_int_cols(out, {"sleep_score", "rhr", "min_hr", "max_hr", "avg_stress",
                                  "body_battery", "body_battery_high", "body_battery_low",
                                  "training_readiness", "fitness_age",
                                  "steps", "step_goal", "floors_climbed",
                                  "cals_total", "cals_active",
                                  "intensity_moderate_mins", "intensity_vigorous_mins",
                                  "race_5k_secs", "race_10k_secs", "race_half_secs", "race_full_secs",
                                  "lactate_threshold_hr"})
    out = _coerce_float_cols(out, {"weight_lbs", "muscle_mass_lbs", "body_fat_pct", "water_pct",
                                    "sleep_total_hr", "sleep_deep_hr", "sleep_rem_hr",
                                    "sleep_light_hr", "sleep_awake_hr",
                                    "respiration", "spo2", "vo2_max", "hrv_avg",
                                    "atl", "ctl", "tsb", "tl_aerobic_pct",
                                    "lactate_threshold_pace"})
    out = _ensure_schema_cols(out, _STATS_SCHEMA)

    job_config = bigquery.LoadJobConfig(
        schema=_STATS_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="run_date",
        ),
    )
    job = client.load_table_from_dataframe(out, table_id, job_config=job_config)
    job.result()
    LOGGER.info("Wrote %d stats rows to %s", len(out), table_id)
    return len(out)


def write_activities_range(
    df: pd.DataFrame,
    project_id: str,
    dates: list[str],
    batch_id: str = "",
) -> int:
    """Write activity rows for the given dates, skipping any activity_ids already in BQ."""
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.garmin.garmin_activities"

    out = df.copy()
    if "date" not in out.columns:
        return 0

    # Only keep rows in the requested date window
    out = out[out["date"].astype(str).isin(dates)]
    if out.empty:
        LOGGER.info("No activity rows in date range %s–%s", dates[-1], dates[0])
        return 0

    # Skip activity_ids already in BQ
    if "activity_id" in out.columns:
        out["activity_id"] = out["activity_id"].astype(str)
        existing = _existing_activity_ids(client, table_id, dates)
        if existing:
            LOGGER.info("Activity IDs already in BQ (skipping): %d", len(existing))
            out = out[~out["activity_id"].isin(existing)]
    if out.empty:
        LOGGER.info("All activities for range already in BQ")
        return 0

    out.insert(0, "run_date", out["date"].astype(str))
    out.insert(1, "batch_id", batch_id)

    if "avg_pace_min_mile" in out.columns:
        out["avg_pace_min_mile"] = out["avg_pace_min_mile"].apply(_pace_str_to_float)

    out = _coerce_int_cols(out, {"calories", "avg_hr", "max_hr",
                                  "running_cadence_spm", "cycling_cadence_rpm"})
    out = _coerce_float_cols(out, {"distance_m", "duration_s", "avg_speed_mps", "max_speed_mps",
                                    "avg_pace_min_mile", "avg_power_w", "max_power_w",
                                    "elevation_gain_m", "aerobic_te", "anaerobic_te",
                                    "best_20m_watts", "ftp_watts", "normalized_power_w",
                                    "intensity_factor", "tss",
                                    "recovery_time_s", "vo2max_activity", "performance_condition",
                                    "hr_zone_1_secs", "hr_zone_2_secs", "hr_zone_3_secs",
                                    "hr_zone_4_secs", "hr_zone_5_secs",
                                    "ground_contact_time_ms", "vertical_oscillation_mm",
                                    "stride_length_m", "vertical_ratio_pct",
                                    "avg_temp_c", "humidity_pct",
                                    "aerobic_decoupling_pct",
                                    "power_zone_1_secs", "power_zone_2_secs", "power_zone_3_secs",
                                    "power_zone_4_secs", "power_zone_5_secs"})
    out = _ensure_schema_cols(out, _ACTIVITIES_SCHEMA)

    job_config = bigquery.LoadJobConfig(
        schema=_ACTIVITIES_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="run_date",
        ),
    )
    job = client.load_table_from_dataframe(out, table_id, job_config=job_config)
    job.result()
    LOGGER.info("Wrote %d activity rows to %s", len(out), table_id)
    return len(out)


def write_stats(
    df: pd.DataFrame,
    project_id: str,
    run_date: date,
    batch_id: str = "",
) -> int:
    """Rename CSV columns, inject run_date/batch_id, and APPEND today's rows to garmin.garmin_stats."""
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.garmin.garmin_stats"

    out = df.rename(columns=_CSV_TO_STATS_COLS).copy()

    # Filter to only today's rows to avoid duplicates on re-runs
    today_str = run_date.isoformat()
    if "date" in out.columns:
        out = out[out["date"].astype(str) == today_str]

    if out.empty:
        LOGGER.info("No stats rows for %s to write to BQ", today_str)
        return 0

    out.insert(0, "run_date", today_str)
    out.insert(1, "batch_id", batch_id)

    out = _coerce_int_cols(out, {"sleep_score", "rhr", "min_hr", "max_hr", "avg_stress",
                                  "body_battery", "body_battery_high", "body_battery_low",
                                  "training_readiness", "fitness_age",
                                  "steps", "step_goal", "floors_climbed",
                                  "cals_total", "cals_active",
                                  "intensity_moderate_mins", "intensity_vigorous_mins",
                                  "race_5k_secs", "race_10k_secs", "race_half_secs", "race_full_secs",
                                  "lactate_threshold_hr"})
    out = _coerce_float_cols(out, {"weight_lbs", "muscle_mass_lbs", "body_fat_pct", "water_pct",
                                    "sleep_total_hr", "sleep_deep_hr", "sleep_rem_hr",
                                    "sleep_light_hr", "sleep_awake_hr",
                                    "respiration", "spo2", "vo2_max", "hrv_avg",
                                    "atl", "ctl", "tsb", "tl_aerobic_pct",
                                    "lactate_threshold_pace"})
    out = _ensure_schema_cols(out, _STATS_SCHEMA)

    job_config = bigquery.LoadJobConfig(
        schema=_STATS_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="run_date",
        ),
    )
    job = client.load_table_from_dataframe(out, table_id, job_config=job_config)
    job.result()
    LOGGER.info("Wrote %d stats rows to %s", len(out), table_id)
    return len(out)


def write_activities(
    df: pd.DataFrame,
    project_id: str,
    run_date: date,
    batch_id: str = "",
) -> int:
    """Inject run_date/batch_id and APPEND today's rows to garmin.garmin_activities."""
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.garmin.garmin_activities"

    out = df.copy()

    # Filter to only today's rows to avoid duplicates on re-runs
    today_str = run_date.isoformat()
    if "date" in out.columns:
        out = out[out["date"].astype(str) == today_str]

    if out.empty:
        LOGGER.info("No activity rows for %s to write to BQ", today_str)
        return 0

    out.insert(0, "run_date", today_str)
    out.insert(1, "batch_id", batch_id)

    # Convert pace string "5:30" → float 5.5
    if "avg_pace_min_mile" in out.columns:
        out["avg_pace_min_mile"] = out["avg_pace_min_mile"].apply(_pace_str_to_float)

    out = _coerce_int_cols(out, {"calories", "avg_hr", "max_hr",
                                  "running_cadence_spm", "cycling_cadence_rpm"})
    out = _coerce_float_cols(out, {"distance_m", "duration_s", "avg_speed_mps", "max_speed_mps",
                                    "avg_pace_min_mile", "avg_power_w", "max_power_w",
                                    "elevation_gain_m", "aerobic_te", "anaerobic_te",
                                    "best_20m_watts", "ftp_watts", "normalized_power_w",
                                    "intensity_factor", "tss",
                                    "recovery_time_s", "vo2max_activity", "performance_condition",
                                    "hr_zone_1_secs", "hr_zone_2_secs", "hr_zone_3_secs",
                                    "hr_zone_4_secs", "hr_zone_5_secs",
                                    "ground_contact_time_ms", "vertical_oscillation_mm",
                                    "stride_length_m", "vertical_ratio_pct",
                                    "avg_temp_c", "humidity_pct",
                                    "aerobic_decoupling_pct",
                                    "power_zone_1_secs", "power_zone_2_secs", "power_zone_3_secs",
                                    "power_zone_4_secs", "power_zone_5_secs"})
    out = _ensure_schema_cols(out, _ACTIVITIES_SCHEMA)

    job_config = bigquery.LoadJobConfig(
        schema=_ACTIVITIES_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="run_date",
        ),
    )
    job = client.load_table_from_dataframe(out, table_id, job_config=job_config)
    job.result()
    LOGGER.info("Wrote %d activity rows to %s", len(out), table_id)
    return len(out)
