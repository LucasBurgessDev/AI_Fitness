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
    bigquery.SchemaField("sleep_score", "INT64"),
    bigquery.SchemaField("rhr", "INT64"),
    bigquery.SchemaField("min_hr", "INT64"),
    bigquery.SchemaField("max_hr", "INT64"),
    bigquery.SchemaField("avg_stress", "INT64"),
    bigquery.SchemaField("body_battery", "INT64"),
    bigquery.SchemaField("respiration", "FLOAT64"),
    bigquery.SchemaField("spo2", "FLOAT64"),
    bigquery.SchemaField("vo2_max", "FLOAT64"),
    bigquery.SchemaField("training_status", "STRING"),
    bigquery.SchemaField("hrv_status", "STRING"),
    bigquery.SchemaField("hrv_avg", "FLOAT64"),
    bigquery.SchemaField("steps", "INT64"),
    bigquery.SchemaField("step_goal", "INT64"),
    bigquery.SchemaField("cals_total", "INT64"),
    bigquery.SchemaField("cals_active", "INT64"),
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
    "Sleep Score": "sleep_score",
    "RHR": "rhr",
    "Min HR": "min_hr",
    "Max HR": "max_hr",
    "Avg Stress": "avg_stress",
    "Body Battery": "body_battery",
    "Respiration": "respiration",
    "SpO2": "spo2",
    "VO2 Max": "vo2_max",
    "Training Status": "training_status",
    "HRV Status": "hrv_status",
    "HRV Avg": "hrv_avg",
    "Steps": "steps",
    "Step Goal": "step_goal",
    "Cals Total": "cals_total",
    "Cals Active": "cals_active",
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
                                  "body_battery", "steps", "step_goal", "cals_total", "cals_active"})
    out = _coerce_float_cols(out, {"weight_lbs", "muscle_mass_lbs", "body_fat_pct", "water_pct",
                                    "sleep_total_hr", "sleep_deep_hr", "sleep_rem_hr",
                                    "respiration", "spo2", "vo2_max", "hrv_avg"})
    out = _ensure_schema_cols(out, _STATS_SCHEMA)

    job_config = bigquery.LoadJobConfig(
        schema=_STATS_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
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
                                    "intensity_factor", "tss"})
    out = _ensure_schema_cols(out, _ACTIVITIES_SCHEMA)

    job_config = bigquery.LoadJobConfig(
        schema=_ACTIVITIES_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="run_date",
        ),
    )
    job = client.load_table_from_dataframe(out, table_id, job_config=job_config)
    job.result()
    LOGGER.info("Wrote %d activity rows to %s", len(out), table_id)
    return len(out)
