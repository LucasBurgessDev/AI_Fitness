"""
One-shot historic bulk load for morning and evening check-in data.

Reads from local Excel files (preferred) or Google Sheets API and loads into
BigQuery tables garmin.morning_checkin and garmin.evening_checkin.

Usage (local Excel files — no Sheets API auth needed):
  MORNING_XLSX=/path/Morning Qs.xlsx EVENING_XLSX=/path/Evening Check-in .xlsx \
    BQ_PROJECT_ID=health-data-482722 python3 load_checkin_history.py

Usage (Google Sheets API — requires ADC with spreadsheets scope):
  MORNING_SHEET_ID=<id> EVENING_SHEET_ID=<id> BQ_PROJECT_ID=<project> \
    python3 load_checkin_history.py

Idempotent: uses WRITE_TRUNCATE so re-running replaces existing data.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from uuid import uuid4

import pandas as pd
from google.cloud import bigquery

MORNING_SHEET_ID = os.environ.get("MORNING_SHEET_ID", "")
EVENING_SHEET_ID = os.environ.get("EVENING_SHEET_ID", "")
MORNING_XLSX     = os.environ.get("MORNING_XLSX", "")
EVENING_XLSX     = os.environ.get("EVENING_XLSX", "")
PROJECT_ID       = os.environ.get("BQ_PROJECT_ID", "health-data-482722")

_MOOD_SCORE = {"Terrible": 1, "Bad": 2, "Fine": 3, "Good": 4, "Great!": 5}

_MORNING_SCHEMA = [
    bigquery.SchemaField("date",           "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("submitted_at",   "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("feeling",        "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("mood_score",     "INT64",     mode="NULLABLE"),
    bigquery.SchemaField("working_out",    "BOOL",      mode="NULLABLE"),
    bigquery.SchemaField("stretching",     "BOOL",      mode="NULLABLE"),
    bigquery.SchemaField("drinks_tonight", "BOOL",      mode="NULLABLE"),
    bigquery.SchemaField("notes",          "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("priority",       "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("fill_in_blank",  "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("source",         "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("submission_id",  "STRING",    mode="REQUIRED"),
]

_EVENING_SCHEMA = [
    bigquery.SchemaField("date",           "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("submitted_at",   "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("did_workout",    "BOOL",      mode="NULLABLE"),
    bigquery.SchemaField("alcohol_drinks", "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("tracked_eating", "BOOL",      mode="NULLABLE"),
    bigquery.SchemaField("feeling",        "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("mood_score",     "INT64",     mode="NULLABLE"),
    bigquery.SchemaField("worked_late",    "BOOL",      mode="NULLABLE"),
    bigquery.SchemaField("notes",          "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("gratitude",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("chocolate",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("source",         "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("submission_id",  "STRING",    mode="REQUIRED"),
]


def _read_xlsx(path: str) -> list[list]:
    df = pd.read_excel(path, sheet_name="Form Responses", header=0, dtype=str)
    df = df.fillna("")
    rows = [list(df.columns)] + df.values.tolist()
    return rows


def _read_sheet_api(sheet_id: str) -> list[list]:
    from google.auth import default as google_auth_default
    from googleapiclient.discovery import build
    creds, _ = google_auth_default(
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    svc = build("sheets", "v4", credentials=creds)
    result = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=sheet_id, range="Form Responses!A:M")
        .execute()
    )
    return result.get("values", [])


def _yn_to_bool(val) -> bool | None:
    if val is None or str(val).strip() == "":
        return None
    return str(val).strip().upper() == "YES"


def _parse_ts(val) -> datetime | None:
    if not val or str(val).strip() == "":
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(val).strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


def load_morning(bq_client: bigquery.Client) -> int:
    if MORNING_XLSX:
        print(f"Morning: reading from {MORNING_XLSX}")
        rows_raw = _read_xlsx(MORNING_XLSX)
    elif MORNING_SHEET_ID:
        print(f"Morning: reading from Sheets API ({MORNING_SHEET_ID})")
        rows_raw = _read_sheet_api(MORNING_SHEET_ID)
    else:
        print("Morning: no source set — skipping")
        return 0

    if len(rows_raw) < 2:
        print("Morning sheet: no data rows")
        return 0

    records = []
    for r in rows_raw[1:]:
        def _col(i, default=None):
            v = r[i] if i < len(r) else None
            return str(v).strip() if v is not None and str(v).strip() != "" else default

        ts = _parse_ts(_col(0))
        if ts is None:
            continue
        date_str = ts.strftime("%Y-%m-%d")
        feeling = _col(2, "")
        sub_id = _col(10) or str(uuid4())

        records.append({
            "date":           date_str,
            "submitted_at":   ts.isoformat(),
            "feeling":        feeling,
            "mood_score":     _MOOD_SCORE.get(feeling),
            "working_out":    _yn_to_bool(_col(3)),
            "stretching":     _yn_to_bool(_col(4)),
            "drinks_tonight": _yn_to_bool(_col(5)),
            "notes":          _col(6, ""),
            "priority":       _col(7, ""),
            "fill_in_blank":  _col(8, ""),
            "source":         "sheet",
            "submission_id":  sub_id,
        })

    df = pd.DataFrame(records)
    table_id = f"{PROJECT_ID}.garmin.morning_checkin"
    job_config = bigquery.LoadJobConfig(
        schema=_MORNING_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Morning: loaded {len(records)} rows → {table_id}")
    return len(records)


def load_evening(bq_client: bigquery.Client) -> int:
    if EVENING_XLSX:
        print(f"Evening: reading from {EVENING_XLSX}")
        rows_raw = _read_xlsx(EVENING_XLSX)
    elif EVENING_SHEET_ID:
        print(f"Evening: reading from Sheets API ({EVENING_SHEET_ID})")
        rows_raw = _read_sheet_api(EVENING_SHEET_ID)
    else:
        print("Evening: no source set — skipping")
        return 0

    if len(rows_raw) < 2:
        print("Evening sheet: no data rows")
        return 0

    records = []
    for r in rows_raw[1:]:
        def _col(i, default=None):
            v = r[i] if i < len(r) else None
            return str(v).strip() if v is not None and str(v).strip() != "" else default

        ts = _parse_ts(_col(0))
        if ts is None:
            continue
        date_str = ts.strftime("%Y-%m-%d")
        feeling = _col(5, "")

        try:
            alcohol = float(_col(3) or 0)
        except (ValueError, TypeError):
            alcohol = 0.0

        sub_id = _col(11) or str(uuid4())

        records.append({
            "date":           date_str,
            "submitted_at":   ts.isoformat(),
            "did_workout":    _yn_to_bool(_col(2)),
            "alcohol_drinks": alcohol,
            "tracked_eating": _yn_to_bool(_col(4)),
            "feeling":        feeling,
            "mood_score":     _MOOD_SCORE.get(feeling),
            "worked_late":    _yn_to_bool(_col(6)),
            "notes":          _col(7, ""),
            "gratitude":      _col(8, ""),
            "chocolate":      _col(9, "None"),
            "source":         "sheet",
            "submission_id":  sub_id,
        })

    df = pd.DataFrame(records)
    table_id = f"{PROJECT_ID}.garmin.evening_checkin"
    job_config = bigquery.LoadJobConfig(
        schema=_EVENING_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Evening: loaded {len(records)} rows → {table_id}")
    return len(records)


if __name__ == "__main__":
    bq = bigquery.Client(project=PROJECT_ID)
    m = load_morning(bq)
    e = load_evening(bq)
    print(f"\nDone. Total: {m + e} rows loaded.")
