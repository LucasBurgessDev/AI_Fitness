"""
One-shot historic bulk load for morning and evening check-in data.

Reads all rows from both Google Sheets via the Sheets API and loads them
into BigQuery tables garmin.morning_checkin and garmin.evening_checkin.

Usage:
  MORNING_SHEET_ID=<id> EVENING_SHEET_ID=<id> BQ_PROJECT_ID=<project> python load_checkin_history.py

Idempotent: uses WRITE_TRUNCATE so re-running replaces existing data.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from uuid import uuid4

import pandas as pd
from google.cloud import bigquery
from google.auth import default as google_auth_default
from googleapiclient.discovery import build

MORNING_SHEET_ID = os.environ.get("MORNING_SHEET_ID", "")
EVENING_SHEET_ID = os.environ.get("EVENING_SHEET_ID", "")
PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "health-data-482722")

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


def _sheets_service():
    creds, _ = google_auth_default(
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    return build("sheets", "v4", credentials=creds)


def _read_sheet(service, sheet_id: str) -> list[list]:
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=sheet_id, range="Form Responses!A:M")
        .execute()
    )
    return result.get("values", [])


def _yn_to_bool(val) -> bool | None:
    if val is None:
        return None
    return str(val).strip().upper() == "YES"


def _parse_ts(val) -> datetime | None:
    if not val:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(str(val).strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


def load_morning(service, bq_client: bigquery.Client) -> int:
    if not MORNING_SHEET_ID:
        print("MORNING_SHEET_ID not set — skipping morning load")
        return 0
    rows_raw = _read_sheet(service, MORNING_SHEET_ID)
    if len(rows_raw) < 2:
        print("Morning sheet: no data rows")
        return 0

    # Headers: Submission Date | Last Update Date | How do you feel today? |
    #          Are you working out? | Are you stretching? | Are you going for drinks? |
    #          Tell me about how you feel | What is the priority today? | Fill in the Blank |
    #          IP | Submission ID
    records = []
    for r in rows_raw[1:]:
        def _col(i, default=None):
            return r[i].strip() if i < len(r) and r[i] else default

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


def load_evening(service, bq_client: bigquery.Client) -> int:
    if not EVENING_SHEET_ID:
        print("EVENING_SHEET_ID not set — skipping evening load")
        return 0
    rows_raw = _read_sheet(service, EVENING_SHEET_ID)
    if len(rows_raw) < 2:
        print("Evening sheet: no data rows")
        return 0

    # Headers: Submission Date | Last Update Date | Did you workout today? |
    #          How many alcoholic drinks? | Did you track your eating? |
    #          How do you feel this evening? | Did you work late? |
    #          Tell me a little more | I Am Thankful For | How much chocco? |
    #          IP | Submission ID
    records = []
    for r in rows_raw[1:]:
        def _col(i, default=None):
            return r[i].strip() if i < len(r) and r[i] else default

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
    if not MORNING_SHEET_ID and not EVENING_SHEET_ID:
        print("Set MORNING_SHEET_ID and/or EVENING_SHEET_ID environment variables")
        sys.exit(1)

    svc = _sheets_service()
    bq = bigquery.Client(project=PROJECT_ID)

    m = load_morning(svc, bq)
    e = load_evening(svc, bq)
    print(f"\nDone. Total: {m + e} rows loaded.")
