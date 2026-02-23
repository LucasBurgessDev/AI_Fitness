from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timezone
from typing import Optional

from google.cloud import bigquery

LOGGER = logging.getLogger(__name__)

_TABLE = "data_control.batch_control"

_SCHEMA = [
    bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("job_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("run_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("start_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("end_time", "TIMESTAMP"),
    bigquery.SchemaField("rows_inserted", "INT64"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("error_message", "STRING"),
]


def start_batch(project_id: str, job_name: str) -> str:
    """Insert a RUNNING row into data_control.batch_control and return the new batch_id."""
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{_TABLE}"

    batch_id = str(uuid.uuid4())
    now = datetime.now(tz=timezone.utc)

    rows = [
        {
            "batch_id": batch_id,
            "job_name": job_name,
            "run_date": date.today().isoformat(),
            "start_time": now.isoformat(),
            "end_time": None,
            "rows_inserted": None,
            "status": "RUNNING",
            "error_message": None,
        }
    ]

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        LOGGER.warning("start_batch insert errors: %s", errors)
    else:
        LOGGER.info("Batch started: %s  job=%s", batch_id, job_name)

    return batch_id


def end_batch(
    project_id: str,
    batch_id: str,
    rows_inserted: int,
    status: str,
    error: Optional[str] = None,
) -> None:
    """Update the batch_control row with end_time, final status, and row count."""
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{_TABLE}"
    now = datetime.now(tz=timezone.utc)

    query = f"UPDATE `{table_id}` SET end_time = @end_time, rows_inserted = @rows_inserted, status = @status, error_message = @error_message WHERE batch_id = @batch_id"

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", now),
            bigquery.ScalarQueryParameter("rows_inserted", "INT64", rows_inserted),
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("error_message", "STRING", error),
            bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id),
        ]
    )
    job = client.query(query, job_config=job_config)
    job.result()
    LOGGER.info("Batch ended: %s  status=%s  rows=%s", batch_id, status, rows_inserted)
