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

# In-memory start times so end_batch can write a single complete row (avoids
# streaming-buffer UPDATE restriction in BigQuery).
_start_times: dict[str, datetime] = {}


def start_batch(project_id: str, job_name: str) -> str:
    """Record a new batch start and return the batch_id.

    No row is written to BigQuery yet — a single complete row is written by
    end_batch, which avoids the streaming-buffer UPDATE restriction.
    """
    batch_id = str(uuid.uuid4())
    _start_times[batch_id] = datetime.now(tz=timezone.utc)
    LOGGER.info("Batch started: %s  job=%s", batch_id, job_name)
    return batch_id


def end_batch(
    project_id: str,
    batch_id: str,
    rows_inserted: int,
    status: str,
    error: Optional[str] = None,
) -> None:
    """Insert a single complete row into data_control.batch_control."""
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{_TABLE}"

    start_time = _start_times.pop(batch_id, datetime.now(tz=timezone.utc))
    end_time = datetime.now(tz=timezone.utc)

    rows = [
        {
            "batch_id": batch_id,
            "job_name": "garmin-fitness-daily",
            "run_date": date.today().isoformat(),
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "rows_inserted": rows_inserted,
            "status": status,
            "error_message": error,
        }
    ]

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        LOGGER.warning("end_batch insert errors: %s", errors)
    else:
        LOGGER.info("Batch ended: %s  status=%s  rows=%s", batch_id, status, rows_inserted)
