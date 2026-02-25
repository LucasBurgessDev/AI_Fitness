"""
BigQuery-backed coaching log.

Table: garmin.coaching_log
Columns: id, session_id, email, date, timestamp, category, content, context
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone

LOGGER = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID", "health-data-482722")
_TABLE = f"{PROJECT_ID}.garmin.coaching_log"


def save_insight(
    project_id: str,
    session_id: str,
    email: str,
    category: str,
    content: str,
    context: str = "",
) -> str:
    """Insert a coaching insight into the BigQuery coaching_log table.

    Args:
        project_id: GCP project ID.
        session_id: Current conversation session ID.
        email: User email.
        category: One of "PR", "recommendation", "observation", "goal_progress".
        content: The insight text.
        context: Optional supporting context or data snippet.

    Returns:
        "Insight saved." or an error string.
    """
    try:
        from google.cloud import bigquery
        now = datetime.now(timezone.utc)
        row = {
            "id": str(uuid.uuid4()),
            "session_id": session_id,
            "email": email,
            "date": now.date().isoformat(),
            "timestamp": now.isoformat(),
            "category": category,
            "content": content,
            "context": context or None,
        }
        client = bigquery.Client(project=project_id)
        errors = client.insert_rows_json(
            f"{project_id}.garmin.coaching_log", [row]
        )
        if errors:
            LOGGER.error("coaching_log insert errors: %s", errors)
            return f"Error saving insight: {errors}"
        return "Insight saved."
    except Exception as exc:
        LOGGER.error("coaching_log.save_insight error: %s", exc)
        return f"Error saving insight: {exc}"


def get_insights(
    project_id: str,
    email: str,
    weeks: int = 52,
    category: str = "",
) -> str:
    """Query recent coaching log entries from BigQuery.

    Args:
        project_id: GCP project ID.
        email: User email to filter by.
        weeks: How many weeks back to look (default 52).
        category: Optional category filter (e.g. "PR", "recommendation").

    Returns:
        Formatted string of coaching log entries, or a "no entries" message.
    """
    try:
        from google.cloud import bigquery
        cat_filter = f"AND category = '{category}'" if category else ""
        sql = f"""
            SELECT date, category, content, context
            FROM `{project_id}.garmin.coaching_log`
            WHERE email = '{email}'
              AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL {weeks * 7} DAY)
              {cat_filter}
            ORDER BY timestamp DESC
            LIMIT 200
        """
        client = bigquery.Client(project=project_id)
        results = client.query(sql).result()
        rows = [dict(row) for row in results]
        if not rows:
            return "No coaching log entries found."
        lines = [f"[Coaching log — last {weeks} weeks]"]
        for row in rows:
            ctx = f" | Context: {row['context']}" if row.get("context") else ""
            lines.append(f"[{row['date']}] [{row['category']}] {row['content']}{ctx}")
        return "\n".join(lines)
    except Exception as exc:
        LOGGER.error("coaching_log.get_insights error: %s", exc)
        return f"Error querying coaching log: {exc}"
