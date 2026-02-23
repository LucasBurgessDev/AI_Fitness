# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Python pipeline that pulls biometric and activity data from Garmin Connect and syncs it to Google Drive as CSVs, feeding AI tools (Gemini/ChatGPT) for personalized fitness coaching. Runs locally (Windows Task Scheduler) or on Google Cloud Run.

## Commands

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# One-time Garmin authentication (creates .garth/ token directory)
python direct_login.py

# Run daily data pulls (reads LOOKBACK_DAYS env var, default 3)
python garmin_stats_daily.py
python garmin_activities_daily.py

# Historical backfill
python garmin_stats_history.py
python garmin_activities_history.py
python garmin_runs_history.py

# Bootstrap tokens to GCS for Cloud Run
python bootstrap_tokens_to_gcs.py
```

### Cloud Deployment (via Makefile)

```bash
make build              # Build Docker image and push to Artifact Registry
make deploy             # Deploy to Cloud Run Job
make run                # Execute Cloud Run Job on demand
make logs               # View job execution logs
make scheduler-create   # Create Cloud Scheduler cron jobs (midnight + noon UTC)
make scheduler-now      # Trigger scheduler jobs immediately
```

No test suite or linter is configured for this project.

## Architecture

### Data Flow

```
Garmin Connect API
    → Authentication (garth OAuth2 tokens, cached in .garth/ or GCS)
    → Data collection modules (daily or history)
    → CSV files (appended, deduped by signature)
    → Google Drive sync (drive_uploader.py)
    → AI tools consume the CSVs
```

### Entry Points

- **Local:** Individual scripts (`garmin_stats_daily.py`, `garmin_activities_daily.py`, etc.)
- **Cloud Run:** `cloud_run_entrypoint.py` — orchestrates token download from GCS, runs data collection, then uploads to Drive

### Token Management

Garmin uses OAuth2 tokens stored in `.garth/`. For Cloud Run, tokens are serialized as a `.tar.gz` and persisted in GCS (`TOKEN_CACHE_GCS_URI`). `token_cache_gcs.py` handles download/upload; `garth` handles automatic refresh.

### CSV Append Strategy

Daily modules download the existing CSV from Google Drive, append new rows, deduplicate by a composite signature, then re-upload. This avoids database dependencies.

### Activity Filtering

`activity_filters.yaml` defines which Garmin activity types to include/map. `activity_filter.py` normalizes raw Garmin activity type strings against this config.

### FTP Resolution

`ftp_resolver.py` extracts Functional Threshold Power from historical activity data when not directly available from Garmin's stats endpoint (looks back `FTP_LOOKBACK_DAYS`, default 60).

## Configuration

All config via `.env` or environment variables:

| Variable | Description |
|---|---|
| `GARMIN_EMAIL` / `GARMIN_PASSWORD` | Garmin credentials |
| `SAVE_PATH` | Local CSV output directory |
| `GARTH_DIR` | Token cache dir (default: `.garth`) |
| `LOOKBACK_DAYS` | Days to pull on each run (default: 3) |
| `FTP_LOOKBACK_DAYS` | FTP metric lookback (default: 60) |
| `TOKEN_CACHE_GCS_URI` | GCS path for token persistence (Cloud Run) |
| `DRIVE_FOLDER_ID` | Google Drive folder ID for CSV sync |
| `LOG_LEVEL` | Logging level (default: INFO) |

## CI/CD

GitHub Actions (`.github/workflows/deploy.yml`) triggers on push to `main`:
1. Authenticates to GCP using `GCP_SA_KEY` secret
2. Builds Docker image via Cloud Build and pushes to Artifact Registry (`europe-west2-docker.pkg.dev/health-data-482722/health-jobs/garmin-fitness-daily:latest`)
3. Deploys updated image to Cloud Run Job (`garmin-fitness-daily`, region `europe-west2`)

Cloud Run Job limits: 1Gi memory, 1 CPU, 900s timeout, max 1 retry.
