# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Python pipeline that pulls biometric and activity data from Garmin Connect, syncs it to Google Drive as CSVs **and writes it to BigQuery**, feeding AI tools for personalized fitness coaching. Includes a separate ADK-powered cycling expert web service with Google OAuth2 login and MCP access to BigQuery + Google Drive. Runs locally (Windows Task Scheduler) or on Google Cloud Run.

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
# Garmin pipeline (Cloud Run Job)
make enable-apis        # Enable required GCP APIs (includes bigquery.googleapis.com)
make build              # Build Docker image and push to Artifact Registry
make deploy             # Deploy to Cloud Run Job (includes BQ_PROJECT_ID env var)
make run                # Execute Cloud Run Job on demand
make logs               # View job execution logs

# BigQuery setup (one-time)
make bq-create-datasets # Create garmin + data_control datasets in europe-west2
make bq-iam             # Grant SA bigquery.dataEditor + bigquery.jobUser roles

# Scheduler (every 30 min, 06:00–23:30 London time)
make scheduler-create   # Create Cloud Scheduler job
make scheduler-now      # Trigger scheduler immediately
make scheduler-list     # List scheduler jobs

# ADK Cycling Coach service (Cloud Run Service)
make oauth-setup        # Print OAuth2 setup instructions
make build-adk          # Build cycling coach Docker image
make deploy-adk         # Deploy cycling-coach Cloud Run service
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
    → BigQuery write (bigquery_writer.py) — today's rows only, partitioned by run_date
    → batch_control table (data_control.batch_control) records each pipeline run
    → AI tools consume CSVs and/or query BigQuery directly
```

### Entry Points

- **Local:** Individual scripts (`garmin_stats_daily.py`, `garmin_activities_daily.py`, etc.)
- **Cloud Run Job:** `cloud_run_entrypoint.py` — orchestrates token download from GCS, runs data collection, writes to BQ, uploads to Drive
- **Cloud Run Service:** `adk_cycling/app.py` — FastAPI web app: Google OAuth2 login + ADK cycling coach chat

### Token Management

Garmin uses OAuth2 tokens stored in `.garth/`. For Cloud Run, tokens are serialized as a `.tar.gz` and persisted in GCS (`TOKEN_CACHE_GCS_URI`). `token_cache_gcs.py` handles download/upload; `garth` handles automatic refresh.

### CSV Append Strategy

Daily modules download the existing CSV from Google Drive, append new rows, deduplicate by a composite signature, then re-upload. This avoids database dependencies.

### BigQuery Write Strategy

`bigquery_writer.py` reads the full CSV but filters to today's rows before writing (avoids duplicates). Uses `WRITE_APPEND` with `DAY` partitioning on `run_date`. Tables:

- `garmin.garmin_stats` — 28 cols, partitioned on `run_date`
- `garmin.garmin_activities` — 29 cols, partitioned on `run_date`
- `data_control.batch_control` — pipeline observability (one row per run)

### Activity Filtering

`activity_filters.yaml` defines which Garmin activity types to include/map. `activity_filter.py` normalizes raw Garmin activity type strings against this config.

### FTP Resolution

`ftp_resolver.py` extracts Functional Threshold Power from historical activity data when not directly available from Garmin's stats endpoint (looks back `FTP_LOOKBACK_DAYS`, default 60).

### ADK Cycling Coach (`adk_cycling/`)

FastAPI app with Google OAuth2 login (single-user: `ALLOWED_EMAIL`). Uses ADK `LlmAgent` (Gemini 2.0 Flash) with two MCP toolsets:
- `@modelcontextprotocol/server-bigquery` — queries `garmin.*` tables
- `@modelcontextprotocol/server-gdrive` — reads Drive CSVs

The agent has a detailed cycling coaching system prompt (`system_prompt.txt`) and maintains conversation history via ADK `InMemorySessionService`.

## Configuration

### Garmin Pipeline

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
| `BQ_PROJECT_ID` | GCP project for BigQuery writes (skipped if unset) |
| `LOG_LEVEL` | Logging level (default: INFO) |

### ADK Cycling Coach

Stored as Secret Manager secrets, mounted by Cloud Run:

| Variable | Description |
|---|---|
| `GOOGLE_CLIENT_ID` | OAuth2 Web client ID |
| `GOOGLE_CLIENT_SECRET` | OAuth2 Web client secret |
| `ALLOWED_EMAIL` | Gmail address allowed to log in |
| `SECRET_KEY` | Random string for signed session cookies |
| `PROJECT_ID` | GCP project (for BQ MCP tool) |
| `DRIVE_FOLDER_ID` | Drive folder (for Drive MCP tool) |
| `REDIRECT_URI` | OAuth callback URL (default: localhost for dev) |

## CI/CD

GitHub Actions (`.github/workflows/deploy.yml`) triggers on push to `main` and runs two parallel jobs:

**deploy-pipeline** (Garmin Cloud Run Job):
1. Authenticates to GCP using `GCP_SA_KEY` secret
2. Builds Docker image via Cloud Build → Artifact Registry
3. Deploys `garmin-fitness-daily` Cloud Run Job with `BQ_PROJECT_ID` env var

**deploy-cycling-coach** (ADK Cloud Run Service):
1. Authenticates to GCP using `GCP_SA_KEY` secret
2. Builds `adk_cycling/` Docker image via Cloud Build
3. Deploys `cycling-coach` Cloud Run Service with secrets mounted

Cloud Run Job limits: 1Gi memory, 1 CPU, 900s timeout, max 1 retry.
Cloud Run Service limits: 1Gi memory, 1 CPU.
