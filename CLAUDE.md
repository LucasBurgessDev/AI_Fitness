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

### System Overview

Two independent Cloud Run deployments share a single GCS bucket, BigQuery project, and Google Drive folder:

```
┌─────────────────────────────────────────────────────────────────┐
│  Cloud Run Job — garmin-fitness-daily  (runs every 30 min)      │
│  cloud_run_entrypoint.py                                        │
│    ├─ token_cache_gcs.py   ← download .garth/ from GCS         │
│    ├─ garmin_activities_daily.py  ← pull activities             │
│    ├─ garmin_stats_daily.py       ← pull biometrics             │
│    ├─ bigquery_writer.py   ← write today's rows to BQ           │
│    ├─ batch_control.py     ← record run in data_control table   │
│    ├─ drive_uploader.py    ← upload full CSVs to Drive          │
│    └─ token_cache_gcs.py   ← re-upload refreshed tokens        │
└──────────────────┬──────────────────────────────────────────────┘
                   │ reads/writes
        ┌──────────┴──────────────────────────────┐
        │  GCS: gs://garmin-fitness-*/             │
        │    garmin/token_cache.tar.gz             │
        │    cycling-coach/profile.json            │
        └──────────────────────────────────────────┘
                   │                    │
        ┌──────────┴──────┐   ┌────────┴────────────────────────┐
        │  BigQuery        │   │  Google Drive                   │
        │  garmin.*        │   │  garmin_activities/ folder      │
        │  data_control.*  │   │  garmin_stats.csv               │
        └──────────┬───────┘   │  garmin_activities.csv          │
                   │           └────────────────────┬────────────┘
                   │ queries                        │ reads
        ┌──────────┴────────────────────────────────┴────────────┐
        │  Cloud Run Service — cycling-coach                      │
        │  adk_cycling/app.py  (FastAPI + Google OAuth2)          │
        │    ├─ profile.py    ← load/save GCS-backed profile      │
        │    └─ agent.py      ← ADK LlmAgent + MCP toolsets       │
        │         ├─ @mcp/server-bigquery  (npx, queries BQ)      │
        │         └─ @mcp/server-gdrive   (npx, reads Drive)      │
        └────────────────────────────────────────────────────────┘
```

### Pipeline Execution Flow (`cloud_run_entrypoint.py`)

Each Cloud Run Job execution follows this sequence:

1. **Download tokens** — `token_cache_gcs.py` fetches `token_cache.tar.gz` from GCS, unpacks `.garth/` to `/tmp/.garth`
2. **Seed CSVs** — `drive_uploader.download_file_if_exists` pulls the current `garmin_stats.csv` and `garmin_activities.csv` from Drive to `/tmp/`, so daily scripts can append to the full history
3. **Collect data** — runs `garmin_activities_daily.py` then `garmin_stats_daily.py` as subprocesses; each appends new rows to the local CSV
4. **Write to BigQuery** — `batch_control.start_batch` records a `RUNNING` row; `bigquery_writer` loads both CSVs, filters to today's rows, and appends to BQ; `batch_control.end_batch` records `SUCCESS` or `FAILED` with row count
5. **Upload CSVs** — `drive_uploader.upload_all_csvs` replaces both files in Drive (update-in-place if they exist, create otherwise)
6. **Persist tokens** — `token_cache_gcs.upload_token_cache` re-uploads `.garth/` in case `garth` refreshed the OAuth2 token during the run

The BQ step is skipped entirely if `BQ_PROJECT_ID` is unset, preserving backward compatibility.

### Garmin Stats Collection (`garmin_stats_daily.py`)

Pulls one row per day covering:
- **Body composition** — weight, muscle mass, body fat %, water % (grams → lbs conversion via 453.592)
- **Sleep** — total, deep, REM hours (seconds → hours); sleep score
- **Heart rate** — RHR, min HR, max HR (falls back to raw HR timeseries if summary is missing)
- **Stress & recovery** — avg stress (falls back to `stressValuesArray`), body battery, respiration, SpO2
- **Fitness** — VO2 max, training status, HRV status & weekly average
- **Activity** — steps, step goal, total/active calories; filtered activity name string

Deduplication: on re-run, the existing CSV is read and existing rows are kept; the new row is appended and the full file is rewritten. Schema migrations (e.g. adding `Timestamp` column) are handled in-place.

### Activity Collection (`garmin_activities_daily.py`)

Pulls activities over the `LOOKBACK_DAYS` window (default 3):

1. **FTP resolution** — tries `get_cycling_ftp()` or the `biometric-service` API endpoint first; if unavailable, scans recent `virtual_ride`/`indoor_cycling` activities for best 20-minute power and applies a 95% multiplier. Result cached for the run.
2. **Activity fetch** — broad date-range query; falls back to per-type queries if the broad call fails
3. **Filter** — `activity_filter.py` checks each activity type against `activity_filters.yaml` (allows: running, cycling variants; blocks: strength, weightlifting)
4. **Deduplication** — skips any `activity_id` already present in the CSV
5. **Detail fetch** — for cycling activity types, fetches full activity details to extract normalised power and best 20-minute watts via recursive key scanning
6. **Metrics** — computes Intensity Factor (`NP / FTP`) and TSS (`(duration × NP × IF) / (FTP × 3600) × 100`) when FTP is available
7. **Append** — writes only new rows in date/time order; handles schema migration if column set has changed

### Activity Filtering (`activity_filter.py` + `activity_filters.yaml`)

`ActivityFilter` is a frozen dataclass with `include_types` and `exclude_types` sets. Types are normalised (lowercase, spaces → underscores) before comparison. Current config:

- **Include:** `running`, `treadmill_running`, `trail_running`, `cycling`, `road_cycling`, `gravel_cycling`, `mountain_biking`, `indoor_cycling`, `virtual_ride`, `spinning`
- **Exclude:** `strength_training`, `weightlifting`

Override the config path via `ACTIVITY_FILTER_PATH` env var.

### Token Management

Garmin uses OAuth2 tokens managed by the `garth` library, stored in a `.garth/` directory:

- **Local setup:** `direct_login.py` performs interactive login and writes `.garth/`
- **Bootstrap to cloud:** `bootstrap_tokens_to_gcs.py` tarballs the local `.garth/` and uploads to `TOKEN_CACHE_GCS_URI`
- **Cloud Run:** `token_cache_gcs.py` downloads and unpacks the tarball at job start, re-uploads after the run so any token refreshes are persisted
- `garth` handles OAuth2 refresh automatically on each API call

### Google Drive Integration (`drive_uploader.py`)

Uses the Google Drive API v3 with Application Default Credentials (ADC):

- **Auth:** `google.auth.default()` with full Drive scope — SA permissions on the folder constrain access
- **Upload:** `upload_or_replace_csv` — searches for the file by name in the target folder; calls `files().update()` if found, `files().create()` otherwise; supports Shared Drives via `supportsAllDrives=True`
- **Download:** `download_file_if_exists` — streams the file via `MediaIoBaseDownload` with progress logging

### BigQuery Integration

**`bigquery_writer.py`**

- `write_stats` renames the human-readable CSV headers (e.g. `"Weight (lbs)"`) to BQ column names (e.g. `weight_lbs`), filters to today's date string, coerces nullable integers to pandas `Int64`, and loads via `load_table_from_dataframe`
- `write_activities` handles the pace string format (`"5:30"` → `5.5` float minutes/mile) before loading
- Both functions use `WRITE_APPEND` with `DAY` partitioning on `run_date` — filtering to today prevents duplicates across re-runs

**`batch_control.py`**

- `start_batch` inserts a `RUNNING` row using `insert_rows_json` (streaming insert) and returns a UUID
- `end_batch` updates the row using a parameterised DML `UPDATE` query (not streaming insert, since BQ streaming inserts cannot be updated)

**BigQuery schema:**

| Dataset | Table | Rows | Partition |
|---|---|---|---|
| `garmin` | `garmin_stats` | 28 cols | `run_date` |
| `garmin` | `garmin_activities` | 29 cols | `run_date` |
| `data_control` | `batch_control` | 8 cols | none |

### ADK Cycling Coach (`adk_cycling/`)

**`app.py` — FastAPI application**

- Google OAuth2 login restricted to `ALLOWED_EMAIL`; tokens exchanged via `google-auth-oauthlib` Flow, user info verified against Google's userinfo endpoint
- Sessions stored as `itsdangerous.URLSafeSerializer` signed cookies (7-day expiry, `httponly`, `samesite=lax`)
- Routes: `GET /login`, `GET /auth/start` → OAuth redirect, `GET /auth/callback` → set cookie, `GET /logout`, `GET /` (chat), `POST /chat`, `GET|POST /settings`, `GET /health`

**`agent.py` — ADK agent**

- One `InMemorySessionService` is shared across all requests; conversation history persists for the session lifetime (i.e. until the Cloud Run instance restarts)
- Runners are cached per `session_id` in `_runners: dict[str, tuple[Runner, dict]]`. Each entry stores the runner alongside the profile snapshot used to build it. If `profile.load()` returns a different dict on the next request, the runner is rebuilt with the updated system prompt — conversation history is preserved because the `InMemorySessionService` is not cleared
- `invalidate_sessions()` clears all runners immediately; called by `app.py` after a settings save

**`profile.py` — editable profile store**

- Persists to `gs://{GCS_PROFILE_BUCKET}/cycling-coach/profile.json`
- 30-second in-memory cache to avoid a GCS read on every chat message
- `invalidate_cache()` resets the cache TTL so the next `load()` re-fetches from GCS
- Falls back to hardcoded `DEFAULTS` dict if GCS is unavailable (e.g. local dev without `GCS_PROFILE_BUCKET`)

**`system_prompt.txt` — dynamic coaching prompt**

Python `str.format()` template with placeholders filled at runner creation time:

| Placeholder | Source |
|---|---|
| `{ftp}`, `{weight_kg}`, `{height_cm}`, `{age}`, `{stats_date}` | `profile.json` |
| `{wpkg}` | computed: `round(ftp / weight_kg, 2)` |
| `{goals}` | `profile.json` — multiline text |
| `{equipment}` | `profile.json` — comma-separated list |

### GCS Bucket Layout

```
gs://garmin-fitness-health-data-482722/
  garmin/
    token_cache.tar.gz          ← packed .garth/ OAuth2 token dir
  cycling-coach/
    profile.json                ← editable coach profile (stats, goals, equipment)
  cloudbuild_source/            ← Cloud Build staging area
  cloudbuild_logs/              ← Cloud Build logs
```

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
