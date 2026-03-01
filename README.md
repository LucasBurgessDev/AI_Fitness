# AI Cycling Coach

A personal, data-driven cycling coach powered by Google Gemini, your live Garmin data, and Google Cloud.

The project has two components:

1. **Garmin Data Pipeline** — a Cloud Run Job that pulls biometrics and activity data from Garmin Connect every 30 minutes, writes it to BigQuery, and syncs CSVs to Google Drive.
2. **Cycling Coach AI** — a web app (FastAPI + Google ADK) that gives you a conversational AI coach backed by your real Garmin data, with Google Calendar integration, push notifications, and a Gemini-powered chat interface.

---

## Features

- **Live Garmin data** — weight, sleep, HRV, VO2 max, body battery, stress, steps, RHR pulled daily
- **Activity metrics** — TSS, Intensity Factor, Normalised Power, FTP-derived zones for every ride
- **AI coaching** — Gemini 2.0 Flash with direct BigQuery access; asks real questions about your actual fitness
- **Google Calendar sync** — create, view, and manage training events from the chat
- **Push notifications** — morning recovery check-ins and training reminders
- **Session history** — named conversations persisted to GCS
- **Dark mode** — full light/dark theme
- **PWA** — installable on Android and iOS
- **Manual data sync** — trigger a Garmin data pull from the Settings page

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Cloud Run Job — garmin-fitness-daily  (every 30 min)        │
│  Pulls Garmin data → appends to BigQuery → syncs to Drive    │
└──────────────────────┬───────────────────────────────────────┘
                       │
          ┌────────────┴───────────────┐
          │  BigQuery (garmin.*)       │  Google Drive (CSVs)
          │  garmin_stats              │  garmin_stats.csv
          │  garmin_activities         │  garmin_activities.csv
          └────────────┬───────────────┘
                       │ queries
┌──────────────────────┴───────────────────────────────────────┐
│  Cloud Run Service — cycling-coach                            │
│  FastAPI + Google ADK + Gemini 2.0 Flash                      │
│  Google OAuth2 login · BigQuery tools · Calendar tools        │
│  GCS-backed sessions · Push notifications                     │
└──────────────────────────────────────────────────────────────┘
```

**GCP stack:** Cloud Run · BigQuery · Cloud Storage · Secret Manager · Cloud Scheduler · Artifact Registry · OpenTofu (infrastructure as code) · GitHub Actions (CI/CD)

### BigQuery Response Cache

All BigQuery query results are cached in-process inside the `cycling-coach` Cloud Run service. The cache lives in **`adk_cycling/bq_cache.py`** as a thread-safe Python dict — no external dependencies, no network hop.

| Property | Detail |
|---|---|
| **Location** | In-process memory of the Cloud Run instance (`adk_cycling/bq_cache.py`) |
| **Cache key** | MD5 of the SQL string |
| **TTL** | 4 hours (override via `BQ_CACHE_TTL` env var, in seconds) |
| **Scope** | Per Cloud Run instance — not shared if multiple instances are running |
| **Errors** | Never cached; only successful query results are stored |

**Lifecycle:**
1. On the first query for a given SQL string, BigQuery is hit (~1–3 s) and the result is stored in the cache.
2. Subsequent identical queries within the TTL return instantly from memory.
3. When the user triggers **Garmin Sync** from the Settings page, the cache is cleared immediately, then a background thread waits ~3 minutes (enough for the pipeline to finish) and pre-warms the cache by fetching `get_recent_stats(30)`, `get_recent_activities(30)`, `get_weekly_summary(8)`, and `get_training_load(8, ftp)`.
4. The 4-hour TTL acts as a safety net if a sync fails or the Cloud Run instance restarts.

---

## Repository Layout

```
.
├── pipeline/                   # Cloud Run Job — Garmin data pipeline
│   ├── garmin_stats_daily.py       # Daily biometrics pull
│   ├── garmin_activities_daily.py  # Daily activity pull (TSS, NP, IF, FTP)
│   ├── garmin_stats_history.py     # Historical backfill
│   ├── garmin_activities_history.py
│   ├── bigquery_writer.py          # BQ append (partitioned, dedup by date)
│   ├── batch_control.py            # Run tracking table
│   ├── drive_uploader.py           # Google Drive sync
│   ├── token_cache_gcs.py          # Garmin OAuth token persistence
│   ├── activity_filter.py          # Allowlist/denylist activity types
│   ├── activity_filters.yaml
│   ├── cloud_run_entrypoint.py     # Orchestrates the full pipeline run
│   ├── direct_login.py             # One-time local Garmin login
│   └── bootstrap_tokens_to_gcs.py  # Upload local tokens to GCS
├── adk_cycling/                # Cloud Run Service — Cycling Coach web app
│   ├── app.py                      # FastAPI routes + OAuth2
│   ├── agent.py                    # ADK LlmAgent with BigQuery + Calendar tools
│   ├── bq_cache.py                 # In-process TTL cache for BigQuery results (4 hr default)
│   ├── profile.py                  # GCS-backed coach profile store
│   ├── session_store.py            # GCS-backed conversation history
│   ├── coaching_log.py             # BQ-backed insight persistence
│   ├── calendar_store.py           # Google Calendar OAuth token store
│   ├── vapid_store.py / push_store.py  # Web push notification support
│   ├── system_prompt.txt           # Dynamic coaching prompt template
│   └── templates/
│       ├── chat.html               # Main chat UI (PWA, dark mode)
│       ├── settings.html           # Profile, reminders, notifications, sync
│       └── login.html
├── terraform/                  # OpenTofu infrastructure-as-code
│   ├── main.tf / variables.tf
│   ├── apis.tf                     # GCP API enablement
│   ├── storage.tf                  # GCS bucket + Artifact Registry
│   ├── bigquery.tf                 # Datasets, tables, Cloud Scheduler
│   └── secrets.tf                  # Secret Manager resources
├── .github/workflows/
│   └── deploy.yml              # CI/CD: provision → build → deploy (parallel)
└── makefile                    # Manual GCP ops targets
```

---

## Local Development

### Prerequisites

- Python 3.11+
- A Garmin Connect account
- A GCP project with billing enabled
- `gcloud` CLI authenticated (`gcloud auth application-default login`)

### Garmin Pipeline

```bash
# Install dependencies
pip install -r pipeline/requirements.txt

# One-time Garmin login (creates .garth/ token directory)
python pipeline/direct_login.py

# Pull the last 3 days of data (reads LOOKBACK_DAYS, default 3)
python pipeline/garmin_stats_daily.py
python pipeline/garmin_activities_daily.py

# Historical backfill
python pipeline/garmin_stats_history.py
python pipeline/garmin_activities_history.py
```

### Cycling Coach (local)

```bash
pip install -r adk_cycling/requirements.txt

# Set required env vars (see Configuration below)
export GOOGLE_CLIENT_ID=...
export GOOGLE_CLIENT_SECRET=...
export ALLOWED_EMAIL=you@gmail.com
export SECRET_KEY=$(python -c "import secrets; print(secrets.token_hex(32))")
export PROJECT_ID=your-gcp-project

uvicorn adk_cycling.app:app --reload --port 8080
```

Open `http://localhost:8080`.

---

## Cloud Deployment

Infrastructure is managed by OpenTofu. Application images are built and deployed by GitHub Actions on every push to `main`.

### One-time setup

```bash
# 1. Enable the bootstrap API manually (required before first tofu apply)
gcloud services enable cloudresourcemanager.googleapis.com

# 2. Create a GCP Service Account with these roles:
#    roles/bigquery.admin
#    roles/secretmanager.admin
#    roles/serviceusage.serviceUsageAdmin
#    roles/resourcemanager.projectIamAdmin
#    roles/storage.admin
#    roles/artifactregistry.admin
#    roles/run.admin

# 3. Store the SA JSON key as a GitHub secret: GCP_SA_KEY

# 4. Create OAuth2 Web credentials in GCP Console, then store in Secret Manager:
gcloud secrets create cycling-coach-oauth-client-id --data-file=-  <<< "CLIENT_ID"
gcloud secrets create cycling-coach-oauth-client-secret --data-file=- <<< "CLIENT_SECRET"
gcloud secrets create cycling-coach-allowed-email --data-file=- <<< "you@gmail.com"
# (secret-key is auto-generated by Terraform on first apply)

# 5. Bootstrap Garmin tokens to GCS after local login
python pipeline/bootstrap_tokens_to_gcs.py
```

After that, push to `main` — GitHub Actions handles everything else.

### Manual operations (Makefile)

```bash
make build              # Build & push pipeline image
make deploy             # Deploy Cloud Run Job
make run                # Trigger pipeline run immediately

make build-adk          # Build & push cycling coach image
make deploy-adk         # Deploy cycling coach Cloud Run Service

make scheduler-create   # Create Cloud Scheduler (every 30 min, 06:00–23:30 London)
make scheduler-now      # Trigger scheduler immediately
make logs               # View pipeline job logs
```

---

## Configuration

### Garmin Pipeline

| Variable | Description | Default |
|---|---|---|
| `GARMIN_EMAIL` | Garmin Connect email | required |
| `GARMIN_PASSWORD` | Garmin Connect password | required |
| `SAVE_PATH` | Local CSV output directory | required |
| `LOOKBACK_DAYS` | Days to pull per run | `3` |
| `FTP_LOOKBACK_DAYS` | FTP estimation lookback | `60` |
| `TOKEN_CACHE_GCS_URI` | GCS URI for token persistence | required in cloud |
| `DRIVE_FOLDER_ID` | Google Drive folder ID for CSV sync | optional |
| `BQ_PROJECT_ID` | GCP project for BigQuery writes | optional |

### Cycling Coach

Stored as Secret Manager secrets, mounted automatically by Cloud Run:

| Variable | Description |
|---|---|
| `GOOGLE_CLIENT_ID` | OAuth2 Web client ID |
| `GOOGLE_CLIENT_SECRET` | OAuth2 Web client secret |
| `ALLOWED_EMAIL` | Comma-separated list of allowed Gmail addresses |
| `SECRET_KEY` | Random string for signed session cookies |
| `PROJECT_ID` | GCP project (for BigQuery queries) |
| `DRIVE_FOLDER_ID` | Google Drive folder (for Drive MCP access) |
| `REDIRECT_URI` | OAuth callback URL |
| `GARMIN_JOB_NAME` | Cloud Run Job name to trigger from UI | `garmin-fitness-daily` |
| `GARMIN_JOB_REGION` | Region of the Cloud Run Job | `europe-west2` |

---

## CI/CD

Three GitHub Actions jobs run on push to `main`:

1. **provision-infra** — runs `tofu apply` to ensure all GCP resources exist
2. **deploy-pipeline** — builds the pipeline Docker image via Cloud Build, deploys the Cloud Run Job
3. **deploy-cycling-coach** — builds the coach image via Cloud Build, deploys the Cloud Run Service

Jobs 2 and 3 run in parallel after job 1 completes.

---

## Data Collected

### Daily Stats (`garmin.garmin_stats`)

Body composition · sleep (total/deep/REM hours + score) · resting HR · min/max HR · stress · body battery · respiration · SpO2 · VO2 max · training status · HRV status + weekly average · steps · calories

### Activities (`garmin.garmin_activities`)

Activity type · duration · distance · avg/max HR · avg/max power · normalised power · FTP · Intensity Factor · TSS · cadence · elevation · pace · calories

### Activity types tracked

Running, treadmill running, trail running, road cycling, gravel cycling, mountain biking, indoor cycling, virtual ride, spinning.
Strength training and weightlifting are excluded.
