SHELL := /bin/bash

PROJECT_ID ?= health-data-482722
REGION     ?= europe-west2

AR_REPO    ?= health-jobs
IMAGE_NAME ?= garmin-fitness-daily
JOB_NAME   ?= garmin-fitness-daily

SA_EMAIL   ?= garmin-fitness-daily-sa@health-data-482722.iam.gserviceaccount.com

BUCKET      ?= garmin-fitness-$(PROJECT_ID)
TOKENS_OBJ  ?= garmin/token_cache.tar.gz

DRIVE_FOLDER_ID ?= 1hTPk71hZmqLk8RNPggjZOfIRY9Ax1op-

IMAGE_URI           := $(REGION)-docker.pkg.dev/$(PROJECT_ID)/$(AR_REPO)/$(IMAGE_NAME):latest
TOKEN_CACHE_GCS_URI := gs://$(BUCKET)/$(TOKENS_OBJ)

# BigQuery datasets
BQ_DATASET_GARMIN  ?= garmin
BQ_DATASET_CONTROL ?= data_control

# ADK cycling service
ADK_SERVICE_NAME ?= cycling-coach
ADK_IMAGE_URI    := $(REGION)-docker.pkg.dev/$(PROJECT_ID)/$(AR_REPO)/$(ADK_SERVICE_NAME):latest

.PHONY: show enable-apis infra bootstrap-tokens build deploy run logs \
        bq-create-datasets bq-iam backfill-bq \
        build-adk deploy-adk oauth-setup \
        scheduler-enable scheduler-iam scheduler-create scheduler-now scheduler-list

show:
	@echo "PROJECT_ID=$(PROJECT_ID)"
	@echo "REGION=$(REGION)"
	@echo "IMAGE_URI=$(IMAGE_URI)"
	@echo "JOB_NAME=$(JOB_NAME)"
	@echo "SA_EMAIL=$(SA_EMAIL)"
	@echo "TOKEN_CACHE_GCS_URI=$(TOKEN_CACHE_GCS_URI)"
	@echo "DRIVE_FOLDER_ID=$(DRIVE_FOLDER_ID)"
	@echo "ADK_IMAGE_URI=$(ADK_IMAGE_URI)"

enable-apis:
	gcloud config set project "$(PROJECT_ID)"
	gcloud config set run/region "$(REGION)"
	gcloud services enable \
	  run.googleapis.com \
	  artifactregistry.googleapis.com \
	  cloudbuild.googleapis.com \
	  storage.googleapis.com \
	  drive.googleapis.com \
	  bigquery.googleapis.com

infra:
	gcloud storage buckets create "gs://$(BUCKET)" \
	  --location="$(REGION)" \
	  --uniform-bucket-level-access || true
	gcloud storage buckets add-iam-policy-binding "gs://$(BUCKET)" \
	  --member="serviceAccount:$(SA_EMAIL)" \
	  --role="roles/storage.objectAdmin"
	gcloud artifacts repositories create "$(AR_REPO)" \
	  --repository-format=docker \
	  --location="$(REGION)" || true

bootstrap-tokens:
	TOKEN_CACHE_GCS_URI="$(TOKEN_CACHE_GCS_URI)" python pipeline/bootstrap_tokens_to_gcs.py

build:
	gcloud builds submit pipeline/ --tag "$(IMAGE_URI)"

deploy:
	gcloud run jobs deploy "$(JOB_NAME)" \
	  --image "$(IMAGE_URI)" \
	  --service-account "$(SA_EMAIL)" \
	  --set-env-vars "TOKEN_CACHE_GCS_URI=$(TOKEN_CACHE_GCS_URI),DRIVE_FOLDER_ID=$(DRIVE_FOLDER_ID),SAVE_PATH=/tmp,LOOKBACK_DAYS=3,FTP_LOOKBACK_DAYS=60,BQ_PROJECT_ID=$(PROJECT_ID)" \
	  --max-retries 1 \
	  --task-timeout 900 \
	  --memory 1Gi \
	  --cpu 1 \
	  --region "$(REGION)"

run:
	gcloud run jobs execute "$(JOB_NAME)" --wait

backfill-bq:
	gcloud run jobs update "$(JOB_NAME)" --region "$(REGION)" --update-env-vars "BACKFILL=1" --quiet
	gcloud run jobs execute "$(JOB_NAME)" --region "$(REGION)" --wait
	gcloud run jobs update "$(JOB_NAME)" --region "$(REGION)" --remove-env-vars "BACKFILL" --quiet

# Backfill from local historic_data folder (no Drive/GCS needed — uses ADC for BQ auth)
backfill-bq-local:
	cd pipeline && LOCAL_DATA_PATH=../historic_data BQ_PROJECT_ID=$(PROJECT_ID) python backfill_bq.py

logs:
	gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=$(JOB_NAME)" --limit 120

# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------

bq-create-datasets:
	bq --location="$(REGION)" mk --dataset --description="Garmin activity and stats data" "$(PROJECT_ID):$(BQ_DATASET_GARMIN)" || true
	bq --location="$(REGION)" mk --dataset --description="Pipeline batch control" "$(PROJECT_ID):$(BQ_DATASET_CONTROL)" || true
	@echo "Datasets created (or already exist)."

bq-iam:
	gcloud projects add-iam-policy-binding "$(PROJECT_ID)" \
	  --member="serviceAccount:$(SA_EMAIL)" \
	  --role="roles/bigquery.dataEditor"
	gcloud projects add-iam-policy-binding "$(PROJECT_ID)" \
	  --member="serviceAccount:$(SA_EMAIL)" \
	  --role="roles/bigquery.jobUser"

# ---------------------------------------------------------------------------
# ADK Cycling Coach service
# ---------------------------------------------------------------------------

build-adk:
	gcloud builds submit adk_cycling/ --tag "$(ADK_IMAGE_URI)"

deploy-adk:
	gcloud run deploy "$(ADK_SERVICE_NAME)" \
	  --image "$(ADK_IMAGE_URI)" \
	  --region "$(REGION)" \
	  --service-account "$(SA_EMAIL)" \
	  --set-env-vars "PROJECT_ID=$(PROJECT_ID),DRIVE_FOLDER_ID=$(DRIVE_FOLDER_ID),GCS_PROFILE_BUCKET=$(BUCKET),REDIRECT_URI=https://cycling-coach-l3h3kcxbia-nw.a.run.app/auth/callback,GOOGLE_CLOUD_PROJECT=$(PROJECT_ID),GOOGLE_CLOUD_LOCATION=us-central1,GOOGLE_GENAI_USE_VERTEXAI=1" \
	  --set-secrets "GOOGLE_CLIENT_ID=cycling-coach-oauth-client-id:latest,GOOGLE_CLIENT_SECRET=cycling-coach-oauth-client-secret:latest,ALLOWED_EMAIL=cycling-coach-allowed-email:latest,SECRET_KEY=cycling-coach-secret-key:latest" \
	  --allow-unauthenticated \
	  --memory 1Gi \
	  --cpu 1 \
	  --timeout 3600 \
	  --port 8080

oauth-setup:
	@echo "1. Go to: https://console.cloud.google.com/apis/credentials?project=$(PROJECT_ID)"
	@echo "2. Create OAuth 2.0 Client ID (Web application)"
	@echo "3. Add Authorised redirect URI:"
	@echo "   https://<cycling-coach-hash>-$(REGION).a.run.app/auth/callback"
	@echo "4. Store credentials in Secret Manager:"
	@echo "   echo -n 'CLIENT_ID'     | gcloud secrets create cycling-coach-oauth-client-id --data-file=-"
	@echo "   echo -n 'CLIENT_SECRET' | gcloud secrets create cycling-coach-oauth-client-secret --data-file=-"
	@echo "   echo -n 'your@email'    | gcloud secrets create cycling-coach-allowed-email --data-file=-"
	@echo "   python -c \"import secrets; print(secrets.token_hex(32))\" | gcloud secrets create cycling-coach-secret-key --data-file=-"
	@echo "5. Grant SA access: gcloud secrets add-iam-policy-binding <secret> --member=serviceAccount:$(SA_EMAIL) --role=roles/secretmanager.secretAccessor"

# ---------------------------------------------------------------------------
# Scheduler  (every 30 min, 06:00–23:30 London time)
# ---------------------------------------------------------------------------
SCHEDULER_REGION ?= europe-west2
TZ               ?= Europe/London

RUN_URI := https://run.googleapis.com/v2/projects/$(PROJECT_ID)/locations/$(REGION)/jobs/$(JOB_NAME):run

scheduler-enable:
	gcloud config set project "$(PROJECT_ID)"
	gcloud services enable cloudscheduler.googleapis.com

scheduler-iam:
	gcloud run jobs add-iam-policy-binding "$(JOB_NAME)" \
	  --region "$(REGION)" \
	  --member "serviceAccount:$(SA_EMAIL)" \
	  --role "roles/run.invoker"

scheduler-create:
	gcloud scheduler jobs create http "$(JOB_NAME)-30min" \
	  --location "$(SCHEDULER_REGION)" \
	  --schedule "*/30 6-23 * * *" \
	  --time-zone "$(TZ)" \
	  --uri "$(RUN_URI)" \
	  --http-method POST \
	  --oauth-service-account-email "$(SA_EMAIL)" || true
	@echo "Scheduler created: every 30 min from 06:00 to 23:30 ($(TZ))"

scheduler-now:
	gcloud scheduler jobs run "$(JOB_NAME)-30min" --location "$(SCHEDULER_REGION)"

scheduler-list:
	gcloud scheduler jobs list --location "$(SCHEDULER_REGION)"
