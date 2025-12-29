SHELL := /bin/bash

PROJECT_ID ?= health-data-482722
REGION ?= europe-west2

AR_REPO ?= health-jobs
IMAGE_NAME ?= garmin-fitness-daily
JOB_NAME ?= garmin-fitness-daily

SA_EMAIL ?= garmin-fitness-daily-sa@health-data-482722.iam.gserviceaccount.com

BUCKET ?= garmin-fitness-$(PROJECT_ID)
TOKENS_OBJ ?= garmin/token_cache.tar.gz

DRIVE_FOLDER_ID ?= 1hTPk71hZmqLk8RNPggjZOfIRY9Ax1op-

IMAGE_URI := $(REGION)-docker.pkg.dev/$(PROJECT_ID)/$(AR_REPO)/$(IMAGE_NAME):latest
TOKEN_CACHE_GCS_URI := gs://$(BUCKET)/$(TOKENS_OBJ)

.PHONY: show enable-apis infra bootstrap-tokens build deploy run logs

show:
	@echo "PROJECT_ID=$(PROJECT_ID)"
	@echo "REGION=$(REGION)"
	@echo "IMAGE_URI=$(IMAGE_URI)"
	@echo "JOB_NAME=$(JOB_NAME)"
	@echo "SA_EMAIL=$(SA_EMAIL)"
	@echo "TOKEN_CACHE_GCS_URI=$(TOKEN_CACHE_GCS_URI)"
	@echo "DRIVE_FOLDER_ID=$(DRIVE_FOLDER_ID)"

enable-apis:
	gcloud config set project "$(PROJECT_ID)"
	gcloud config set run/region "$(REGION)"
	gcloud services enable \
	  run.googleapis.com \
	  artifactregistry.googleapis.com \
	  cloudbuild.googleapis.com \
	  storage.googleapis.com \
	  drive.googleapis.com

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
	TOKEN_CACHE_GCS_URI="$(TOKEN_CACHE_GCS_URI)" python bootstrap_tokens_to_gcs.py

build:
	gcloud builds submit --tag "$(IMAGE_URI)"

deploy:
	gcloud run jobs deploy "$(JOB_NAME)" \
	  --image "$(IMAGE_URI)" \
	  --service-account "$(SA_EMAIL)" \
	  --set-env-vars "TOKEN_CACHE_GCS_URI=$(TOKEN_CACHE_GCS_URI),DRIVE_FOLDER_ID=$(DRIVE_FOLDER_ID),SAVE_PATH=/tmp,LOOKBACK_DAYS=3,FTP_LOOKBACK_DAYS=60" \
	  --max-retries 1 \
	  --task-timeout 900 \
	  --memory 1Gi \
	  --cpu 1

run:
	gcloud run jobs execute "$(JOB_NAME)" --wait

logs:
	gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=$(JOB_NAME)" --limit 120

# Scheduler settings
SCHEDULER_REGION ?= europe-west2
TZ ?= Europe/London

RUN_URI := https://run.googleapis.com/v2/projects/$(PROJECT_ID)/locations/$(REGION)/jobs/$(JOB_NAME):run

.PHONY: scheduler-enable scheduler-iam scheduler-create scheduler-now scheduler-list

scheduler-enable:
	gcloud config set project "$(PROJECT_ID)"
	gcloud services enable cloudscheduler.googleapis.com

scheduler-iam:
	gcloud run jobs add-iam-policy-binding "$(JOB_NAME)" \
	  --region "$(REGION)" \
	  --member "serviceAccount:$(SA_EMAIL)" \
	  --role "roles/run.invoker"

scheduler-create:
	gcloud scheduler jobs create http "$(JOB_NAME)-midnight" \
	  --location "$(SCHEDULER_REGION)" \
	  --schedule "0 0 * * *" \
	  --time-zone "$(TZ)" \
	  --uri "$(RUN_URI)" \
	  --http-method POST \
	  --oauth-service-account-email "$(SA_EMAIL)" || true
	gcloud scheduler jobs create http "$(JOB_NAME)-noon" \
	  --location "$(SCHEDULER_REGION)" \
	  --schedule "0 12 * * *" \
	  --time-zone "$(TZ)" \
	  --uri "$(RUN_URI)" \
	  --http-method POST \
	  --oauth-service-account-email "$(SA_EMAIL)" || true

scheduler-now:
	gcloud scheduler jobs run "$(JOB_NAME)-midnight" --location "$(SCHEDULER_REGION)"
	gcloud scheduler jobs run "$(JOB_NAME)-noon" --location "$(SCHEDULER_REGION)"

scheduler-list:
	gcloud scheduler jobs list --location "$(SCHEDULER_REGION)"
