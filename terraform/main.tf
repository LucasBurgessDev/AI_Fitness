terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }

  backend "gcs" {
    bucket = "garmin-fitness-health-data-482722"
    # prefix is set at init time via -backend-config="prefix=terraform/state/${ENV}"
    # dev:  terraform/state/dev
    # prod: terraform/state/prod
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  env_suffix = var.env == "prod" ? "" : "-${var.env}"
}
