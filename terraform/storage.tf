resource "google_storage_bucket" "pipeline_bucket" {
  name                        = var.bucket
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  depends_on = [google_project_service.apis["storage.googleapis.com"]]
}

resource "google_storage_bucket_iam_member" "sa_object_admin" {
  bucket = google_storage_bucket.pipeline_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.sa_email}"
}

resource "google_artifact_registry_repository" "health_jobs" {
  repository_id = "health-jobs"
  format        = "DOCKER"
  location      = var.region

  depends_on = [google_project_service.apis["artifactregistry.googleapis.com"]]
}
