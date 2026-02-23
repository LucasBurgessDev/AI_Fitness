# ---------------------------------------------------------------------------
# Auto-generated secret: session signing key
# The initial value is set once and never rotated (ignore_changes).
# ---------------------------------------------------------------------------

resource "random_password" "secret_key" {
  length  = 64
  special = false
}

resource "google_secret_manager_secret" "secret_key" {
  secret_id = "cycling-coach-secret-key"
  replication { auto {} }

  depends_on = [google_project_service.apis["secretmanager.googleapis.com"]]
}

resource "google_secret_manager_secret_version" "secret_key" {
  secret      = google_secret_manager_secret.secret_key.id
  secret_data = random_password.secret_key.result

  lifecycle {
    # Never rotate the key after the first apply — changing it would
    # invalidate all active user sessions.
    ignore_changes = [secret_data]
  }
}

# ---------------------------------------------------------------------------
# User-provided secrets: Terraform creates the resource and manages IAM.
# Secret versions (actual values) must be set manually via gcloud.
# ---------------------------------------------------------------------------

resource "google_secret_manager_secret" "oauth_client_id" {
  secret_id = "cycling-coach-oauth-client-id"
  replication { auto {} }

  depends_on = [google_project_service.apis["secretmanager.googleapis.com"]]
}

resource "google_secret_manager_secret" "oauth_client_secret" {
  secret_id = "cycling-coach-oauth-client-secret"
  replication { auto {} }

  depends_on = [google_project_service.apis["secretmanager.googleapis.com"]]
}

resource "google_secret_manager_secret" "allowed_email" {
  secret_id = "cycling-coach-allowed-email"
  replication { auto {} }

  depends_on = [google_project_service.apis["secretmanager.googleapis.com"]]
}

# ---------------------------------------------------------------------------
# Grant the pipeline SA accessor rights on all secrets
# ---------------------------------------------------------------------------

locals {
  managed_secrets = {
    secret_key          = google_secret_manager_secret.secret_key.secret_id
    oauth_client_id     = google_secret_manager_secret.oauth_client_id.secret_id
    oauth_client_secret = google_secret_manager_secret.oauth_client_secret.secret_id
    allowed_email       = google_secret_manager_secret.allowed_email.secret_id
  }
}

resource "google_secret_manager_secret_iam_member" "sa_accessor" {
  for_each  = local.managed_secrets
  secret_id = each.value
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${var.sa_email}"
}
