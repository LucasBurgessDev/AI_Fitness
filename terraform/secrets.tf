# ---------------------------------------------------------------------------
# Auto-generated secret: session signing key (one per env — dev and prod
# have independent signing keys so sessions don't cross-contaminate)
# The initial value is set once and never rotated (ignore_changes).
# ---------------------------------------------------------------------------

resource "random_password" "secret_key" {
  length  = 64
  special = false
}

resource "google_secret_manager_secret" "secret_key" {
  secret_id = "cycling-coach${local.env_suffix}-secret-key"

  replication {
    auto {}
  }

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

resource "google_secret_manager_secret_iam_member" "sa_accessor_secret_key" {
  secret_id = google_secret_manager_secret.secret_key.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${var.sa_email}"
}

# ---------------------------------------------------------------------------
# User-provided secrets: prod-only — dev reuses these directly.
# Secret versions (actual values) must be set manually via gcloud.
# ---------------------------------------------------------------------------

resource "google_secret_manager_secret" "oauth_client_id" {
  count     = var.env == "prod" ? 1 : 0
  secret_id = "cycling-coach-oauth-client-id"

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis["secretmanager.googleapis.com"]]
}

resource "google_secret_manager_secret" "oauth_client_secret" {
  count     = var.env == "prod" ? 1 : 0
  secret_id = "cycling-coach-oauth-client-secret"

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis["secretmanager.googleapis.com"]]
}

resource "google_secret_manager_secret" "allowed_email" {
  count     = var.env == "prod" ? 1 : 0
  secret_id = "cycling-coach-allowed-email"

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis["secretmanager.googleapis.com"]]
}

# ---------------------------------------------------------------------------
# Grant the pipeline SA accessor rights on the prod-only secrets.
# (The SA already has access in prod state; dev inherits it — same SA.)
# ---------------------------------------------------------------------------

locals {
  prod_secrets = var.env == "prod" ? {
    oauth_client_id     = google_secret_manager_secret.oauth_client_id[0].secret_id
    oauth_client_secret = google_secret_manager_secret.oauth_client_secret[0].secret_id
    allowed_email       = google_secret_manager_secret.allowed_email[0].secret_id
  } : {}
}

resource "google_secret_manager_secret_iam_member" "sa_accessor" {
  for_each  = local.prod_secrets
  secret_id = each.value
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${var.sa_email}"
}
