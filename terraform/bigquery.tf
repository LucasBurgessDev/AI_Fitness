resource "google_bigquery_dataset" "garmin" {
  dataset_id  = "garmin"
  description = "Garmin activity and stats data"
  location    = var.region

  depends_on = [google_project_service.apis["bigquery.googleapis.com"]]
}

resource "google_bigquery_dataset" "data_control" {
  dataset_id  = "data_control"
  description = "Pipeline batch control"
  location    = var.region

  depends_on = [google_project_service.apis["bigquery.googleapis.com"]]
}

resource "google_bigquery_table" "batch_control" {
  dataset_id          = google_bigquery_dataset.data_control.dataset_id
  table_id            = "batch_control"
  deletion_protection = false

  schema = jsonencode([
    { name = "batch_id",      type = "STRING",    mode = "REQUIRED" },
    { name = "job_name",      type = "STRING",    mode = "REQUIRED" },
    { name = "run_date",      type = "DATE",      mode = "REQUIRED" },
    { name = "start_time",    type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "end_time",      type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "rows_inserted", type = "INT64",     mode = "NULLABLE" },
    { name = "status",        type = "STRING",    mode = "REQUIRED" },
    { name = "error_message", type = "STRING",    mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "coaching_log" {
  dataset_id          = google_bigquery_dataset.garmin.dataset_id
  table_id            = "coaching_log"
  deletion_protection = false

  schema = jsonencode([
    { name = "id",         type = "STRING",    mode = "REQUIRED" },
    { name = "session_id", type = "STRING",    mode = "REQUIRED" },
    { name = "email",      type = "STRING",    mode = "REQUIRED" },
    { name = "date",       type = "DATE",      mode = "REQUIRED" },
    { name = "timestamp",  type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "category",   type = "STRING",    mode = "REQUIRED" },
    { name = "content",    type = "STRING",    mode = "REQUIRED" },
    { name = "context",    type = "STRING",    mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "morning_checkin" {
  dataset_id          = google_bigquery_dataset.garmin.dataset_id
  table_id            = "morning_checkin"
  deletion_protection = false

  schema = jsonencode([
    { name = "date",           type = "STRING",    mode = "REQUIRED" },
    { name = "submitted_at",   type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "feeling",        type = "STRING",    mode = "NULLABLE" },
    { name = "mood_score",     type = "INT64",     mode = "NULLABLE" },
    { name = "working_out",    type = "BOOL",      mode = "NULLABLE" },
    { name = "stretching",     type = "BOOL",      mode = "NULLABLE" },
    { name = "drinks_tonight", type = "BOOL",      mode = "NULLABLE" },
    { name = "notes",          type = "STRING",    mode = "NULLABLE" },
    { name = "priority",       type = "STRING",    mode = "NULLABLE" },
    { name = "fill_in_blank",  type = "STRING",    mode = "NULLABLE" },
    { name = "source",         type = "STRING",    mode = "NULLABLE" },
    { name = "submission_id",  type = "STRING",    mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "evening_checkin" {
  dataset_id          = google_bigquery_dataset.garmin.dataset_id
  table_id            = "evening_checkin"
  deletion_protection = false

  schema = jsonencode([
    { name = "date",           type = "STRING",    mode = "REQUIRED" },
    { name = "submitted_at",   type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "did_workout",    type = "BOOL",      mode = "NULLABLE" },
    { name = "alcohol_drinks", type = "FLOAT64",   mode = "NULLABLE" },
    { name = "tracked_eating", type = "BOOL",      mode = "NULLABLE" },
    { name = "feeling",        type = "STRING",    mode = "NULLABLE" },
    { name = "mood_score",     type = "INT64",     mode = "NULLABLE" },
    { name = "worked_late",    type = "BOOL",      mode = "NULLABLE" },
    { name = "notes",          type = "STRING",    mode = "NULLABLE" },
    { name = "gratitude",      type = "STRING",    mode = "NULLABLE" },
    { name = "chocolate",      type = "STRING",    mode = "NULLABLE" },
    { name = "source",         type = "STRING",    mode = "NULLABLE" },
    { name = "submission_id",  type = "STRING",    mode = "REQUIRED" },
  ])
}

resource "google_project_iam_member" "sa_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${var.sa_email}"
}

resource "google_project_iam_member" "sa_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.sa_email}"
}

resource "google_project_iam_member" "sa_cloudscheduler_admin" {
  project = var.project_id
  role    = "roles/cloudscheduler.admin"
  member  = "serviceAccount:${var.sa_email}"
}

resource "google_cloud_scheduler_job" "reminders" {
  name      = "cycling-coach-reminders"
  region    = var.region
  schedule  = "*/30 * * * *"
  time_zone = "Europe/London"

  http_target {
    http_method = "POST"
    uri         = "https://cycling-coach-l3h3kcxbia-nw.a.run.app/api/send-reminders"
  }

  depends_on = [
    google_project_service.apis["cloudscheduler.googleapis.com"],
    google_project_iam_member.sa_cloudscheduler_admin,
  ]
}
