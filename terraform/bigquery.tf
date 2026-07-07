resource "google_bigquery_dataset" "garmin" {
  dataset_id  = var.env == "prod" ? "garmin" : "garmin_dev"
  description = "Garmin activity and stats data"
  location    = var.region

  depends_on = [google_project_service.apis["bigquery.googleapis.com"]]
}

resource "google_bigquery_dataset" "data_control" {
  dataset_id  = var.env == "prod" ? "data_control" : "data_control_dev"
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

resource "google_bigquery_table" "calorie_entries" {
  dataset_id          = google_bigquery_dataset.garmin.dataset_id
  table_id            = "calorie_entries"
  deletion_protection = false

  schema = jsonencode([
    { name = "date",           type = "STRING",    mode = "REQUIRED" },
    { name = "calories_eaten", type = "INT64",     mode = "NULLABLE" },
    { name = "sugar_g",        type = "FLOAT64",   mode = "NULLABLE" },
    { name = "protein_g",      type = "FLOAT64",   mode = "NULLABLE" },
    { name = "notes",          type = "STRING",    mode = "NULLABLE" },
    { name = "updated_at",     type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

# Estimates hrv_status/hrv_avg/sleep_score for rows where Garmin didn't report them
# (e.g. an older watch with no HRV sensor). Estimates are heuristic, not real Garmin
# readings — every row it touches gets is_estimated_biometrics = TRUE so downstream
# consumers (dashboard, chat agent) can tell real data from filled-in data.
#
# NOTE: garmin_stats always lives in the literal "garmin" dataset, never "garmin_dev" —
# pipeline/bigquery_writer.py hardcodes that dataset name regardless of environment,
# since there is only one physical Garmin watch/user and biometric history isn't
# environment-scoped the way app state (calorie_entries, coaching_log, etc.) is. This
# routine's home dataset follows the same convention for consistency.
#
# Both deploy-dev.yml and deploy-prod.yml now idempotently `tofu import` this routine
# before apply (see those workflows) — this is a real shared GCP object between the two
# environments' Terraform states, same as the shared GCS bucket / Artifact Registry repo.
resource "google_bigquery_routine" "fill_missing_biometrics" {
  dataset_id   = "garmin"
  routine_id   = "fill_missing_biometrics"
  routine_type = "PROCEDURE"
  language     = "SQL"

  definition_body = <<-EOSQL
    BEGIN
      ALTER TABLE `${var.project_id}.garmin.garmin_stats`
        ADD COLUMN IF NOT EXISTS is_estimated_biometrics BOOL;

      -- Sleep score: derive a 0-100 score from total sleep duration (peaks near 8h)
      -- and the deep+REM share of that sleep, when Garmin reported sleep stages but
      -- no overall score for the day.
      UPDATE `${var.project_id}.garmin.garmin_stats` t
      SET
        sleep_score = CAST(ROUND(GREATEST(0, LEAST(100,
          0.5 * GREATEST(0, 100 - ABS(t.sleep_total_hr - 8) * 12)
          + 0.5 * GREATEST(0, LEAST(100, SAFE_DIVIDE(t.sleep_deep_hr + t.sleep_rem_hr, t.sleep_total_hr) * 100))
        ))) AS INT64),
        is_estimated_biometrics = TRUE
      WHERE t.sleep_score IS NULL
        AND t.sleep_total_hr IS NOT NULL
        AND t.sleep_total_hr > 0;

      -- HRV status/avg: no real HRV sensor reading exists to estimate from, so instead
      -- derive a recovery-direction proxy from how today's resting HR compares to a
      -- trailing 14-day baseline, combined with stress and body battery.
      UPDATE `${var.project_id}.garmin.garmin_stats` t
      SET
        hrv_status = CASE WHEN e.recovery_score >= 0 THEN 'BALANCED' ELSE 'UNBALANCED' END,
        hrv_avg = CAST(GREATEST(10, LEAST(200, 50 + e.recovery_score)) AS FLOAT64),
        is_estimated_biometrics = TRUE
      FROM (
        WITH baseline AS (
          SELECT
            date,
            rhr,
            avg_stress,
            body_battery,
            hrv_status,
            AVG(rhr) OVER (
              ORDER BY date
              ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
            ) AS rhr_baseline
          FROM `${var.project_id}.garmin.garmin_stats`
          QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY run_date DESC, timestamp DESC) = 1
        )
        SELECT
          date,
          (COALESCE(rhr_baseline - rhr, 0) * 2)
            - COALESCE(avg_stress - 35, 0) * 0.5
            + COALESCE(body_battery - 50, 0) * 0.3 AS recovery_score
        FROM baseline
        WHERE hrv_status IS NULL AND rhr_baseline IS NOT NULL AND rhr IS NOT NULL
      ) e
      WHERE t.date = e.date
        AND t.hrv_status IS NULL;
    END
  EOSQL
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
  name      = "cycling-coach${local.env_suffix}-reminders"
  region    = var.region
  schedule  = "*/5 * * * *"
  time_zone = "Europe/London"

  http_target {
    http_method = "POST"
    uri         = "https://cycling-coach${local.env_suffix}-l3h3kcxbia-nw.a.run.app/api/send-reminders"
  }

  depends_on = [
    google_project_service.apis["cloudscheduler.googleapis.com"],
    google_project_iam_member.sa_cloudscheduler_admin,
  ]
}
