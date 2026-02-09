terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ------------------------------------------------------------------------------
# Enable required APIs
# ------------------------------------------------------------------------------
resource "google_project_service" "bigquery" {
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudfunctions" {
  service            = "cloudfunctions.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudscheduler" {
  service            = "cloudscheduler.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudbuild" {
  service            = "cloudbuild.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "run" {
  service            = "run.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "artifactregistry" {
  service            = "artifactregistry.googleapis.com"
  disable_on_destroy = false
}

# ------------------------------------------------------------------------------
# Service Account for ingestion & dbt
# ------------------------------------------------------------------------------
resource "google_service_account" "pipeline_sa" {
  account_id   = "pipeline-sa"
  display_name = "Pipeline Service Account (ingestion + dbt)"
}

resource "google_project_iam_member" "pipeline_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "pipeline_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# ------------------------------------------------------------------------------
# BigQuery Dataset
# ------------------------------------------------------------------------------
resource "google_bigquery_dataset" "main" {
  dataset_id = var.dataset_id
  location   = var.dataset_location

  access {
    role          = "OWNER"
    user_by_email = var.owner_email
  }

  access {
    role          = "WRITER"
    user_by_email = google_service_account.pipeline_sa.email
  }

  depends_on = [google_project_service.bigquery]
}

# ------------------------------------------------------------------------------
# Bronze: Weather Daily Table (partitioned by date)
# ------------------------------------------------------------------------------
resource "google_bigquery_table" "bronze_weather_daily" {
  dataset_id          = google_bigquery_dataset.main.dataset_id
  table_id            = "bronze_weather_daily"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  schema = jsonencode([
    { name = "date",                      type = "DATE",      mode = "REQUIRED" },
    { name = "temperature_2m_max_c",      type = "FLOAT64",   mode = "NULLABLE" },
    { name = "temperature_2m_min_c",      type = "FLOAT64",   mode = "NULLABLE" },
    { name = "temperature_2m_mean_c",     type = "FLOAT64",   mode = "NULLABLE" },
    { name = "precipitation_sum_mm",      type = "FLOAT64",   mode = "NULLABLE" },
    { name = "rain_sum_mm",               type = "FLOAT64",   mode = "NULLABLE" },
    { name = "snowfall_sum_cm",           type = "FLOAT64",   mode = "NULLABLE" },
    { name = "wind_speed_10m_max_kmh",    type = "FLOAT64",   mode = "NULLABLE" },
    { name = "wind_gusts_10m_max_kmh",    type = "FLOAT64",   mode = "NULLABLE" },
    { name = "weather_code",              type = "INT64",     mode = "NULLABLE" },
    { name = "weather_description",       type = "STRING",    mode = "NULLABLE" },
    { name = "shortwave_radiation_sum_mj",type = "FLOAT64",   mode = "NULLABLE" },
    { name = "ingestion_timestamp",       type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "data_source",               type = "STRING",    mode = "NULLABLE" }
  ])
}

# ------------------------------------------------------------------------------
# Bronze: Taxi Trips Filtered (copy from public dataset via scheduled query)
# We create the destination table; data is loaded by a one-time job in scripts.
# ------------------------------------------------------------------------------
resource "google_bigquery_table" "bronze_taxi_trips_filtered" {
  dataset_id          = google_bigquery_dataset.main.dataset_id
  table_id            = "bronze_taxi_trips_filtered"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "trip_start_timestamp"
  }

  schema = jsonencode([
    { name = "unique_key",            type = "STRING",    mode = "NULLABLE" },
    { name = "taxi_id",               type = "STRING",    mode = "NULLABLE" },
    { name = "trip_start_timestamp",  type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "trip_end_timestamp",    type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "trip_seconds",          type = "INT64",     mode = "NULLABLE" },
    { name = "trip_miles",            type = "FLOAT64",   mode = "NULLABLE" },
    { name = "pickup_census_tract",   type = "STRING",    mode = "NULLABLE" },
    { name = "dropoff_census_tract",  type = "STRING",    mode = "NULLABLE" },
    { name = "pickup_community_area", type = "INT64",     mode = "NULLABLE" },
    { name = "dropoff_community_area",type = "INT64",     mode = "NULLABLE" },
    { name = "fare",                  type = "FLOAT64",   mode = "NULLABLE" },
    { name = "tips",                  type = "FLOAT64",   mode = "NULLABLE" },
    { name = "tolls",                 type = "FLOAT64",   mode = "NULLABLE" },
    { name = "extras",                type = "FLOAT64",   mode = "NULLABLE" },
    { name = "trip_total",            type = "FLOAT64",   mode = "NULLABLE" },
    { name = "payment_type",          type = "STRING",    mode = "NULLABLE" },
    { name = "company",               type = "STRING",    mode = "NULLABLE" },
    { name = "pickup_latitude",       type = "FLOAT64",   mode = "NULLABLE" },
    { name = "pickup_longitude",      type = "FLOAT64",   mode = "NULLABLE" },
    { name = "dropoff_latitude",      type = "FLOAT64",   mode = "NULLABLE" },
    { name = "dropoff_longitude",     type = "FLOAT64",   mode = "NULLABLE" },
    { name = "pickup_location",       type = "STRING",    mode = "NULLABLE" },
    { name = "dropoff_location",      type = "STRING",    mode = "NULLABLE" }
  ])
}

# ------------------------------------------------------------------------------
# Authorized View: excludes payment_type for general access
# ------------------------------------------------------------------------------
resource "google_bigquery_table" "bronze_taxi_trips_secure" {
  dataset_id          = google_bigquery_dataset.main.dataset_id
  table_id            = "bronze_taxi_trips_secure"
  deletion_protection = false

  view {
    query          = <<-SQL
      SELECT
        unique_key,
        taxi_id,
        trip_start_timestamp,
        trip_end_timestamp,
        trip_seconds,
        trip_miles,
        pickup_census_tract,
        dropoff_census_tract,
        pickup_community_area,
        dropoff_community_area,
        fare,
        tips,
        tolls,
        extras,
        trip_total,
        company,
        pickup_latitude,
        pickup_longitude,
        dropoff_latitude,
        dropoff_longitude,
        pickup_location,
        dropoff_location
      FROM `${var.project_id}.${var.dataset_id}.bronze_taxi_trips_filtered`
    SQL
    use_legacy_sql = false
  }

  depends_on = [google_bigquery_table.bronze_taxi_trips_filtered]
}

# ------------------------------------------------------------------------------
# Cloud Storage bucket for Cloud Function source code
# ------------------------------------------------------------------------------
resource "google_storage_bucket" "functions_bucket" {
  name                        = "${var.project_id}-cf-source"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}

resource "google_storage_bucket_object" "function_zip" {
  name   = "ingestion-${filemd5("${path.module}/../ingestion/function_source.zip")}.zip"
  bucket = google_storage_bucket.functions_bucket.name
  source = "${path.module}/../ingestion/function_source.zip"
}

# ------------------------------------------------------------------------------
# Cloud Function (Gen2) for daily weather ingestion
# ------------------------------------------------------------------------------
resource "google_cloudfunctions2_function" "weather_ingestion" {
  name     = "weather-ingestion"
  location = var.region

  build_config {
    runtime     = "python311"
    entry_point = "ingest_weather"

    source {
      storage_source {
        bucket = google_storage_bucket.functions_bucket.name
        object = google_storage_bucket_object.function_zip.name
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "256M"
    timeout_seconds    = 300

    environment_variables = {
      GCP_PROJECT = var.project_id
      BQ_DATASET  = var.dataset_id
      BQ_TABLE    = "bronze_weather_daily"
    }

    service_account_email = google_service_account.pipeline_sa.email
  }

  depends_on = [
    google_project_service.cloudfunctions,
    google_project_service.cloudbuild,
    google_project_service.run,
    google_project_service.artifactregistry
  ]
}

# Allow Cloud Scheduler to invoke the function
resource "google_cloud_run_v2_service_iam_member" "invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.weather_ingestion.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# ------------------------------------------------------------------------------
# Cloud Scheduler: daily at 06:00 UTC
# ------------------------------------------------------------------------------
resource "google_cloud_scheduler_job" "daily_weather" {
  name      = "daily-weather-ingestion"
  region    = var.region
  schedule  = "0 6 * * *"
  time_zone = "UTC"

  http_target {
    uri         = google_cloudfunctions2_function.weather_ingestion.url
    http_method = "POST"

    oidc_token {
      service_account_email = google_service_account.pipeline_sa.email
    }
  }

  depends_on = [google_project_service.cloudscheduler]
}
