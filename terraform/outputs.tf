output "dataset_id" {
  value = google_bigquery_dataset.main.dataset_id
}

output "service_account_email" {
  value = google_service_account.pipeline_sa.email
}

output "cloud_function_url" {
  value = google_cloudfunctions2_function.weather_ingestion.url
}

output "scheduler_job_name" {
  value = google_cloud_scheduler_job.daily_weather.name
}
