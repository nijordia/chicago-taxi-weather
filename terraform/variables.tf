variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "chicago_taxi_weather"
}

variable "dataset_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "owner_email" {
  description = "Email of the project owner (has access to payment_type column)"
  type        = string
}
