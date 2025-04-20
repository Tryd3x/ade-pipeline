variable "google_credentials_path" {
  description = "Google crenentials to access service account"
  default     = "../keys/gcs-credentials.json"
}

variable "project" {
  description = "Project"
  default     = "zoomcamp-454219"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "zoomcamp-454219-ade-pipeline"
}

variable "gcs_log_bucket_name" {
  description = "Name of the bucket to dump bucket logs"
  default     = "zoomcamp-454219-ade-pipeline-logs"
}

variable "labels" {
  description = "Tags for filter purposes"
  default = {
    environmnet = "dev"
    project     = "ade-pipeline"
  }

}
