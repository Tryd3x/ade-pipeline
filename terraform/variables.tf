variable "google_credentials_path" {
  description = "Google credentials to access service account"
  default     = "../keys/gcs-credentials.json"
}

variable "project" {
  description = "Project"
  default     = "ade-pipeline"
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
  default     = "ade-pipeline-bucket"
}

# variable "labels" {
#   description = "Tags for filter purposes"
#   default = {
#     environmnet = "dev"
#     project     = "ade-pipeline"
#   }

# }
