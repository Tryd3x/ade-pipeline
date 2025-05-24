terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.30.0"
    }
  }
}

provider "google" {
  credentials = file(var.google_credentials_path)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "ade-pipeline" {
  name          = var.gcs_bucket_name
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }

    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "ade_dataset_staging" {
  dataset_id                 = "ade_dataset_staging"
  location                   = var.region
  delete_contents_on_destroy = true
}