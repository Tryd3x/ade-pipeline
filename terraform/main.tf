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

module "ade_bucket" {
	source = "./modules/storage"
	gcs_bucket_name = var.gcs_bucket_name
	region = var.region 
}

# Initializes `ade_external_staging` and `ade_core`
module "ade_bq_dataset" {
	source = "./modules/bigquery"
	region = var.region
}