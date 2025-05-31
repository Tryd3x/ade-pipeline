# resource "google_bigquery_dataset" "ade_core" {
#   dataset_id                 = "ade_core"
#   location                   = var.region
#   delete_contents_on_destroy = true
# }

resource "google_bigquery_dataset" "ade_external_staging" {
  dataset_id                 = "ade_external_staging"
  location                   = var.region
  delete_contents_on_destroy = true
}

