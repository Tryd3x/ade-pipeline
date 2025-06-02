resource "google_bigquery_dataset" "ade_external" {
  dataset_id                 = "ade_external"
  location                   = var.region
  delete_contents_on_destroy = true
}
