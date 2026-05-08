
# Temporary: allow deletion of per-EDP views from old dataset
# Remove after successful apply

resource "google_bigquery_table" "requisition_overview_old" {
  for_each            = var.data_provider_resource_ids
  dataset_id          = google_bigquery_dataset.dashboard_views.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "requisition_overview_${each.key}"
  deletion_protection = false
  view {
    query          = "SELECT 1"
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "mc_details_old" {
  for_each            = var.data_provider_resource_ids
  dataset_id          = google_bigquery_dataset.dashboard_views.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "mc_details_${each.key}"
  deletion_protection = false
  view {
    query          = "SELECT 1"
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "edp_coverage_old" {
  for_each            = var.data_provider_resource_ids
  dataset_id          = google_bigquery_dataset.dashboard_views.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "edp_coverage_${each.key}"
  deletion_protection = false
  view {
    query          = "SELECT 1"
    use_legacy_sql = false
  }
}
