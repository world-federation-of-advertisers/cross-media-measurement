
# Temporary resources to allow Terraform to destroy old views
# Remove this block after successful apply

resource "google_bigquery_table" "event_groups_summary" {
  for_each            = var.data_provider_resource_ids
  dataset_id          = google_bigquery_dataset.dashboard_views.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "event_groups_summary_${each.key}"
  deletion_protection = false

  view {
    query          = "SELECT 1"
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "event_groups_summary_platform" {
  dataset_id          = google_bigquery_dataset.dashboard_views.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "event_groups_summary_platform"
  deletion_protection = false

  view {
    query          = "SELECT 1"
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "account_ids" {
  for_each            = var.data_provider_resource_ids
  dataset_id          = google_bigquery_dataset.dashboard_views.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "account_ids_${each.key}"
  deletion_protection = false

  view {
    query          = "SELECT 1"
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "account_ids_platform" {
  dataset_id          = google_bigquery_dataset.dashboard_views.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "account_ids_platform"
  deletion_protection = false

  view {
    query          = "SELECT 1"
    use_legacy_sql = false
  }
}
