# Copyright 2026 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# BigQuery Dashboard Resources for EDPA Reporting Dashboard

# --- Dataset ---

resource "google_bigquery_dataset" "dashboard_views" {
  dataset_id = "dashboard_views"
  project    = data.google_client_config.default.project
  location   = data.google_client_config.default.region
}


# --- UDFs ---

resource "google_bigquery_routine" "external_id_to_api_id" {
  dataset_id   = google_bigquery_dataset.dashboard_views.dataset_id
  project      = data.google_client_config.default.project
  routine_id   = "externalIdToApiId"
  routine_type = "SCALAR_FUNCTION"
  language     = "JAVASCRIPT"

  arguments {
    name      = "id"
    data_type = jsonencode({ "typeKind" : "INT64" })
  }

  return_type = jsonencode({ "typeKind" : "STRING" })

  definition_body = <<-JS
    var bytes = new Uint8Array(8);
    var val_bi = BigInt(id);
    for (var i = 7; i >= 0; i--) {
      bytes[i] = Number(val_bi & 0xFFn);
      val_bi >>= 8n;
    }
    var chars = '';
    for (var i = 0; i < bytes.length; i++) {
      chars += String.fromCharCode(bytes[i]);
    }
    var lookup = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
    var result = '';
    for (var i = 0; i < chars.length; i += 3) {
      var c1 = chars.charCodeAt(i);
      var c2 = i + 1 < chars.length ? chars.charCodeAt(i + 1) : 0;
      var c3 = i + 2 < chars.length ? chars.charCodeAt(i + 2) : 0;
      result += lookup[c1 >> 2];
      result += lookup[((c1 & 3) << 4) | (c2 >> 4)];
      if (i + 1 < chars.length) result += lookup[((c2 & 15) << 2) | (c3 >> 6)];
      if (i + 2 < chars.length) result += lookup[c3 & 63];
    }
    return result;
  JS
}

resource "google_bigquery_routine" "decode_event_group_details" {
  dataset_id   = google_bigquery_dataset.dashboard_views.dataset_id
  project      = data.google_client_config.default.project
  routine_id   = "decode_EventGroupDetails"
  routine_type = "SCALAR_FUNCTION"
  language     = "JAVASCRIPT"

  arguments {
    name      = "b"
    data_type = jsonencode({ "typeKind" : "BYTES" })
  }

  return_type = jsonencode({ "typeKind" : "STRING" })

  imported_libraries = [
    "gs://xmm-dashboard/lib/protobuf.global.min.js",
    "gs://xmm-dashboard/descriptors/kingdom_descriptor.js",
  ]

  definition_body = <<-JS
    const root = protobuf.Root.fromJSON(DESCRIPTOR_kingdom);
    const T = root.lookupType('wfa.measurement.internal.kingdom.EventGroupDetails');
    const m = T.decode(new Uint8Array(b));
    return JSON.stringify(T.toObject(m, {longs: Number, enums: String, defaults: true}));
  JS
}

resource "google_bigquery_routine" "decode_basic_report_details" {
  dataset_id   = google_bigquery_dataset.dashboard_views.dataset_id
  project      = data.google_client_config.default.project
  routine_id   = "decode_BasicReportDetails"
  routine_type = "SCALAR_FUNCTION"
  language     = "JAVASCRIPT"

  arguments {
    name      = "b"
    data_type = jsonencode({ "typeKind" : "BYTES" })
  }

  return_type = jsonencode({ "typeKind" : "STRING" })

  imported_libraries = [
    "gs://xmm-dashboard/lib/protobuf.global.min.js",
    "gs://xmm-dashboard/descriptors/BasicReportDetails_descriptor.js",
  ]

  definition_body = <<-JS
    const root = protobuf.Root.fromJSON(DESCRIPTOR_BasicReportDetails);
    const T = root.lookupType('wfa.measurement.internal.reporting.v2.BasicReportDetails');
    const m = T.decode(new Uint8Array(b));
    return JSON.stringify(T.toObject(m, {longs: Number, enums: String, defaults: true}));
  JS
}

resource "google_bigquery_routine" "decode_data_provider_details" {
  dataset_id   = google_bigquery_dataset.dashboard_views.dataset_id
  project      = data.google_client_config.default.project
  routine_id   = "decode_DataProviderDetails"
  routine_type = "SCALAR_FUNCTION"
  language     = "JAVASCRIPT"

  arguments {
    name      = "b"
    data_type = jsonencode({ "typeKind" : "BYTES" })
  }

  return_type = jsonencode({ "typeKind" : "STRING" })

  imported_libraries = [
    "gs://xmm-dashboard/lib/protobuf.global.min.js",
    "gs://xmm-dashboard/descriptors/kingdom_descriptor.js",
  ]

  definition_body = <<-JS
    const root = protobuf.Root.fromJSON(DESCRIPTOR_kingdom);
    const T = root.lookupType('wfa.measurement.internal.kingdom.DataProviderDetails');
    const m = T.decode(new Uint8Array(b));
    return JSON.stringify(T.toObject(m, {longs: Number, enums: String, defaults: true}));
  JS
}
# --- BigQuery Connections (Spanner with Data Boost) ---

resource "google_bigquery_connection" "edp_aggregator" {
  connection_id = "edp-aggregator-conn"
  project       = data.google_client_config.default.project
  location      = data.google_client_config.default.region

  cloud_spanner {
    database        = "projects/${data.google_client_config.default.project}/instances/${google_spanner_instance.spanner_instance.name}/databases/edp-aggregator"
    use_data_boost  = true
    use_parallelism = true
  }
}

resource "google_bigquery_connection" "kingdom" {
  connection_id = "kingdom-conn"
  project       = data.google_client_config.default.project
  location      = data.google_client_config.default.region

  cloud_spanner {
    database        = "projects/${data.google_client_config.default.project}/instances/${google_spanner_instance.spanner_instance.name}/databases/kingdom"
    use_data_boost  = true
    use_parallelism = true
  }
}

resource "google_bigquery_connection" "reporting" {
  connection_id = "reporting-conn"
  project       = data.google_client_config.default.project
  location      = data.google_client_config.default.region

  cloud_spanner {
    database        = "projects/${data.google_client_config.default.project}/instances/${google_spanner_instance.spanner_instance.name}/databases/reporting"
    use_data_boost  = true
    use_parallelism = true
  }
}

# --- BigQuery Connection (Cloud SQL - Reporting Postgres) ---

resource "google_bigquery_connection" "reporting_postgres" {
  connection_id = "reporting-postgres-conn"
  project       = data.google_client_config.default.project
  location      = data.google_client_config.default.region

  cloud_sql {
    instance_id = google_sql_database_instance.postgres.connection_name
    database    = "reporting-v2"
    type        = "POSTGRES"

    credential {
      username = google_sql_user.postgres.name
      password = google_sql_user.postgres.password
    }
  }
}

# IAM: BigQuery Connection Service Agent needs roles/cloudsql.client
resource "google_project_iam_member" "bq_cloudsql_client" {
  project = data.google_client_config.default.project
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-bigqueryconnection.iam.gserviceaccount.com"
}

data "google_project" "project" {
  project_id = data.google_client_config.default.project
}

# --- Per-EDP Views ---

resource "google_bigquery_table" "requisition_overview" {
  for_each   = var.data_provider_resource_ids
  dataset_id = google_bigquery_dataset.dashboard_views.dataset_id
  project    = data.google_client_config.default.project
  table_id   = "requisition_overview_${each.key}"

  view {
    query = templatefile("${path.module}/sql/requisition_overview.sql", {
      project_id       = data.google_client_config.default.project
      region           = data.google_client_config.default.region
      data_provider_id = each.value
    })
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "event_groups_summary" {
  for_each   = var.data_provider_resource_ids
  dataset_id = google_bigquery_dataset.dashboard_views.dataset_id
  project    = data.google_client_config.default.project
  table_id   = "event_groups_summary_${each.key}"

  view {
    query = templatefile("${path.module}/sql/event_groups_summary.sql", {
      project_id       = data.google_client_config.default.project
      region           = data.google_client_config.default.region
      data_provider_id = each.value
    })
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "account_ids" {
  for_each   = var.data_provider_resource_ids
  dataset_id = google_bigquery_dataset.dashboard_views.dataset_id
  project    = data.google_client_config.default.project
  table_id   = "account_ids_${each.key}"

  view {
    query = templatefile("${path.module}/sql/account_ids.sql", {
      project_id       = data.google_client_config.default.project
      region           = data.google_client_config.default.region
      data_provider_id = each.value
    })
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "edp_coverage" {
  for_each   = var.data_provider_resource_ids
  dataset_id = google_bigquery_dataset.dashboard_views.dataset_id
  project    = data.google_client_config.default.project
  table_id   = "edp_coverage_${each.key}"

  view {
    query = templatefile("${path.module}/sql/edp_coverage.sql", {
      project_id       = data.google_client_config.default.project
      region           = data.google_client_config.default.region
      data_provider_id = each.value
    })
    use_legacy_sql = false
  }
}

# --- Platform Views (no EDP filter) ---

resource "google_bigquery_table" "requisition_overview_platform" {
  dataset_id = google_bigquery_dataset.dashboard_views.dataset_id
  project    = data.google_client_config.default.project
  table_id   = "requisition_overview_platform"

  view {
    query = templatefile("${path.module}/sql/requisition_overview.sql", {
      project_id       = data.google_client_config.default.project
      region           = data.google_client_config.default.region
      data_provider_id = ""
    })
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "event_groups_summary_platform" {
  dataset_id = google_bigquery_dataset.dashboard_views.dataset_id
  project    = data.google_client_config.default.project
  table_id   = "event_groups_summary_platform"

  view {
    query = templatefile("${path.module}/sql/event_groups_summary.sql", {
      project_id       = data.google_client_config.default.project
      region           = data.google_client_config.default.region
      data_provider_id = ""
    })
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "account_ids_platform" {
  dataset_id = google_bigquery_dataset.dashboard_views.dataset_id
  project    = data.google_client_config.default.project
  table_id   = "account_ids_platform"

  view {
    query = templatefile("${path.module}/sql/account_ids.sql", {
      project_id       = data.google_client_config.default.project
      region           = data.google_client_config.default.region
      data_provider_id = ""
    })
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "edp_coverage_platform" {
  dataset_id = google_bigquery_dataset.dashboard_views.dataset_id
  project    = data.google_client_config.default.project
  table_id   = "edp_coverage_platform"

  view {
    query = templatefile("${path.module}/sql/edp_coverage.sql", {
      project_id       = data.google_client_config.default.project
      region           = data.google_client_config.default.region
      data_provider_id = ""
    })
    use_legacy_sql = false
  }
}

# --- Per-EDP Service Accounts and IAM ---

resource "google_service_account" "edp_dashboard" {
  for_each     = var.data_provider_resource_ids
  account_id   = "edp-${each.key}-dashboard"
  display_name = "EDP ${each.key} Dashboard"
  project      = data.google_client_config.default.project
}

# Per-view IAM: each EDP service account can only access their own views
resource "google_bigquery_table_iam_member" "requisition_overview_viewer" {
  for_each   = var.data_provider_resource_ids
  project    = data.google_client_config.default.project
  dataset_id = google_bigquery_dataset.dashboard_views.dataset_id
  table_id   = google_bigquery_table.requisition_overview[each.key].table_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.edp_dashboard[each.key].email}"
}

resource "google_bigquery_table_iam_member" "event_groups_summary_viewer" {
  for_each   = var.data_provider_resource_ids
  project    = data.google_client_config.default.project
  dataset_id = google_bigquery_dataset.dashboard_views.dataset_id
  table_id   = google_bigquery_table.event_groups_summary[each.key].table_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.edp_dashboard[each.key].email}"
}

resource "google_bigquery_table_iam_member" "account_ids_viewer" {
  for_each   = var.data_provider_resource_ids
  project    = data.google_client_config.default.project
  dataset_id = google_bigquery_dataset.dashboard_views.dataset_id
  table_id   = google_bigquery_table.account_ids[each.key].table_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.edp_dashboard[each.key].email}"
}

resource "google_bigquery_table_iam_member" "edp_coverage_viewer" {
  for_each   = var.data_provider_resource_ids
  project    = data.google_client_config.default.project
  dataset_id = google_bigquery_dataset.dashboard_views.dataset_id
  table_id   = google_bigquery_table.edp_coverage[each.key].table_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.edp_dashboard[each.key].email}"
}
