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
# Architecture: Materialized tables with row access policies (Section 3.1)

# --- Dataset ---

resource "google_bigquery_dataset" "dashboard" {
  dataset_id = "dashboard"
  project    = data.google_client_config.default.project
  location   = data.google_client_config.default.region
}

# --- UDFs ---

resource "google_bigquery_routine" "external_id_to_api_id" {
  dataset_id   = google_bigquery_dataset.dashboard.dataset_id
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
  dataset_id   = google_bigquery_dataset.dashboard.dataset_id
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
    "gs://${google_storage_bucket.dashboard_udfs.name}/lib/protobuf.global.min.js",
    "gs://${google_storage_bucket.dashboard_udfs.name}/descriptors/kingdom_descriptor.js",
  ]

  definition_body = <<-JS
    // SECURITY: Allowlisted output only. Do not return full decoded proto.
    // Adding new fields requires security review for cross-EDP data leakage.
    const root = protobuf.Root.fromJSON(DESCRIPTOR_kingdom);
    const T = root.lookupType('wfa.measurement.internal.kingdom.EventGroupDetails');
    const m = T.decode(new Uint8Array(b));
    const decoded = T.toObject(m, {longs: Number, enums: String, defaults: true});
    return JSON.stringify({
      metadata: {
        adMetadata: {
          campaignMetadata: {
            brandName: (decoded.metadata && decoded.metadata.adMetadata && decoded.metadata.adMetadata.campaignMetadata) ? decoded.metadata.adMetadata.campaignMetadata.brandName : null,
            campaignName: (decoded.metadata && decoded.metadata.adMetadata && decoded.metadata.adMetadata.campaignMetadata) ? decoded.metadata.adMetadata.campaignMetadata.campaignName : null
          }
        }
      }
    });
  JS
}

resource "google_bigquery_routine" "decode_basic_report_details" {
  dataset_id   = google_bigquery_dataset.dashboard.dataset_id
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
    "gs://${google_storage_bucket.dashboard_udfs.name}/lib/protobuf.global.min.js",
    "gs://${google_storage_bucket.dashboard_udfs.name}/descriptors/reporting_descriptor.js",
  ]

  definition_body = <<-JS
    // SECURITY: Allowlisted output only. Do not return full decoded proto.
    // EXCLUDED: result_group_specs (contains data_provider_keys listing all EDPs).
    // Adding new fields requires security review for cross-EDP data leakage.
    const root = protobuf.Root.fromJSON(DESCRIPTOR_reporting);
    const T = root.lookupType('wfa.measurement.internal.reporting.v2.BasicReportDetails');
    const m = T.decode(new Uint8Array(b));
    const decoded = T.toObject(m, {longs: Number, enums: String, defaults: true});
    return JSON.stringify({
      title: decoded.title || null,
      reportingInterval: decoded.reportingInterval || null,
      impressionQualificationFilters: decoded.impressionQualificationFilters || null
    });
  JS
}

resource "google_bigquery_routine" "decode_basic_report_result_details" {
  dataset_id   = google_bigquery_dataset.dashboard.dataset_id
  project      = data.google_client_config.default.project
  routine_id   = "decode_BasicReportResultDetails"
  routine_type = "SCALAR_FUNCTION"
  language     = "JAVASCRIPT"

  arguments {
    name      = "b"
    data_type = jsonencode({ "typeKind" : "BYTES" })
  }

  return_type = jsonencode({ "typeKind" : "STRING" })

  imported_libraries = [
    "gs://${google_storage_bucket.dashboard_udfs.name}/lib/protobuf.global.min.js",
    "gs://${google_storage_bucket.dashboard_udfs.name}/descriptors/reporting_descriptor.js",
  ]

  definition_body = <<-JS
    // SECURITY: Allowlisted output only. Do not return full decoded proto.
    // EXCLUDED: Any future top-level aggregate fields that span across EDPs.
    // The view filters per-EDP at the component level via cmmsDataProviderId.
    // Adding new fields requires security review for cross-EDP data leakage.
    const root = protobuf.Root.fromJSON(DESCRIPTOR_reporting);
    const T = root.lookupType('wfa.measurement.internal.reporting.v2.BasicReportResultDetails');
    const m = T.decode(new Uint8Array(b));
    const decoded = T.toObject(m, {longs: Number, enums: String, defaults: true});
    return JSON.stringify({
      resultGroups: decoded.resultGroups || []
    });
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

data "google_project" "project" {
  project_id = data.google_client_config.default.project
}

# --- Tables (created empty, populated by scheduled queries via WRITE_TRUNCATE) ---

resource "google_bigquery_table" "requisition_overview" {
  dataset_id          = google_bigquery_dataset.dashboard.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "requisition_overview"
  deletion_protection = false
}

resource "google_bigquery_table" "mc_details" {
  dataset_id          = google_bigquery_dataset.dashboard.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "mc_details"
  deletion_protection = false
}

resource "google_bigquery_table" "mc_details_edp" {
  dataset_id          = google_bigquery_dataset.dashboard.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "mc_details_edp"
  deletion_protection = false
}

resource "google_bigquery_table" "report_detail" {
  dataset_id          = google_bigquery_dataset.dashboard.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "report_detail"
  deletion_protection = false
}

resource "google_bigquery_table" "report_detail_edp" {
  dataset_id          = google_bigquery_dataset.dashboard.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "report_detail_edp"
  deletion_protection = false
}

# --- Scheduled Queries (materialize Spanner data into BigQuery tables) ---

resource "google_bigquery_data_transfer_config" "requisition_overview" {
  display_name           = "Dashboard: requisition_overview"
  data_source_id         = "scheduled_query"
  schedule               = "every 1 hours"
  destination_dataset_id = google_bigquery_dataset.dashboard.dataset_id
  project                = data.google_client_config.default.project
  location               = data.google_client_config.default.region

  params = {
    query                      = templatefile("${path.module}/sql/requisition_overview.sql", {
      project_id = data.google_client_config.default.project
      region     = data.google_client_config.default.region
    })
    destination_table_name_template = "requisition_overview"
    write_disposition               = "WRITE_TRUNCATE"
  }
}

resource "google_bigquery_data_transfer_config" "mc_details" {
  display_name           = "Dashboard: mc_details (platform)"
  data_source_id         = "scheduled_query"
  schedule               = "every 1 hours"
  destination_dataset_id = google_bigquery_dataset.dashboard.dataset_id
  project                = data.google_client_config.default.project
  location               = data.google_client_config.default.region

  params = {
    query                      = templatefile("${path.module}/sql/mc_details.sql", {
      project_id               = data.google_client_config.default.project
      region                   = data.google_client_config.default.region
      include_platform_columns = true
    })
    destination_table_name_template = "mc_details"
    write_disposition               = "WRITE_TRUNCATE"
  }
}

resource "google_bigquery_data_transfer_config" "mc_details_edp" {
  display_name           = "Dashboard: mc_details_edp"
  data_source_id         = "scheduled_query"
  schedule               = "every 1 hours"
  destination_dataset_id = google_bigquery_dataset.dashboard.dataset_id
  project                = data.google_client_config.default.project
  location               = data.google_client_config.default.region

  params = {
    query                      = templatefile("${path.module}/sql/mc_details.sql", {
      project_id               = data.google_client_config.default.project
      region                   = data.google_client_config.default.region
      include_platform_columns = false
    })
    destination_table_name_template = "mc_details_edp"
    write_disposition               = "WRITE_TRUNCATE"
  }
}

resource "google_bigquery_data_transfer_config" "report_detail" {
  display_name           = "Dashboard: report_detail (platform)"
  data_source_id         = "scheduled_query"
  schedule               = "every 1 hours"
  destination_dataset_id = google_bigquery_dataset.dashboard.dataset_id
  project                = data.google_client_config.default.project
  location               = data.google_client_config.default.region

  params = {
    query                      = templatefile("${path.module}/sql/report_detail.sql", {
      project_id               = data.google_client_config.default.project
      region                   = data.google_client_config.default.region
      include_platform_columns = true
    })
    destination_table_name_template = "report_detail"
    write_disposition               = "WRITE_TRUNCATE"
  }
}

resource "google_bigquery_data_transfer_config" "report_detail_edp" {
  display_name           = "Dashboard: report_detail_edp"
  data_source_id         = "scheduled_query"
  schedule               = "every 1 hours"
  destination_dataset_id = google_bigquery_dataset.dashboard.dataset_id
  project                = data.google_client_config.default.project
  location               = data.google_client_config.default.region

  params = {
    query                      = templatefile("${path.module}/sql/report_detail.sql", {
      project_id               = data.google_client_config.default.project
      region                   = data.google_client_config.default.region
      include_platform_columns = false
    })
    destination_table_name_template = "report_detail_edp"
    write_disposition               = "WRITE_TRUNCATE"
  }
}

# --- Per-EDP Service Accounts ---

resource "google_service_account" "edp_dashboard" {
  for_each     = var.data_provider_resource_ids
  account_id   = "edp-${each.key}-dashboard"
  display_name = "EDP ${each.key} Dashboard"
  project      = data.google_client_config.default.project
}

# --- Row Access Policies ---
# Applied to tables that EDPs query. Each EDP SA sees only their own rows.
# Platform operators see all rows.

resource "google_bigquery_job" "row_policy_requisition_overview" {
  for_each   = var.data_provider_resource_ids
  job_id     = "row_policy_req_${each.key}_${formatdate("YYYYMMDDhhmmss", timestamp())}"
  project    = data.google_client_config.default.project
  location   = data.google_client_config.default.region
  depends_on = [google_bigquery_table.requisition_overview, google_bigquery_dataset_iam_member.terraform_data_owner]

  query {
    query          = "CREATE OR REPLACE ROW ACCESS POLICY ${each.key}_filter ON `${data.google_client_config.default.project}.${google_bigquery_dataset.dashboard.dataset_id}.requisition_overview` GRANT TO ('serviceAccount:${google_service_account.edp_dashboard[each.key].email}') FILTER USING (DataProviderResourceId = '${each.value}')"
    use_legacy_sql = false
  }
}

resource "google_bigquery_job" "row_policy_requisition_overview_platform" {
  job_id     = "row_policy_req_platform_${formatdate("YYYYMMDDhhmmss", timestamp())}"
  project    = data.google_client_config.default.project
  location   = data.google_client_config.default.region
  depends_on = [google_bigquery_table.requisition_overview, google_bigquery_dataset_iam_member.terraform_data_owner]

  query {
    query          = "CREATE OR REPLACE ROW ACCESS POLICY platform_full_access ON `${data.google_client_config.default.project}.${google_bigquery_dataset.dashboard.dataset_id}.requisition_overview` GRANT TO ('serviceAccount:${var.terraform_service_account}') FILTER USING (TRUE)"
    use_legacy_sql = false
  }
}

resource "google_bigquery_job" "row_policy_mc_details_edp" {
  for_each   = var.data_provider_resource_ids
  job_id     = "row_policy_mc_${each.key}_${formatdate("YYYYMMDDhhmmss", timestamp())}"
  project    = data.google_client_config.default.project
  location   = data.google_client_config.default.region
  depends_on = [google_bigquery_table.mc_details_edp, google_bigquery_dataset_iam_member.terraform_data_owner]

  query {
    query          = "CREATE OR REPLACE ROW ACCESS POLICY ${each.key}_filter ON `${data.google_client_config.default.project}.${google_bigquery_dataset.dashboard.dataset_id}.mc_details_edp` GRANT TO ('serviceAccount:${google_service_account.edp_dashboard[each.key].email}') FILTER USING (CmmsDataProvider = '${each.value}')"
    use_legacy_sql = false
  }
}

resource "google_bigquery_job" "row_policy_report_detail_edp" {
  for_each   = var.data_provider_resource_ids
  job_id     = "row_policy_rd_${each.key}_${formatdate("YYYYMMDDhhmmss", timestamp())}"
  project    = data.google_client_config.default.project
  location   = data.google_client_config.default.region
  depends_on = [google_bigquery_table.report_detail_edp, google_bigquery_dataset_iam_member.terraform_data_owner]

  query {
    query          = "CREATE OR REPLACE ROW ACCESS POLICY ${each.key}_filter ON `${data.google_client_config.default.project}.${google_bigquery_dataset.dashboard.dataset_id}.report_detail_edp` GRANT TO ('serviceAccount:${google_service_account.edp_dashboard[each.key].email}') FILTER USING (CmmsDataProvider = '${each.value}')"
    use_legacy_sql = false
  }
}

# Terraform SA needs dataOwner on the dashboard dataset to create row access policies
resource "google_bigquery_dataset_iam_member" "terraform_data_owner" {
  dataset_id = google_bigquery_dataset.dashboard.dataset_id
  project    = data.google_client_config.default.project
  role       = "roles/bigquery.dataOwner"
  member     = "serviceAccount:${var.terraform_service_account}"
}

# --- Table-level IAM ---
# EDP SAs get dataViewer on requisition_overview, mc_details_edp, report_detail_edp only.
# Platform-only tables (mc_details, report_detail) have no EDP SA grants — they get 403.

resource "google_bigquery_table_iam_member" "requisition_overview_viewer" {
  for_each   = var.data_provider_resource_ids
  project    = data.google_client_config.default.project
  dataset_id = google_bigquery_dataset.dashboard.dataset_id
  table_id   = "requisition_overview"
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.edp_dashboard[each.key].email}"
}

resource "google_bigquery_table_iam_member" "mc_details_edp_viewer" {
  for_each   = var.data_provider_resource_ids
  project    = data.google_client_config.default.project
  dataset_id = google_bigquery_dataset.dashboard.dataset_id
  table_id   = "mc_details_edp"
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.edp_dashboard[each.key].email}"
}

resource "google_bigquery_table_iam_member" "report_detail_edp_viewer" {
  for_each   = var.data_provider_resource_ids
  project    = data.google_client_config.default.project
  dataset_id = google_bigquery_dataset.dashboard.dataset_id
  table_id   = "report_detail_edp"
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.edp_dashboard[each.key].email}"
}

# EDP service accounts need bigquery.jobUser to run queries
resource "google_project_iam_member" "edp_bigquery_job_user" {
  for_each = var.data_provider_resource_ids
  project  = data.google_client_config.default.project
  role     = "roles/bigquery.jobUser"
  member   = "serviceAccount:${google_service_account.edp_dashboard[each.key].email}"
}

# Operators can impersonate EDP SAs for manual isolation testing
resource "google_service_account_iam_member" "edp_sa_operator_token_creator" {
  for_each           = var.data_provider_resource_ids
  service_account_id = google_service_account.edp_dashboard[each.key].name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "user:tinage@meta.com"
}

# --- GCS Bucket for UDF Libraries ---

resource "google_storage_bucket" "dashboard_udfs" {
  name     = "${data.google_client_config.default.project}-dashboard-udfs"
  project  = data.google_client_config.default.project
  location = data.google_client_config.default.region

  uniform_bucket_level_access = true
}

resource "google_storage_bucket_object" "protobuf_lib" {
  name   = "lib/protobuf.global.min.js"
  bucket = google_storage_bucket.dashboard_udfs.name
  source = "${path.module}/udf_libs/protobuf.global.min.js"
}

resource "google_storage_bucket_object" "kingdom_descriptor" {
  name   = "descriptors/kingdom_descriptor.js"
  bucket = google_storage_bucket.dashboard_udfs.name
  source = "${path.module}/udf_libs/kingdom_descriptor.js"
}

resource "google_storage_bucket_object" "reporting_descriptor" {
  name   = "descriptors/reporting_descriptor.js"
  bucket = google_storage_bucket.dashboard_udfs.name
  source = "${path.module}/udf_libs/reporting_descriptor.js"
}

# EDP service accounts need read access to UDF library files
resource "google_storage_bucket_iam_member" "edp_udf_reader" {
  for_each = var.data_provider_resource_ids
  bucket   = google_storage_bucket.dashboard_udfs.name
  role     = "roles/storage.objectViewer"
  member   = "serviceAccount:${google_service_account.edp_dashboard[each.key].email}"
}
