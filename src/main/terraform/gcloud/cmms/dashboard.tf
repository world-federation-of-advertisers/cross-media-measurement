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

# --- BigQuery Connections (Spanner with Data Boost) ---

resource "google_bigquery_connection" "edp_aggregator" {
  connection_id = "edp-aggregator-conn"
  project       = data.google_client_config.default.project
  location      = data.google_client_config.default.region

  cloud_spanner {
    database        = "projects/${var.edp_aggregator_spanner_project}/instances/${var.edp_aggregator_spanner_instance}/databases/edp-aggregator"
    use_data_boost  = true
    use_parallelism = true
  }

  depends_on = [terraform_data.bigqueryconnection_service_identity]
}

resource "google_bigquery_connection" "kingdom" {
  connection_id = "kingdom-conn"
  project       = data.google_client_config.default.project
  location      = data.google_client_config.default.region

  cloud_spanner {
    database        = "projects/${var.kingdom_spanner_project}/instances/${var.kingdom_spanner_instance}/databases/kingdom"
    use_data_boost  = true
    use_parallelism = true
  }

  depends_on = [terraform_data.bigqueryconnection_service_identity]
}

resource "google_bigquery_connection" "reporting" {
  connection_id = "reporting-conn"
  project       = data.google_client_config.default.project
  location      = data.google_client_config.default.region

  cloud_spanner {
    database        = "projects/${var.reporting_spanner_project}/instances/${var.reporting_spanner_instance}/databases/reporting"
    use_data_boost  = true
    use_parallelism = true
  }

  depends_on = [terraform_data.bigqueryconnection_service_identity]
}

# Cloud SQL (Postgres) connection to the reporting database. Used by
# report_detail to resolve report -> campaign group -> event group
# associations, which live in Postgres (ReportingSets / EventGroups), not
# Spanner.
resource "google_bigquery_connection" "reporting_postgres" {
  connection_id = "reporting-postgres-conn"
  project       = data.google_client_config.default.project
  location      = data.google_client_config.default.region

  cloud_sql {
    # Reuses the reporting v2 Postgres instance (var.postgres_instance_name) and
    # password (var.postgres_password), both supplied via GitHub vars/secrets in
    # the same way as the Spanner connections. The "reporting-v2" database name
    # is a literal, matching how the Spanner db names are written above.
    #
    # Cloud SQL connections in the BigQuery Connection API only support
    # username/password auth (see the terraform-provider-google schema:
    # `credential` is Required; `service_account_id` is Computed/output-only
    # and cannot be set). We authenticate as `postgres`, the Cloud SQL
    # built-in admin — but in Cloud SQL for Postgres, `postgres` is member of
    # `cloudsqlsuperuser`, NOT true superuser, and does not automatically get
    # SELECT on tables created by other roles (Liquibase runs as
    # reporting-v2-internal@…iam, which owns the reporting tables). The
    # postgresql_grant resources below give `postgres` explicit read access.
    instance_id = "${data.google_client_config.default.project}:${data.google_client_config.default.region}:${var.postgres_instance_name}"
    database    = "reporting-v2"
    type        = "POSTGRES"
    credential {
      username = "postgres"
      password = var.postgres_password
    }
  }

  depends_on = [terraform_data.bigqueryconnection_service_identity]
}

# Grant SELECT on all existing reporting tables in the public schema to the
# `postgres` user that the reporting-postgres-conn BigQuery Connection
# authenticates as. Without this the report_detail_edp scheduled query fails
# with "ERROR: permission denied for table reportingsets".
resource "postgresql_grant" "reporting_postgres_conn_select_tables" {
  database    = "reporting-v2"
  role        = "postgres"
  schema      = "public"
  object_type = "table"
  objects     = [] # empty = all tables in the schema
  privileges  = ["SELECT"]
}

# USAGE on the schema is required to reference tables inside it, even when the
# grantee already holds SELECT on the tables themselves.
resource "postgresql_grant" "reporting_postgres_conn_usage_schema" {
  database    = "reporting-v2"
  role        = "postgres"
  schema      = "public"
  object_type = "schema"
  privileges  = ["USAGE"]
}

data "google_project" "project" {
  project_id = data.google_client_config.default.project
}

# --- Tables (created empty, populated by scheduled queries via atomic MERGE) ---

resource "google_bigquery_table" "requisition_overview" {
  dataset_id          = google_bigquery_dataset.dashboard.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "requisition_overview"
  deletion_protection = var.dashboard_deletion_protection

  schema = <<EOF
[
  {
    "name": "DataProviderResourceId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "Report",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "CmmsMeasurementConsumer",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "RequisitionState",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "RefusalMessage",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "CmmsCreateTime",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  },
  {
    "name": "FulfilledTime",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  },
  {
    "name": "FulfillmentDurationSeconds",
    "type": "INT64",
    "mode": "NULLABLE"
  },
  {
    "name": "ReportState",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "ReportStartDate",
    "type": "DATE",
    "mode": "NULLABLE"
  },
  {
    "name": "ReportEndDate",
    "type": "DATE",
    "mode": "NULLABLE"
  },
  {
    "name": "ImpressionQualificationFilters",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "ReportTitle",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "ResultGroupTitles",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "ResultGroupMetricFrequencies",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]
EOF
}

resource "google_bigquery_table" "mc_details" {
  dataset_id          = google_bigquery_dataset.dashboard.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "mc_details"
  deletion_protection = var.dashboard_deletion_protection

  schema = <<EOF
[
  {
    "name": "CmmsMeasurementConsumer",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "CmmsDataProvider",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "EventGroupCount",
    "type": "INT64",
    "mode": "NULLABLE"
  },
  {
    "name": "ProvidedEventGroupIds",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "CampaignNames",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "BrandNames",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "EventTemplates",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "MediaTypes",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "AccountIds",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "DataAvailabilityStartTime",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  },
  {
    "name": "DataAvailabilityEndTime",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  },
  {
    "name": "TotalMcs",
    "type": "INT64",
    "mode": "NULLABLE"
  },
  {
    "name": "CoveragePercent",
    "type": "FLOAT64",
    "mode": "NULLABLE"
  }
]
EOF
}

resource "google_bigquery_table" "mc_details_edp" {
  dataset_id          = google_bigquery_dataset.dashboard.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "mc_details_edp"
  deletion_protection = var.dashboard_deletion_protection

  schema = <<EOF
[
  {
    "name": "CmmsMeasurementConsumer",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "CmmsDataProvider",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "EventGroupCount",
    "type": "INT64",
    "mode": "NULLABLE"
  },
  {
    "name": "ProvidedEventGroupIds",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "EntityTypes",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "EntityIds",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "CampaignNames",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "BrandNames",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "EventTemplates",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "EntityMetadata",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "MediaTypes",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "AccountIds",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "DataAvailabilityStartTime",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  },
  {
    "name": "DataAvailabilityEndTime",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  }
]
EOF
}

resource "google_bigquery_table" "report_detail" {
  dataset_id          = google_bigquery_dataset.dashboard.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "report_detail"
  deletion_protection = var.dashboard_deletion_protection

  schema = <<EOF
[
  {
    "name": "ExternalReportId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "CmmsDataProvider",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "EventGroupCount",
    "type": "INT64",
    "mode": "NULLABLE"
  },
  {
    "name": "CmmsEventGroupIds",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "CampaignNames",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "BrandNames",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "EdpCount",
    "type": "INT64",
    "mode": "NULLABLE"
  }
]
EOF
}

resource "google_bigquery_table" "report_detail_edp" {
  dataset_id          = google_bigquery_dataset.dashboard.dataset_id
  project             = data.google_client_config.default.project
  table_id            = "report_detail_edp"
  deletion_protection = var.dashboard_deletion_protection

  schema = <<EOF
[
  {
    "name": "ExternalReportId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "CmmsDataProvider",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "EventGroupCount",
    "type": "INT64",
    "mode": "NULLABLE"
  },
  {
    "name": "CmmsEventGroupIds",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "CampaignNames",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "BrandNames",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "EntityTypes",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "EntityIds",
    "type": "STRING",
    "mode": "REPEATED"
  }
]
EOF
}

# Terraform SA needs serviceAccountUser on itself to create scheduled queries
resource "google_service_account_iam_member" "terraform_sa_act_as_self" {
  service_account_id = "projects/${data.google_client_config.default.project}/serviceAccounts/${var.terraform_service_account}"
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}

# Terraform SA needs roleAdmin to manage the dashboardComplianceChecker custom
# role. Mirrors terraform_sa_act_as_self above. Without this, terraform fails
# on a fresh project with: "Unable to verify whether custom project role ...
# already exists and must be undeleted" (the get-before-create call returns
# 403 to the terraform SA).
#
# TODO(#4135): scope this to a purpose-built terraformDashboardRoleAdmin custom
# role with iam.roles.{get,list,create,update,undelete,delete} and an IAM
# condition restricting the resource to
# projects/<project>/roles/dashboardComplianceChecker*. roles/iam.roleAdmin
# project-wide is over-broad: it lets the terraform SA create/modify any custom
# role in the project (including one that grants itself setIamPolicy).
resource "google_project_iam_member" "terraform_role_admin" {
  project = data.google_client_config.default.project
  role    = "roles/iam.roleAdmin"
  member  = "serviceAccount:${var.terraform_service_account}"
}


# --- Scheduled Queries (materialize Spanner data into BigQuery tables) ---

resource "google_bigquery_data_transfer_config" "requisition_overview" {
  depends_on           = [google_service_account_iam_member.terraform_sa_act_as_self]
  display_name         = "Dashboard: requisition_overview"
  data_source_id       = "scheduled_query"
  schedule             = "every 1 hours"
  project              = data.google_client_config.default.project
  location             = data.google_client_config.default.region
  service_account_name = var.terraform_service_account

  params = {
    query = templatefile("${path.module}/sql/requisition_overview.sql", {
      project_id = data.google_client_config.default.project
      region     = data.google_client_config.default.region
      dataset    = google_bigquery_dataset.dashboard.dataset_id
      table_name = "requisition_overview"
    })
  }
}

resource "google_bigquery_data_transfer_config" "mc_details" {
  depends_on           = [google_service_account_iam_member.terraform_sa_act_as_self]
  display_name         = "Dashboard: mc_details (platform)"
  data_source_id       = "scheduled_query"
  service_account_name = var.terraform_service_account
  schedule             = "every 1 hours"
  project              = data.google_client_config.default.project
  location             = data.google_client_config.default.region

  params = {
    query = templatefile("${path.module}/sql/mc_details.sql", {
      project_id               = data.google_client_config.default.project
      region                   = data.google_client_config.default.region
      dataset                  = google_bigquery_dataset.dashboard.dataset_id
      table_name               = "mc_details"
      include_platform_columns = true
    })
  }
}

resource "google_bigquery_data_transfer_config" "mc_details_edp" {
  depends_on           = [google_service_account_iam_member.terraform_sa_act_as_self]
  display_name         = "Dashboard: mc_details_edp"
  data_source_id       = "scheduled_query"
  service_account_name = var.terraform_service_account
  schedule             = "every 1 hours"
  project              = data.google_client_config.default.project
  location             = data.google_client_config.default.region

  params = {
    query = templatefile("${path.module}/sql/mc_details.sql", {
      project_id               = data.google_client_config.default.project
      region                   = data.google_client_config.default.region
      dataset                  = google_bigquery_dataset.dashboard.dataset_id
      table_name               = "mc_details_edp"
      include_platform_columns = false
    })
  }
}

resource "google_bigquery_data_transfer_config" "report_detail" {
  depends_on           = [google_service_account_iam_member.terraform_sa_act_as_self]
  display_name         = "Dashboard: report_detail (platform)"
  data_source_id       = "scheduled_query"
  service_account_name = var.terraform_service_account
  schedule             = "every 1 hours"
  project              = data.google_client_config.default.project
  location             = data.google_client_config.default.region

  params = {
    query = templatefile("${path.module}/sql/report_detail.sql", {
      project_id               = data.google_client_config.default.project
      region                   = data.google_client_config.default.region
      dataset                  = google_bigquery_dataset.dashboard.dataset_id
      table_name               = "report_detail"
      include_platform_columns = true
    })
  }
}

resource "google_bigquery_data_transfer_config" "report_detail_edp" {
  depends_on           = [google_service_account_iam_member.terraform_sa_act_as_self]
  display_name         = "Dashboard: report_detail_edp"
  data_source_id       = "scheduled_query"
  service_account_name = var.terraform_service_account
  schedule             = "every 1 hours"
  project              = data.google_client_config.default.project
  location             = data.google_client_config.default.region

  params = {
    query = templatefile("${path.module}/sql/report_detail.sql", {
      project_id               = data.google_client_config.default.project
      region                   = data.google_client_config.default.region
      dataset                  = google_bigquery_dataset.dashboard.dataset_id
      table_name               = "report_detail_edp"
      include_platform_columns = false
    })
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
# Platform operators see all rows via platform_full_access policies.

resource "google_bigquery_row_access_policy" "requisition_overview_platform" {
  project          = data.google_client_config.default.project
  dataset_id       = google_bigquery_dataset.dashboard.dataset_id
  table_id         = google_bigquery_table.requisition_overview.table_id
  policy_id        = "platform_full_access"
  filter_predicate = "TRUE"
  grantees         = concat(["serviceAccount:${var.terraform_service_account}"], var.dashboard_operators)
}

resource "google_bigquery_row_access_policy" "mc_details_platform" {
  project          = data.google_client_config.default.project
  dataset_id       = google_bigquery_dataset.dashboard.dataset_id
  table_id         = google_bigquery_table.mc_details.table_id
  policy_id        = "platform_full_access"
  filter_predicate = "TRUE"
  grantees         = concat(["serviceAccount:${var.terraform_service_account}"], var.dashboard_operators)
}

resource "google_bigquery_row_access_policy" "report_detail_platform" {
  project          = data.google_client_config.default.project
  dataset_id       = google_bigquery_dataset.dashboard.dataset_id
  table_id         = google_bigquery_table.report_detail.table_id
  policy_id        = "platform_full_access"
  filter_predicate = "TRUE"
  grantees         = concat(["serviceAccount:${var.terraform_service_account}"], var.dashboard_operators)
}

resource "google_bigquery_row_access_policy" "requisition_overview" {
  for_each         = var.data_provider_resource_ids
  project          = data.google_client_config.default.project
  dataset_id       = google_bigquery_dataset.dashboard.dataset_id
  table_id         = google_bigquery_table.requisition_overview.table_id
  policy_id        = "${each.key}_filter"
  filter_predicate = "DataProviderResourceId = '${each.value}'"
  grantees         = ["serviceAccount:${google_service_account.edp_dashboard[each.key].email}"]
}

resource "google_bigquery_row_access_policy" "mc_details_edp" {
  for_each         = var.data_provider_resource_ids
  project          = data.google_client_config.default.project
  dataset_id       = google_bigquery_dataset.dashboard.dataset_id
  table_id         = google_bigquery_table.mc_details_edp.table_id
  policy_id        = "${each.key}_filter"
  filter_predicate = "CmmsDataProvider = '${each.value}'"
  grantees         = ["serviceAccount:${google_service_account.edp_dashboard[each.key].email}"]
}


resource "google_bigquery_row_access_policy" "mc_details_edp_platform" {
  project          = data.google_client_config.default.project
  dataset_id       = google_bigquery_dataset.dashboard.dataset_id
  table_id         = google_bigquery_table.mc_details_edp.table_id
  policy_id        = "platform_full_access"
  filter_predicate = "TRUE"
  grantees         = ["serviceAccount:${var.terraform_service_account}"]
}

resource "google_bigquery_row_access_policy" "report_detail_edp_platform" {
  project          = data.google_client_config.default.project
  dataset_id       = google_bigquery_dataset.dashboard.dataset_id
  table_id         = google_bigquery_table.report_detail_edp.table_id
  policy_id        = "platform_full_access"
  filter_predicate = "TRUE"
  grantees         = ["serviceAccount:${var.terraform_service_account}"]
}

resource "google_bigquery_row_access_policy" "report_detail_edp" {
  for_each         = var.data_provider_resource_ids
  project          = data.google_client_config.default.project
  dataset_id       = google_bigquery_dataset.dashboard.dataset_id
  table_id         = google_bigquery_table.report_detail_edp.table_id
  policy_id        = "${each.key}_filter"
  filter_predicate = "CmmsDataProvider = '${each.value}'"
  grantees         = ["serviceAccount:${google_service_account.edp_dashboard[each.key].email}"]
}

# Terraform SA needs dataOwner on the dashboard dataset to create row access policies
resource "google_bigquery_dataset_iam_member" "terraform_data_owner" {
  dataset_id = google_bigquery_dataset.dashboard.dataset_id
  project    = data.google_client_config.default.project
  role       = "roles/bigquery.dataOwner"
  member     = "serviceAccount:${var.terraform_service_account}"
}

# Terraform SA needs connectionUser on Spanner connections for scheduled queries
resource "google_bigquery_connection_iam_member" "terraform_edp_aggregator_conn" {
  project       = data.google_client_config.default.project
  location      = data.google_client_config.default.region
  connection_id = google_bigquery_connection.edp_aggregator.connection_id
  role          = "roles/bigquery.connectionUser"
  member        = "serviceAccount:${var.terraform_service_account}"
}

resource "google_bigquery_connection_iam_member" "terraform_kingdom_conn" {
  project       = data.google_client_config.default.project
  location      = data.google_client_config.default.region
  connection_id = google_bigquery_connection.kingdom.connection_id
  role          = "roles/bigquery.connectionUser"
  member        = "serviceAccount:${var.terraform_service_account}"
}

resource "google_bigquery_connection_iam_member" "terraform_reporting_conn" {
  project       = data.google_client_config.default.project
  location      = data.google_client_config.default.region
  connection_id = google_bigquery_connection.reporting.connection_id
  role          = "roles/bigquery.connectionUser"
  member        = "serviceAccount:${var.terraform_service_account}"
}

resource "google_bigquery_connection_iam_member" "terraform_reporting_postgres_conn" {
  project       = data.google_client_config.default.project
  location      = data.google_client_config.default.region
  connection_id = google_bigquery_connection.reporting_postgres.connection_id
  role          = "roles/bigquery.connectionUser"
  member        = "serviceAccount:${var.terraform_service_account}"
}

# Force-provision the BigQuery Connection service agent. This service-managed
# service account ("service-${project_number}@gcp-sa-bigqueryconnection.iam.gserviceaccount.com")
# is created on-demand by GCP the first time a project uses the BigQuery
# Connection API. Without this resource, the IAM bindings below fail on a
# freshly-bootstrapped project with: "Service account
# service-${project_number}@gcp-sa-bigqueryconnection.iam.gserviceaccount.com
# does not exist."
resource "terraform_data" "bigqueryconnection_service_identity" {
  triggers_replace = data.google_client_config.default.project
  provisioner "local-exec" {
    command = <<-EOT
      gcloud beta services identity create \
        --service=bigqueryconnection.googleapis.com \
        --project=${data.google_client_config.default.project}
    EOT
  }
}

# The BigQuery Connection service agent needs cloudsql.client to read the
# reporting Postgres via the Cloud SQL connection.
resource "google_project_iam_member" "reporting_postgres_conn_client" {
  project    = data.google_client_config.default.project
  role       = "roles/cloudsql.client"
  member     = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-bigqueryconnection.iam.gserviceaccount.com"
  depends_on = [terraform_data.bigqueryconnection_service_identity]
}



# BigQuery Connection service agent needs databaseReaderWithDataBoost on
# target Spanner databases for EXTERNAL_QUERY via Cloud Spanner connections.
resource "google_spanner_database_iam_member" "edp_aggregator_conn_reader" {
  project    = var.edp_aggregator_spanner_project
  instance   = var.edp_aggregator_spanner_instance
  database   = "edp-aggregator"
  role       = "roles/spanner.databaseReaderWithDataBoost"
  member     = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-bigqueryconnection.iam.gserviceaccount.com"
  depends_on = [terraform_data.bigqueryconnection_service_identity]
}

resource "google_spanner_database_iam_member" "kingdom_conn_reader" {
  project    = var.kingdom_spanner_project
  instance   = var.kingdom_spanner_instance
  database   = "kingdom"
  role       = "roles/spanner.databaseReaderWithDataBoost"
  member     = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-bigqueryconnection.iam.gserviceaccount.com"
  depends_on = [terraform_data.bigqueryconnection_service_identity]
}

resource "google_spanner_database_iam_member" "reporting_conn_reader" {
  project    = var.reporting_spanner_project
  instance   = var.reporting_spanner_instance
  database   = "reporting"
  role       = "roles/spanner.databaseReaderWithDataBoost"
  member     = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-bigqueryconnection.iam.gserviceaccount.com"
  depends_on = [terraform_data.bigqueryconnection_service_identity]
}

# Terraform SA needs dataEditor on dashboard dataset for scheduled query writes
resource "google_bigquery_dataset_iam_member" "terraform_data_editor" {
  dataset_id = google_bigquery_dataset.dashboard.dataset_id
  project    = data.google_client_config.default.project
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.terraform_service_account}"
}

# --- Table-level IAM ---

# Operators get dataViewer on platform-only tables (mc_details, report_detail)
# and the shared table (requisition_overview).
resource "google_bigquery_table_iam_member" "requisition_overview_platform_viewer" {
  for_each   = toset(var.dashboard_operators)
  project    = data.google_client_config.default.project
  dataset_id = google_bigquery_dataset.dashboard.dataset_id
  table_id   = google_bigquery_table.requisition_overview.table_id
  role       = "roles/bigquery.dataViewer"
  member     = each.value
}

resource "google_bigquery_table_iam_member" "mc_details_platform_viewer" {
  for_each   = toset(var.dashboard_operators)
  project    = data.google_client_config.default.project
  dataset_id = google_bigquery_dataset.dashboard.dataset_id
  table_id   = google_bigquery_table.mc_details.table_id
  role       = "roles/bigquery.dataViewer"
  member     = each.value
}

resource "google_bigquery_table_iam_member" "report_detail_platform_viewer" {
  for_each   = toset(var.dashboard_operators)
  project    = data.google_client_config.default.project
  dataset_id = google_bigquery_dataset.dashboard.dataset_id
  table_id   = google_bigquery_table.report_detail.table_id
  role       = "roles/bigquery.dataViewer"
  member     = each.value
}

# EDP SAs get dataViewer on shared tables only (requisition_overview, mc_details_edp,
# report_detail_edp). EDP SAs have no access to platform-only tables (mc_details,
# report_detail) — they get 403.

resource "google_bigquery_table_iam_member" "requisition_overview_viewer" {
  for_each   = var.data_provider_resource_ids
  project    = data.google_client_config.default.project
  dataset_id = google_bigquery_dataset.dashboard.dataset_id
  table_id   = google_bigquery_table.requisition_overview.table_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.edp_dashboard[each.key].email}"
}

resource "google_bigquery_table_iam_member" "mc_details_edp_viewer" {
  for_each   = var.data_provider_resource_ids
  project    = data.google_client_config.default.project
  dataset_id = google_bigquery_dataset.dashboard.dataset_id
  table_id   = google_bigquery_table.mc_details_edp.table_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.edp_dashboard[each.key].email}"
}

resource "google_bigquery_table_iam_member" "report_detail_edp_viewer" {
  for_each   = var.data_provider_resource_ids
  project    = data.google_client_config.default.project
  dataset_id = google_bigquery_dataset.dashboard.dataset_id
  table_id   = google_bigquery_table.report_detail_edp.table_id
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
  for_each = {
    for pair in setproduct(keys(var.data_provider_resource_ids), var.dashboard_operators) :
    "${pair[0]}-${pair[1]}" => { edp = pair[0], operator = pair[1] }
  }
  service_account_id = google_service_account.edp_dashboard[each.value.edp].name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = each.value.operator
}

# --- Read-only service account for the dashboard compliance check ---
# Runs the compliance check from one fixed, least-privilege identity instead of
# whatever human/ADC happens to invoke it. Operators impersonate this SA, which
# in turn impersonates the per-EDP SAs for the isolation checks.
resource "google_service_account" "dashboard_compliance" {
  account_id   = "dashboard-compliance"
  display_name = "Dashboard Compliance Check"
  project      = data.google_client_config.default.project
}

resource "google_project_iam_custom_role" "dashboard_compliance" {
  role_id     = "dashboardComplianceChecker"
  project     = data.google_client_config.default.project
  title       = "Dashboard Compliance Checker"
  description = "Read-only access to verify dashboard EDP isolation."
  permissions = [
    "bigquery.jobs.create",
    "bigquery.datasets.get",
    "bigquery.tables.get",
    "bigquery.tables.list",
    "bigquery.routines.get",
    "bigquery.routines.list",
    "bigquery.rowAccessPolicies.list",
    "bigquery.rowAccessPolicies.getIamPolicy",
    "bigquery.connections.get",
    "bigquery.connections.list",
  ]
  depends_on = [google_project_iam_member.terraform_role_admin]
}

resource "google_project_iam_member" "dashboard_compliance_role" {
  project = data.google_client_config.default.project
  role    = google_project_iam_custom_role.dashboard_compliance.id
  member  = "serviceAccount:${google_service_account.dashboard_compliance.email}"
}

# Compliance SA impersonates each EDP SA to run the per-EDP isolation checks.
resource "google_service_account_iam_member" "dashboard_compliance_impersonate_edp" {
  for_each           = var.data_provider_resource_ids
  service_account_id = google_service_account.edp_dashboard[each.key].name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${google_service_account.dashboard_compliance.email}"
}


# Operators impersonate the compliance SA to run the check by hand.
resource "google_service_account_iam_member" "dashboard_compliance_operator_token_creator" {
  for_each           = toset(var.dashboard_operators)
  service_account_id = google_service_account.dashboard_compliance.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = each.value
}

# CI (running as the terraform SA) impersonates the compliance SA to run the
# DashboardComplianceCheck as the dedicated least-privilege identity, not as
# the terraform SA (which has admin-level access to everything the check
# inspects and would silently hide missing permissions on the custom role).
resource "google_service_account_iam_member" "dashboard_compliance_terraform_token_creator" {
  service_account_id = google_service_account.dashboard_compliance.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${var.terraform_service_account}"
}


# --- Scheduled compliance check (#3930) --------------------------------------
# Packages DashboardComplianceCheck as an HTTP Cloud Function and runs it daily
# via Cloud Scheduler (OIDC), independently of the post-deploy CI check, to
# detect drift against the live state between deploys. Failed checks are logged
# at SEVERE with an "ALERT:" prefix and surfaced by the log-based alert policy.
#
# Gated on dashboard_compliance_uber_jar_path: environments that do not supply
# the function uber jar (e.g. local plans) skip the scheduled check cleanly.
locals {
  deploy_dashboard_compliance_scheduler = (
    var.dashboard_compliance_uber_jar_path != null &&
    var.dashboard_compliance_uber_jar_path != "" &&
    length(var.data_provider_resource_ids) > 0
  )
  dashboard_compliance_function_name = "dashboard-compliance-check"

  # Derived from data_provider_resource_ids so it can never drift from the SAs /
  # row access policies. Semicolon-separated so it can travel in the Cloud
  # Function's comma-delimited env-var string.
  dashboard_edps_env = join(
    ";",
    [for name, resource_id in var.data_provider_resource_ids : "${name}:${resource_id}"],
  )
}

module "dashboard_compliance_cloud_function" {
  count  = local.deploy_dashboard_compliance_scheduler ? 1 : 0
  source = "../modules/http-cloud-function"

  http_cloud_function_service_account_name = "dashboard-compliance-fn"
  terraform_service_account                = var.terraform_service_account
  function_name                            = local.dashboard_compliance_function_name
  entry_point                              = "org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.tools.DashboardComplianceCheckFunction"
  uber_jar_path                            = var.dashboard_compliance_uber_jar_path
  secret_mappings                          = ""
  extra_env_vars = join(",", [
    "GOOGLE_CLOUD_PROJECT=${data.google_client_config.default.project}",
    "BIGQUERY_REGION=${data.google_client_config.default.region}",
    "DASHBOARD_DATASET=${google_bigquery_dataset.dashboard.dataset_id}",
    "IMPERSONATE_SERVICE_ACCOUNT=${google_service_account.dashboard_compliance.email}",
    "DASHBOARD_EDPS=${local.dashboard_edps_env}",
  ])
}

# The function runs as its own SA and impersonates the least-privilege
# dashboard-compliance SA (the same identity the post-deploy CI check uses).
resource "google_service_account_iam_member" "dashboard_compliance_function_token_creator" {
  count              = local.deploy_dashboard_compliance_scheduler ? 1 : 0
  service_account_id = google_service_account.dashboard_compliance.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${module.dashboard_compliance_cloud_function[0].cloud_function_service_account.email}"
}

module "dashboard_compliance_cloud_scheduler" {
  count                     = local.deploy_dashboard_compliance_scheduler ? 1 : 0
  source                    = "../modules/cloud-scheduler"
  terraform_service_account = var.terraform_service_account
  scheduler_config = {
    schedule                  = "0 6 * * *"
    time_zone                 = "UTC"
    name                      = "dashboard-compliance-scheduler"
    function_url              = "https://${data.google_client_config.default.region}-${data.google_client_config.default.project}.cloudfunctions.net/${local.dashboard_compliance_function_name}"
    scheduler_sa_display_name = "Dashboard Compliance Scheduler"
    scheduler_sa_description  = "Service account for Cloud Scheduler to trigger the dashboard compliance check"
    scheduler_job_description = "Daily DashboardComplianceCheck run to detect drift between deploys (#3930)"
    scheduler_job_name        = local.dashboard_compliance_function_name
  }
  depends_on = [module.dashboard_compliance_cloud_function]
}

# Log-based alert policy: the function logs SEVERE (Cloud Logging ERROR) on any
# failed check or execution error. Notifies the configured channels (Slack /
# PagerDuty). With no channels configured, the policy still records incidents.
resource "google_monitoring_alert_policy" "dashboard_compliance_failures" {
  count        = local.deploy_dashboard_compliance_scheduler ? 1 : 0
  display_name = "Dashboard Compliance Check Failures"
  combiner     = "OR"

  conditions {
    display_name = "DashboardComplianceCheck reported failures"
    condition_matched_log {
      filter = "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"${local.dashboard_compliance_function_name}\" AND severity>=ERROR"
    }
  }

  notification_channels = var.dashboard_alert_notification_channels

  alert_strategy {
    notification_rate_limit {
      period = "3600s"
    }
    auto_close = "1800s"
  }

  documentation {
    content = "The scheduled DashboardComplianceCheck (#3930) reported one or more failed checks or errored. Inspect the dashboard-compliance-check Cloud Function logs for entries prefixed with 'ALERT:'."
  }
}
