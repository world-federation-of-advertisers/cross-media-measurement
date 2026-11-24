# Copyright 2023 The Cross-Media Measurement Authors
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

data "google_project" "project" {}

locals {
  # All privileges for a PostgreSQL database.
  #
  # See https://www.postgresql.org/docs/14/ddl-priv.html#PRIVILEGES-SUMMARY-TABLE
  all_db_privileges = ["CREATE", "TEMPORARY", "CONNECT"]
  report_trace_observability_projects = setunion(
    toset([data.google_project.project.project_id]),
    toset(var.report_trace_observability_projects),
  )
  vid_labeling_trace_observability_projects = setunion(
    toset([data.google_project.project.project_id]),
    toset(var.vid_labeling_trace_observability_projects),
  )
}

module "reporting_internal" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "internal-reporting-server"
  iam_service_account_name        = var.iam_service_account_name
  iam_service_account_description = "Reporting internal API server."
}

resource "google_project_iam_member" "reporting_internal_metric_writer" {
  project = data.google_project.project.id
  role    = "roles/monitoring.metricWriter"
  member  = module.reporting_internal.iam_service_account.member
}

resource "google_sql_user" "reporting_internal" {
  instance = var.postgres_instance.name
  name     = trimsuffix(module.reporting_internal.iam_service_account.email, ".gserviceaccount.com")
  type     = "CLOUD_IAM_SERVICE_ACCOUNT"
}

resource "google_project_iam_member" "sql_user" {
  project = data.google_project.project.id
  role    = "roles/cloudsql.instanceUser"
  member  = module.reporting_internal.iam_service_account.member
}

resource "google_project_iam_member" "sql_client" {
  project = data.google_project.project.id
  role    = "roles/cloudsql.client"
  member  = module.reporting_internal.iam_service_account.member
}

resource "google_sql_database" "db" {
  name     = var.postgres_database_name
  instance = var.postgres_instance.name
}

resource "postgresql_grant" "db" {
  role        = google_sql_user.reporting_internal.name
  database    = google_sql_database.db.name
  object_type = "database"
  privileges  = local.all_db_privileges

  lifecycle {
    replace_triggered_by = [google_sql_database.db.id]
  }
}

# Operators impersonate the Reporting internal service account to run operational
# CLIs by hand. That service account owns the Postgres tables and holds the
# Cloud SQL and Spanner access the CLIs need, so impersonating it avoids
# provisioning a second set of database credentials.
resource "google_service_account_iam_member" "reporting_internal_operator_token_creator" {
  for_each           = toset(var.reporting_operators)
  service_account_id = module.reporting_internal.iam_service_account.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = each.value
}

# Dedicated least-privilege identity for the report-trace operator CLI. Unlike
# reporting_internal, this identity cannot mutate Reporting storage.
resource "google_service_account" "report_trace_operator" {
  account_id   = "report-trace-operator"
  display_name = "Report trace operator"
  description  = "Read-only identity for the report-trace operator CLI."
}

resource "google_service_account_iam_member" "report_trace_operator_token_creator" {
  for_each           = toset(var.report_trace_operators)
  service_account_id = google_service_account.report_trace_operator.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = each.value
}

resource "google_sql_user" "report_trace_operator" {
  instance = var.postgres_instance.name
  name     = trimsuffix(google_service_account.report_trace_operator.email, ".gserviceaccount.com")
  type     = "CLOUD_IAM_SERVICE_ACCOUNT"
}

resource "google_project_iam_member" "report_trace_operator_sql_user" {
  project = data.google_project.project.id
  role    = "roles/cloudsql.instanceUser"
  member  = google_service_account.report_trace_operator.member
}

resource "google_project_iam_member" "report_trace_operator_sql_client" {
  project = data.google_project.project.id
  role    = "roles/cloudsql.client"
  member  = google_service_account.report_trace_operator.member
}

resource "postgresql_grant" "report_trace_operator_db" {
  role        = google_sql_user.report_trace_operator.name
  database    = google_sql_database.db.name
  object_type = "database"
  privileges  = ["CONNECT"]

  lifecycle {
    replace_triggered_by = [google_sql_database.db.id]
  }
}

resource "postgresql_grant" "report_trace_operator_schema" {
  role        = google_sql_user.report_trace_operator.name
  database    = google_sql_database.db.name
  schema      = "public"
  object_type = "schema"
  privileges  = ["USAGE"]
}

resource "postgresql_grant" "report_trace_operator_tables" {
  role        = google_sql_user.report_trace_operator.name
  database    = google_sql_database.db.name
  schema      = "public"
  object_type = "table"
  objects     = []
  privileges  = ["SELECT"]
}

resource "postgresql_default_privileges" "report_trace_operator_tables" {
  database    = google_sql_database.db.name
  owner       = google_sql_user.reporting_internal.name
  role        = google_sql_user.report_trace_operator.name
  schema      = "public"
  object_type = "table"
  privileges  = ["SELECT"]
}

resource "google_project_iam_member" "report_trace_operator_logging_viewer" {
  for_each = local.report_trace_observability_projects
  project  = each.value
  role     = "roles/logging.viewer"
  member   = google_service_account.report_trace_operator.member
}

resource "google_project_iam_member" "report_trace_operator_trace_viewer" {
  for_each = local.report_trace_observability_projects
  project  = each.value
  role     = "roles/cloudtrace.viewer"
  member   = google_service_account.report_trace_operator.member
}

resource "google_project_iam_member" "report_trace_operator_service_usage_consumer" {
  for_each = local.report_trace_observability_projects
  project  = each.value
  role     = "roles/serviceusage.serviceUsageConsumer"
  member   = google_service_account.report_trace_operator.member
}

# Dedicated least-privilege identity for the VID-labeling trace CLI. Sibling modules grant it
# read-only access to authoritative pipeline state and object metadata.
resource "google_service_account" "vid_labeling_trace_operator" {
  account_id   = "vid-labeling-trace-operator"
  display_name = "VID labeling trace operator"
  description  = "Read-only identity for the vid-labeling-trace operator CLI."
}

resource "google_service_account_iam_member" "vid_labeling_trace_operator_token_creator" {
  for_each           = toset(var.vid_labeling_trace_operators)
  service_account_id = google_service_account.vid_labeling_trace_operator.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = each.value
}

resource "google_project_iam_member" "vid_labeling_trace_operator_logging_viewer" {
  for_each = local.vid_labeling_trace_observability_projects
  project  = each.value
  role     = "roles/logging.viewer"
  member   = google_service_account.vid_labeling_trace_operator.member
}

resource "google_project_iam_member" "vid_labeling_trace_operator_trace_viewer" {
  for_each = local.vid_labeling_trace_observability_projects
  project  = each.value
  role     = "roles/cloudtrace.viewer"
  member   = google_service_account.vid_labeling_trace_operator.member
}

resource "google_project_iam_member" "vid_labeling_trace_operator_service_usage_consumer" {
  for_each = local.vid_labeling_trace_observability_projects
  project  = each.value
  role     = "roles/serviceusage.serviceUsageConsumer"
  member   = google_service_account.vid_labeling_trace_operator.member
}

resource "google_spanner_database" "reporting" {
  instance         = var.spanner_instance.name
  name             = var.reporting_spanner_database_name
  database_dialect = "GOOGLE_STANDARD_SQL"
}

resource "google_spanner_database_iam_member" "reporting_internal" {
  instance = google_spanner_database.reporting.instance
  database = google_spanner_database.reporting.name
  role     = "roles/spanner.databaseUser"
  member   = module.reporting_internal.iam_service_account.member

  lifecycle {
    replace_triggered_by = [google_spanner_database.reporting.id]
  }
}

resource "google_spanner_database_iam_member" "report_trace_operator" {
  instance = google_spanner_database.reporting.instance
  database = google_spanner_database.reporting.name
  role     = "roles/spanner.databaseReader"
  member   = google_service_account.report_trace_operator.member

  lifecycle {
    replace_triggered_by = [google_spanner_database.reporting.id]
  }
}

module "access" {
  source = "../access"

  spanner_instance      = var.spanner_instance
  spanner_database_name = var.access_spanner_database_name
}

resource "google_monitoring_dashboard" "dashboards" {
  for_each = toset(var.dashboard_json_files)

  dashboard_json = file("${path.module}/${each.value}")
}
