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
}

module "reporting_internal" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "internal-reporting-server"
  iam_service_account_name        = var.iam_service_account_name
  iam_service_account_description = "Reporting internal API server."
}

resource "google_project_iam_member" "reporting_internal_metric_writer" {
  project = data.google_project.project.name
  role    = "roles/monitoring.metricWriter"
  member  = module.reporting_internal.iam_service_account.member
}

resource "google_sql_user" "reporting_internal" {
  instance = var.postgres_instance.name
  name     = trimsuffix(module.reporting_internal.iam_service_account.email, ".gserviceaccount.com")
  type     = "CLOUD_IAM_SERVICE_ACCOUNT"
}

resource "google_project_iam_member" "sql_user" {
  project = data.google_project.project.name
  role    = "roles/cloudsql.instanceUser"
  member  = module.reporting_internal.iam_service_account.member
}

resource "google_project_iam_member" "sql_client" {
  project = data.google_project.project.name
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

module "access" {
  source = "../access"

  spanner_instance      = var.spanner_instance
  spanner_database_name = var.access_spanner_database_name
}

resource "google_monitoring_dashboard" "dashboards" {
  for_each = toset(var.dashboard_json_files)

  dashboard_json = file("${path.module}/${each.value}")
  project        = data.google_project.project.project_id
}

