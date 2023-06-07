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
  iam_service_account_name        = "reporting-internal"
  iam_service_account_description = "Reporting internal API server."
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
  name     = "reporting"
  instance = var.postgres_instance.name
}

resource "postgresql_grant" "db" {
  role        = google_sql_user.reporting_internal.name
  database    = google_sql_database.db.name
  object_type = "database"
  privileges  = local.all_db_privileges
}
