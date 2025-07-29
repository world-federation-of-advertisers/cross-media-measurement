# Copyright 2024 The Cross-Media Measurement Authors
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

module "access_internal" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "internal-access-server"
  iam_service_account_name        = "access-internal"
  iam_service_account_description = "Access internal API server."
}

resource "google_project_iam_member" "access_internal_metric_writer" {
  project = data.google_project.project.name
  role    = "roles/monitoring.metricWriter"
  member  = module.access_internal.iam_service_account.member
}

resource "google_spanner_database" "access" {
  instance         = var.spanner_instance.name
  name             = var.spanner_database_name
  database_dialect = "GOOGLE_STANDARD_SQL"
}

resource "google_spanner_database_iam_member" "access_internal" {
  instance = google_spanner_database.access.instance
  database = google_spanner_database.access.name
  role     = "roles/spanner.databaseUser"
  member   = module.access_internal.iam_service_account.member

  lifecycle {
    replace_triggered_by = [google_spanner_database.access.id]
  }
}

resource "google_compute_address" "api_server" {
  name    = "access-public"
  address = var.api_server_ip_address
}
