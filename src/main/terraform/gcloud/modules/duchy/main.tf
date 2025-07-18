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
  database_name = var.database_name == null ? "${var.name}-duchy" : var.database_name
}

module "storage_user" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "storage"
  iam_service_account_name        = "${var.name}-duchy-storage"
  iam_service_account_description = "${var.name} Duchy storage."
}

resource "google_project_iam_member" "storage_metric_writer" {
  project = data.google_project.project.name
  role    = "roles/monitoring.metricWriter"
  member  = module.storage_user.iam_service_account.member
}

module "internal_server_user" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "internal-server"
  iam_service_account_name        = "${var.name}-duchy-internal"
  iam_service_account_description = "${var.name} internal API server."
}

resource "google_project_iam_member" "internal_server_metric_writer" {
  project = data.google_project.project.name
  role    = "roles/monitoring.metricWriter"
  member  = module.internal_server_user.iam_service_account.member
}

resource "google_spanner_database" "db" {
  instance         = var.spanner_instance.name
  name             = local.database_name
  database_dialect = "GOOGLE_STANDARD_SQL"
}

resource "google_spanner_database_iam_member" "internal_server" {
  instance = google_spanner_database.db.instance
  database = google_spanner_database.db.name
  role     = "roles/spanner.databaseUser"
  member   = module.internal_server_user.iam_service_account.member

  lifecycle {
    replace_triggered_by = [google_spanner_database.db.id]
  }
}

resource "google_storage_bucket_iam_member" "internal_server" {
  bucket = var.storage_bucket.name
  role   = "roles/storage.objectAdmin"
  member = module.internal_server_user.iam_service_account.member
}

resource "google_storage_bucket_iam_member" "storage" {
  bucket = var.storage_bucket.name
  role   = "roles/storage.objectAdmin"
  member = module.storage_user.iam_service_account.member
}

resource "google_compute_address" "v2alpha" {
  name    = "${var.name}-duchy-v2alpha"
  address = var.v2alpha_ip_address
}

resource "google_compute_address" "system_v1alpha" {
  name    = "${var.name}-duchy-system-v1alpha"
  address = var.system_v1alpha_ip_address
}

resource "google_monitoring_dashboard" "dashboards" {
  for_each = toset(var.dashboard_json_files)

  dashboard_json = templatefile("${path.module}/${each.value}", {
    duchy_name = var.name
  })
}

