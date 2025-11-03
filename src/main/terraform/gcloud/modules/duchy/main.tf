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

data "google_client_config" "default" {}
data "google_project" "project" {}

locals {
  database_name = var.database_name == null ? "${var.name}-duchy" : var.database_name

  trustee_secrets_to_access = var.trustee_config != null ? [
    {
      secret_id  = var.trustee_config.aggregator_tls_cert.secret_id
      version    = "latest"
    },
    {
      secret_id  = var.trustee_config.aggregator_tls_key.secret_id
      version    = "latest"
    },
    {
      secret_id  = var.trustee_config.aggregator_cert_collection.secret_id
      version    = "latest"
    },
    {
      secret_id  = var.trustee_config.aggregator_cs_cert.secret_id
      version    = "latest"
    },
    {
      secret_id  = var.trustee_config.aggregator_cs_private.secret_id
      version    = "latest"
    },
  ] : []

  all_secrets = var.trustee_config != null ? {
    aggregator_tls_cert        = var.trustee_config.aggregator_tls_cert
    aggregator_tls_key         = var.trustee_config.aggregator_tls_key
    aggregator_cert_collection = var.trustee_config.aggregator_cert_collection
    aggregator_cs_cert         = var.trustee_config.aggregator_cs_cert
    aggregator_cs_private      = var.trustee_config.aggregator_cs_private
  } : {}
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

resource "google_compute_subnetwork" "trustee_mill_subnetwork" {
  count = var.trustee_config != null ? 1 : 0

  name          = "${var.name}-trustee-mill-subnet"
  region        = data.google_client_config.default.region
  network       = var.trustee_mill_private_subnetwork_cidr_range
  ip_cidr_range = var.trustee_mill_subnetwork_cidr_range
  private_ip_google_access = true
}

module "trustee_mill" {
  count = var.trustee_config != null ? 1 : 0
  
  source   = "../mig"

  depends_on = [module.secrets]
  
  instance_template_name        = var.trustee_config.instance_template_name
  base_instance_name            = var.trustee_config.base_instance_name
  managed_instance_group_name   = var.trustee_config.managed_instance_group_name
  mig_service_account_name      = var.trustee_config.mig_service_account_name
  min_replicas                  = var.trustee_config.replicas
  max_replicas                  = var.trustee_config.replicas
  machine_type                  = var.trustee_config.machine_type
  docker_image                  = var.trustee_config.docker_image
  edpa_tee_signed_image_repo    = var.trustee_config.signed_image_repo
  mig_distribution_policy_zones = var.trustee_config.mig_distribution_policy_zones
  terraform_service_account     = var.trustee_config.terraform_service_account
  disk_image_family             = var.trustee_config.disk_image_family
  tee_cmd                       = var.trustee_config.app_flags
  secrets_to_access             = local.trustee_secrets_to_access
  subnetwork_name               = google_compute_subnetwork.trustee_mill_subnetwork[0].name
}

module "secrets" {
  source            = "../secret"

  for_each          = local.all_secrets
  secret_id         = each.value.secret_id
  secret_path       = each.value.secret_local_path
  is_binary_format  = each.value.is_binary_format
}

