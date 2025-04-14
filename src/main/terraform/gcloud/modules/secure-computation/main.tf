# Copyright 2025 The Cross-Media Measurement Authors
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

module "secure_computation_internal" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "internal-secure-computation-server"
  iam_service_account_name        = var.iam_service_account_name
  iam_service_account_description = "Secure Computation internal API server."
}

resource "google_spanner_database" "secure_computation" {
  instance         = var.spanner_instance.name
  name             = var.spanner_database_name
  database_dialect = "GOOGLE_STANDARD_SQL"
}

resource "google_spanner_database_iam_member" "secure_computation_internal" {
  instance = google_spanner_database.secure_computation.instance
  database = google_spanner_database.secure_computation.name
  role     = "roles/spanner.databaseUser"
  member   = module.secure_computation_internal.iam_service_account.member

  lifecycle {
    replace_triggered_by = [google_spanner_database.secure_computation.id]
  }
}

resource "google_compute_address" "api_server" {
  name    = "secure-computation-public"
  address = var.secure_computation_api_server_ip_address
}

module "secure_computation_queues" {
  for_each = var.queue_configs
  source   = "../pubsub"

  topic_name              = each.value.topic_name
  subscription_name       = each.value.subscription_name
  ack_deadline_seconds    = each.value.ack_deadline_seconds
}

resource "google_pubsub_topic_iam_member" "publisher" {
  for_each = var.queue_configs

  topic  = module.secure_computation_queues[each.key].topic.id
  role   = "roles/pubsub.publisher"
  member = local.iam_service_account.member
}

module "secure_computation_bucket" {
  source   = "../storage-bucket"

  name     = var.secure_computation_bucket_name
  location = var.secure_computation_bucket_location
}

module "data_watcher_function_service_accounts" {
  source    = "../gcs-bucket-cloud-function"

  cloud_function_service_account_name       = var.data_watcher_service_account_name
  cloud_function_trigger_service_account    = var.data_watcher_trigger_service_account_name
  trigger_bucket_name                       = module.secure_computation_bucket.storage_bucket.name
}

resource "google_kms_key_ring" "secure_computation_key_ring" {
  name     = var.key_ring_name
  location = var.key_ring_location

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_kms_crypto_key" "secure_computation_kek" {
  name     = var.kms_key_name
  key_ring = google_kms_key_ring.secure_computation_key_ring.id
  purpose  = "ENCRYPT_DECRYPT"
}

module "secure_computation_migs" {
  for_each = var.queue_configs
  source   = "../mig"

  tee_app_artifacts_repo_name   = var.tee_app_artifacts_repo_name
  instance_template_name        = each.value.instance_template_name
  base_instance_name            = each.value.base_instance_name
  managed_instance_group_name   = each.value.managed_instance_group_name
  subscription_id               = module.secure_computation_queues[each.key].subscription.id
  mig_service_account_name      = each.value.mig_service_account_name
  single_instance_assignment    = each.value.single_instance_assignment
  min_replicas                  = each.value.min_replicas
  max_replicas                  = each.value.max_replicas
  app_args                      = each.value.app_args
  machine_type                  = each.value.machine_type
  kms_key_id                    = google_kms_crypto_key.secure_computation_kek.id
  storage_bucket_name           = module.secure_computation_bucket.storage_bucket.name
  docker_image                  = each.value.docker_image
}
