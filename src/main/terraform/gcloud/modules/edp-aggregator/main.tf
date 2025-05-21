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

locals {
  secret_access_map = merge([
    for fn, cfg in var.secret_accessor_configs : {
      for secret in cfg.secrets_to_access :
      "${fn}:${secret.secret_key}" => {
        function_name = fn
        secret_key    = secret.secret_key
      }
    }
  ]...)
  service_accounts = {
    "data_watcher"        = module.data_watcher_function_service_accounts.cloud_function_service_account_email
    "requisition_fetcher" = module.requisition_fetcher_function_service_account.cloud_function_service_account_email
    "event_group_sync"    = module.event_group_sync_function_service_account.cloud_function_service_account_email
  }
}

module "edp_aggregator_bucket" {
  source   = "../storage-bucket"

  name     = var.edp_aggregator_bucket_name
  location = var.edp_aggregator_bucket_location
}

module "secrets" {
  source            = "../secret"
  for_each          = var.secrets
  secret_id         = each.value.secret_id
  secret_path       = each.value.secret_local_path
  is_binary_format  = each.value.is_binary_format
}

module "data_watcher_function_service_accounts" {
  source    = "../gcs-bucket-cloud-function"

  cloud_function_service_account_name       = var.data_watcher_service_account_name
  cloud_function_trigger_service_account_name    = var.data_watcher_trigger_service_account_name
  trigger_bucket_name                       = module.edp_aggregator_bucket.storage_bucket.name
  terraform_service_account                 = var.terraform_service_account
}

module "requisition_fetcher_function_service_account" {
  source    = "../http-cloud-function"

  http_cloud_function_service_account_name  = var.requisition_fetcher_service_account_name
  terraform_service_account                 = var.terraform_service_account
}

module "event_group_sync_function_service_account" {
  source    = "../http-cloud-function"

  http_cloud_function_service_account_name  = var.event_group_sync_service_account_name
  terraform_service_account                 = var.terraform_service_account
}

resource "google_secret_manager_secret_iam_member" "secret_accessor" {
  for_each = local.secret_access_map

  secret_id = var.secrets[each.value.secret_key].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${local.service_accounts[each.value.function_name]}"

}

module "edp_aggregator_queues" {
  for_each = var.queue_worker_configs
  source   = "../pubsub"

  topic_name              = each.value.queue.topic_name
  subscription_name       = each.value.queue.subscription_name
  ack_deadline_seconds    = each.value.queue.ack_deadline_seconds
}

resource "google_pubsub_topic_iam_member" "publisher" {
  for_each = var.queue_worker_configs

  topic  = module.edp_aggregator_queues[each.key].pubsub_topic.id
  role   = "roles/pubsub.publisher"
  member = var.pubsub_iam_service_account_member
}

data "google_kms_key_ring" "existing" {
  name     = var.key_ring_name
  location = var.key_ring_location
}

resource "google_kms_key_ring" "edp_aggregator_key_ring" {
  count    = length(data.google_kms_key_ring.existing) == 0 ? 1 : 0
  name     = var.key_ring_name
  location = var.key_ring_location
}

resource "google_kms_key_ring" "this" {
  count    = length(data.google_kms_key_ring.existing) == 0 ? 1 : 0
  name     = var.key_ring_name
  location = var.key_ring_location
}

locals {
  key_ring_id = length(data.google_kms_key_ring.existing) == 1
    ? data.google_kms_key_ring.existing.id
    : google_kms_key_ring.this[0].id
}

resource "google_kms_crypto_key" "edp_aggregator_kek" {
  name     = var.kms_key_name
#   key_ring = google_kms_key_ring.edp_aggregator_key_ring.id
  key_ring = local.key_ring_id
  purpose  = "ENCRYPT_DECRYPT"
}

module "tee_apps" {
  for_each = var.queue_worker_configs
  source   = "../mig"

  instance_template_name        = each.value.worker.instance_template_name
  base_instance_name            = each.value.worker.base_instance_name
  managed_instance_group_name   = each.value.worker.managed_instance_group_name
  subscription_id               = module.edp_aggregator_queues[each.key].pubsub_subscription.name
  mig_service_account_name      = each.value.worker.mig_service_account_name
  single_instance_assignment    = each.value.worker.single_instance_assignment
  min_replicas                  = each.value.worker.min_replicas
  max_replicas                  = each.value.worker.max_replicas
  app_args                      = each.value.worker.app_args
  machine_type                  = each.value.worker.machine_type
  kms_key_id                    = google_kms_crypto_key.edp_aggregator_kek.id
  docker_image                  = each.value.worker.docker_image
  terraform_service_account     = var.terraform_service_account
  secrets_to_mount              = each.value.worker.secrets_to_mount
  secrets                       = var.secrets
}

resource "google_storage_bucket_iam_member" "mig_storage_viewer" {
  for_each = module.tee_apps

  bucket = module.edp_aggregator_bucket.storage_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${each.value.mig_service_account.email}"
}

resource "google_storage_bucket_iam_member" "mig_storage_creator" {
  for_each = module.tee_apps

  bucket = module.edp_aggregator_bucket.storage_bucket.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${each.value.mig_service_account.email}"
}

resource "google_storage_bucket_iam_binding" "aggregator_storage_viewers" {
  bucket = module.edp_aggregator_bucket.storage_bucket.name
  role   = "roles/storage.objectAdmin"
  members = [
    "serviceAccount:${module.requisition_fetcher_function_service_account.cloud_function_service_account_email}",
    "serviceAccount:${module.event_group_sync_function_service_account.cloud_function_service_account_email}",
  ]
}

resource "google_cloud_run_service_iam_member" "event_group_sync_invoker" {
  service  = var.event_group_sync_function_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${module.data_watcher_function_service_accounts.cloud_function_service_account_email}"
}