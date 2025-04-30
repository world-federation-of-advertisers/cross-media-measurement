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

module "edp_aggregator_bucket" {
  source   = "../storage-bucket"

  name     = var.edp_aggregator_bucket_name
  location = var.edp_aggregator_bucket_location
}

module "data_watcher_function_service_accounts" {
  source    = "../gcs-bucket-cloud-function"

  cloud_function_service_account_name       = var.data_watcher_service_account_name
  cloud_function_trigger_service_account_name    = var.data_watcher_trigger_service_account_name
  trigger_bucket_name                       = module.edp_aggregator_bucket.storage_bucket.name
  terraform_service_account                 = var.terraform_service_account
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

resource "google_kms_key_ring" "edp_aggregator_key_ring" {

  name     = var.key_ring_name
  location = var.key_ring_location

  lifecycle {
    prevent_destroy = false
  }
}

resource "google_kms_crypto_key" "edp_aggregator_kek" {
  name     = var.kms_key_name
  key_ring = google_kms_key_ring.edp_aggregator_key_ring.id
  purpose  = "ENCRYPT_DECRYPT"
}

resource "google_artifact_registry_repository" "secure_computation_tee_app" {
  location      = var.artifacts_registry_repo_location
  repository_id = var.artifacts_registry_repo_name
  description   = "Secure Computation artifacts"
  format        = "DOCKER"
}

resource "google_artifact_registry_repository_iam_member" "allow_terraform_to_use_artifact_registry" {
  location   = google_artifact_registry_repository.secure_computation_tee_app.location
  repository = google_artifact_registry_repository.secure_computation_tee_app.name
  role       = "roles/artifactregistry.admin"
  member     = "serviceAccount:${var.terraform_service_account}"
}

module "tee_apps" {
  for_each = var.queue_worker_configs
  source   = "../mig"

  artifacts_registry_repo_name  = google_artifact_registry_repository.secure_computation_tee_app.repository_id
  instance_template_name        = each.value.worker.instance_template_name
  base_instance_name            = each.value.worker.base_instance_name
  managed_instance_group_name   = each.value.worker.managed_instance_group_name
  subscription_id               = module.edp_aggregator_queues[each.key].pubsub_subscription.id
  mig_service_account_name      = each.value.worker.mig_service_account_name
  single_instance_assignment    = each.value.worker.single_instance_assignment
  min_replicas                  = each.value.worker.min_replicas
  max_replicas                  = each.value.worker.max_replicas
  app_args                      = each.value.worker.app_args
  machine_type                  = each.value.worker.machine_type
  kms_key_id                    = google_kms_crypto_key.edp_aggregator_kek.id
  docker_image                  = each.value.worker.docker_image
  terraform_service_account     = var.terraform_service_account
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