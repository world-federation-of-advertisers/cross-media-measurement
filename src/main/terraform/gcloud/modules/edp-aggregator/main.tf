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

module "edp_aggregator_queues" {
  for_each = var.queue_configs
  source   = "../pubsub"

  topic_name              = each.value.topic_name
  subscription_name       = each.value.subscription_name
  ack_deadline_seconds    = each.value.ack_deadline_seconds
}

resource "google_pubsub_topic_iam_member" "publisher" {
  for_each = var.queue_configs

  topic  = module.edp_aggregator_queues[each.key].topic.id
  role   = "roles/pubsub.publisher"
  member = var.pubsub_iam_service_account_member
}

resource "google_kms_key_ring" "edp_aggregator_key_ring" {
  name     = var.key_ring_name
  location = var.key_ring_location

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_kms_crypto_key" "edp_aggregator_kek" {
  name     = var.kms_key_name
  key_ring = google_kms_key_ring.edp_aggregator_key_ring.id
  purpose  = "ENCRYPT_DECRYPT"
}

module "tee_apps" {
  for_each = var.queue_configs
  source   = "../mig"

  artifacts_registry_repo_name  = var.artifacts_registry_repo_name
  instance_template_name        = each.value.instance_template_name
  base_instance_name            = each.value.base_instance_name
  managed_instance_group_name   = each.value.managed_instance_group_name
  subscription_id               = module.edp_aggregator_queues[each.key].subscription.id
  mig_service_account_name      = each.value.mig_service_account_name
  single_instance_assignment    = each.value.single_instance_assignment
  min_replicas                  = each.value.min_replicas
  max_replicas                  = each.value.max_replicas
  app_args                      = each.value.app_args
  machine_type                  = each.value.machine_type
  kms_key_id                    = google_kms_crypto_key.edp_aggregator_kek.id
  docker_image                  = each.value.docker_image
}

resource "google_storage_bucket_iam_member" "mig_storage_viewer" {
  for_each = module.tee_apps

  bucket = module.secure_computation_bucket.storage_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${each.value.mig_service_account.email}"
}

resource "google_storage_bucket_iam_member" "mig_storage_creator" {
  for_each = module.tee_apps

  bucket = module.secure_computation_bucket.storage_bucket.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${each.value.mig_service_account.email}"
}