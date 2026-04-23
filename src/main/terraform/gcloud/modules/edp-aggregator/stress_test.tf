# Copyright 2026 The Cross-Media Measurement Authors
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

# Stress-test Results Fulfiller for reproducing SWIOTLB buffer exhaustion.
#
# Deploys a separate MIG with the Confidential Space debug image to test
# concurrent blob-read pressure that triggers swiotlb buffer exhaustion
# on Confidential VMs. Disabled by default; set stress_test_fulfiller_config
# in the CMMS layer to enable.

module "stress_test_fulfiller_queue" {
  count  = var.stress_test_fulfiller_config != null ? 1 : 0
  source = "../pubsub"

  topic_name           = "stress-test-fulfiller-topic"
  subscription_name    = "stress-test-fulfiller-subscription"
  ack_deadline_seconds = 600
}

resource "google_pubsub_topic_iam_member" "stress_test_publisher" {
  count = var.stress_test_fulfiller_config != null ? 1 : 0

  topic  = module.stress_test_fulfiller_queue[0].pubsub_topic.id
  role   = "roles/pubsub.publisher"
  member = var.pubsub_iam_service_account_member
}

module "stress_test_fulfiller_tee_app" {
  count  = var.stress_test_fulfiller_config != null ? 1 : 0
  source = "../mig"

  depends_on = [module.secrets]

  instance_template_name        = "stress-test-fulfiller-template"
  base_instance_name            = "stress-test-fulfiller"
  managed_instance_group_name   = "stress-test-fulfiller-mig"
  subscription_id               = module.stress_test_fulfiller_queue[0].pubsub_subscription.name
  mig_service_account_name      = "stress-test-fulfiller-sa"
  single_instance_assignment    = 1
  min_replicas                  = 0
  max_replicas                  = 1
  machine_type                  = var.stress_test_fulfiller_config.machine_type
  java_tool_options             = var.stress_test_fulfiller_config.java_tool_options
  docker_image                  = var.stress_test_fulfiller_config.docker_image
  mig_distribution_policy_zones = ["us-central1-a"]
  terraform_service_account     = var.terraform_service_account
  secrets_to_access             = local.result_fulfiller_secrets_to_access
  tee_cmd                       = var.stress_test_fulfiller_config.app_flags
  disk_image_family             = "confidential-space-debug"
  config_storage_bucket         = module.config_files_bucket.storage_bucket.name
  subnetwork_name               = google_compute_subnetwork.private_subnetwork.name
  tee_signed_image_repo         = "ghcr.io/world-federation-of-advertisers/edp-aggregator/results_fulfiller"
  extra_metadata = merge(local.otel_metadata, {
    "tee-env-OTEL_SERVICE_NAME" = "edpa.results_fulfiller.stress_test"
  })
}

resource "google_storage_bucket_iam_member" "stress_test_fulfiller_storage_viewer" {
  count = var.stress_test_fulfiller_config != null ? 1 : 0

  bucket = module.edp_aggregator_bucket.storage_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${module.stress_test_fulfiller_tee_app[0].mig_service_account.email}"
}

resource "google_storage_bucket_iam_member" "stress_test_fulfiller_storage_creator" {
  count = var.stress_test_fulfiller_config != null ? 1 : 0

  bucket = module.edp_aggregator_bucket.storage_bucket.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${module.stress_test_fulfiller_tee_app[0].mig_service_account.email}"
}

resource "google_storage_bucket_iam_member" "stress_test_fulfiller_config_storage_viewer" {
  count = var.stress_test_fulfiller_config != null ? 1 : 0

  bucket = module.config_files_bucket.storage_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${module.stress_test_fulfiller_tee_app[0].mig_service_account.email}"
}
