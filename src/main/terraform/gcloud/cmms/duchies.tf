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

locals {
  aggregator_tls_cert = {
    secret_id         = "aggregator-tls-cert",
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/aggregator_tls.pem"),
    is_binary_format  = false
  }

  aggregator_tls_key = {
    secret_id         = "aggregator-tls-key",
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/aggregator_tls.key"),
    is_binary_format  = false
  }

  aggregator_cert_collection = {
    secret_id         = "aggregator-cert-collection",
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/all_root_certs.pem"),
    is_binary_format  = false
  }

  aggregator_cs_cert = {
    secret_id         = "aggregator-cs-cert",
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/aggregator_cs_cert.der"),
    is_binary_format  = true
  }

  aggregator_cs_private = {
    secret_id         = "aggregator-cs-private",
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/aggregator_cs_private.der"),
    is_binary_format  = true
  }

  aggregator_trustee_config = {
    instance_template_name             = "trustee-mill-template"
    base_instance_name                 = "trustee-mill"
    managed_instance_group_name        = "trustee-mill-mig"
    mig_service_account_name           = "trustee-mill-mig-sa"
    replicas                           = 1
    machine_type                       = "c4d-standard-2"
    docker_image                       = "ghcr.io/world-federation-of-advertisers/duchy/trus-tee-mill:${var.image_tag}"
    signed_image_repo                  = "ghcr.io/world-federation-of-advertisers/duchy/trus-tee-mill"
    mig_distribution_policy_zones      = ["us-central1-a"]
    # TODO(world-federation-of-advertisers/cross-media-measurement#3370): Use "confidential-space" to turn off debug
    # mode when log is visible on Cloud.
    disk_image_family                  = "confidential-space-debug"

    aggregator_tls_cert                = local.aggregator_tls_cert
    aggregator_tls_key                 = local.aggregator_tls_key
    aggregator_cert_collection         = local.aggregator_cert_collection
    aggregator_cs_cert                 = local.aggregator_cs_cert
    aggregator_cs_private              = local.aggregator_cs_private
    terraform_service_account          = var.terraform_service_account

    app_flags = [
      "--google-project-id", data.google_client_config.default.project,
      "--tls-cert-secret-id", local.aggregator_tls_cert.secret_id,
      "--tls-key-secret-id", local.aggregator_tls_key.secret_id,
      "--cert-collection-secret-id", local.aggregator_cert_collection.secret_id,
      "--cs-cert-secret-id", local.aggregator_cs_cert.secret_id,
      "--cs-private-key-secret-id", local.aggregator_cs_private.secret_id,

      "--tls-cert-file", "/tmp/secrets/aggregator_tls.pem",
      "--tls-key-file", "/tmp/secrets/aggregator_tls.key",
      "--cert-collection-file", "/tmp/secrets/all_root_certs.pem",
      "--consent-signaling-certificate-der-file", "/tmp/secrets/aggregator_cs_cert.der",
      "--consent-signaling-private-key-der-file", "/tmp/secrets/aggregator_cs_private.der",
      "--attestation-token-file", "/run/container_launcher/attestation_verifier_claims_token",

      "--computations-service-target", "${var.duchy_aggregator_computations_service_target}",
      "--computations-service-cert-host", "localhost",
      "--kingdom-system-api-target", "${var.kingdom_system_api_target}",
      "--kingdom-system-api-cert-host", "localhost",

      "--duchy-name", "aggregator",
      "--work-lock-duration", "10m",
      "--polling-interval", "5s",

      "--consent-signaling-certificate-resource-name", "duchies/aggregator/certificates/${var.duchy_aggregator_cert_id}",

      "--google-cloud-storage-project", data.google_client_config.default.project,
      "--google-cloud-storage-bucket", "${var.storage_bucket_name}",
    ]
  }
}

module "clusters" {
  source   = "../modules/cluster"
  for_each = local.duchy_names

  name                = "${each.key}-duchy"
  location            = local.cluster_location
  release_channel     = var.cluster_release_channel
  secret_key          = module.common.cluster_secret_key
  autoscaling_profile = "BALANCED"
}

module "default_node_pools" {
  source   = "../modules/node-pool"
  for_each = module.clusters

  cluster         = each.value.cluster
  name            = "default"
  service_account = module.common.cluster_service_account
  machine_type    = "e2-standard-2"
  max_node_count  = 3
}

module "highmem_node_pools" {
  source   = "../modules/node-pool"
  for_each = module.clusters

  cluster         = each.value.cluster
  name            = "highmem"
  service_account = module.common.cluster_service_account
  machine_type    = "c3d-standard-4"
  max_node_count  = 20
  spot            = true
}

module "storage" {
  source = "../modules/storage-bucket"

  name     = var.storage_bucket_name
  location = local.storage_bucket_location
}

# TODO(hashicorp/terraform#24476): Use a for_each for the Duchy modules once
# that works with providers.

module "aggregator_duchy" {
  source = "../modules/duchy"

  name             = "aggregator"
  database_name    = "aggregator_duchy_computations"
  spanner_instance = google_spanner_instance.spanner_instance
  storage_bucket   = module.storage.storage_bucket

  # TrusTEE MIG configurations
  trustee_config = local.aggregator_trustee_config
}

module "worker1_duchy" {
  source = "../modules/duchy"

  name             = "worker1"
  database_name    = "worker1_duchy_computations"
  spanner_instance = google_spanner_instance.spanner_instance
  storage_bucket   = module.storage.storage_bucket
}

module "worker2_duchy" {
  source = "../modules/duchy"

  name             = "worker2"
  database_name    = "worker2_duchy_computations"
  spanner_instance = google_spanner_instance.spanner_instance
  storage_bucket   = module.storage.storage_bucket
}
