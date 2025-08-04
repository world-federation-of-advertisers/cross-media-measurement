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

locals {
  common_secrets_to_mount = [
    {
      secret_id  = var.edpa_tee_app_tls_key.secret_id
      version    = "latest"
      mount_path = "/etc/ssl/edpa_tee_app_tls.key",
      flag_name  = "--edpa-tls-key-file-path"
    },
    {
      secret_id  = var.edpa_tee_app_tls_pem.secret_id
      version    = "latest"
      mount_path = "/etc/ssl/edpa_tee_app_tls.pem",
      flag_name  = "--edpa-tls-cert-file-path"
    },
    {
      secret_id  = var.secure_computation_root_ca.secret_id
      version    = "latest"
      mount_path = "/etc/ssl/secure_computation_root.pem",
      flag_name  = "--secure-computation-cert-collection-file-path"
    },
    {
      secret_id  = var.kingdom_root_ca.secret_id
      version    = "latest"
      mount_path = "/etc/ssl/kingdom_root.pem",
      flag_name  = "--kingdom-cert-collection-file-path"
    },
  ]

  edp_secrets_to_mount = flatten([
    for edp_name, certs in var.edps_certs : [
      {
        secret_id  = certs.cert_der.secret_id
        version    = "latest"
        mount_path = "/etc/ssl/${edp_name}_cs_cert.der"
      },
      {
        secret_id  = certs.private_der.secret_id
        version    = "latest"
        mount_path = "/etc/ssl/${edp_name}_cs_private.der"
      },
      {
        secret_id  = certs.enc_private.secret_id
        version    = "latest"
        mount_path = "/etc/ssl/${edp_name}_enc_private.tink"
      },
      {
        secret_id  = certs.tls_key.secret_id
        version    = "latest"
        mount_path = "/etc/ssl/${edp_name}_tls.key"
      },
      {
        secret_id  = certs.tls_pem.secret_id
        version    = "latest"
        mount_path = "/etc/ssl/${edp_name}_tls.pem"
      },
    ]
  ])

  result_fulfiller_secrets_to_mount = concat(
    local.common_secrets_to_mount,
    local.edp_secrets_to_mount,
  )

  edps_secrets = merge([
    for edp_name, certs in var.edps_certs : {
      for key, value in certs : "${edp_name}_${key}" => value
    }
  ]...)

  all_secrets = merge(
    { edpa_tee_app_tls_key                          = var.edpa_tee_app_tls_key },
    { edpa_tee_app_tls_pem                          = var.edpa_tee_app_tls_pem },
    { data_watcher_tls_key                          = var.data_watcher_tls_key },
    { data_watcher_tls_pem                          = var.data_watcher_tls_pem },
    { secure_computation_root_ca                    = var.secure_computation_root_ca },
    { kingdom_root_ca                               = var.kingdom_root_ca },
    local.edps_secrets
  )

  data_watcher_secrets_access = [
    "secure_computation_root_ca",
    "data_watcher_tls_key",
    "data_watcher_tls_pem",
  ]

  edp_tls_keys = flatten([
    for edp_name, certs in var.edps_certs : [
      "${edp_name}_tls_key",
      "${edp_name}_tls_pem",
      "${edp_name}_enc_private",
    ]
  ])

  requisition_fetcher_secrets_access = concat(
    ["kingdom_root_ca"],
    local.edp_tls_keys
  )

  event_group_sync_secrets_access = concat(
    ["kingdom_root_ca"],
    local.edp_tls_keys
  )

  cloud_function_secret_pairs = tomap({
    data_watcher        = local.data_watcher_secrets_access,
    requisition_fetcher = local.requisition_fetcher_secrets_access,
    event_group_sync    = local.event_group_sync_secrets_access,
  })

  secret_access_map = merge([
    for fn, key_list in local.cloud_function_secret_pairs : {
      for secret_key in key_list :
        "${fn}:${secret_key}" => {
          function_name = fn
          secret_key    = secret_key
        }
    }
  ]...)

  service_accounts = {
    "data_watcher"        = module.data_watcher_cloud_function.cloud_function_service_account.email
    "requisition_fetcher" = module.requisition_fetcher_cloud_function.cloud_function_service_account.email
    "event_group_sync"    = module.event_group_sync_cloud_function.cloud_function_service_account.email
  }
}

module "edp_aggregator_bucket" {
  source   = "../storage-bucket"

  name     = var.edp_aggregator_bucket_name
  location = var.edp_aggregator_buckets_location
}

module "config_files_bucket" {
  source   = "../storage-bucket"

  name     = var.config_files_bucket_name
  location = var.edp_aggregator_buckets_location
}

resource "google_storage_bucket_object" "upload_data_watcher_config" {
  name   = var.data_watcher_config.destination
  bucket = module.config_files_bucket.storage_bucket.name
  source = var.data_watcher_config.local_path
}

resource "google_storage_bucket_object" "upload_requisition_fetcher_config" {
  name   = var.requisition_fetcher_config.destination
  bucket = module.config_files_bucket.storage_bucket.name
  source = var.requisition_fetcher_config.local_path
}

resource "google_storage_bucket_object" "upload_results_fulfiller_proto_descriptors" {
  name   = var.results_fulfiller_config.destination
  bucket = module.config_files_bucket.storage_bucket.name
  source = var.results_fulfiller_config.local_path
}

resource "google_project_iam_member" "eventarc_service_agent" {
  project = data.google_project.project.project_id
  role    = "roles/eventarc.serviceAgent"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-eventarc.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "storage_service_agent" {
  project = data.google_project.project.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.project.number}@gs-project-accounts.iam.gserviceaccount.com"
}

module "secrets" {
  source            = "../secret"
  for_each          = local.all_secrets
  secret_id         = each.value.secret_id
  secret_path       = each.value.secret_local_path
  is_binary_format  = each.value.is_binary_format
}

module "data_watcher_cloud_function" {
  source    = "../gcs-bucket-cloud-function"

  cloud_function_service_account_name           = var.data_watcher_service_account_name
  cloud_function_trigger_service_account_name   = var.data_watcher_trigger_service_account_name
  trigger_bucket_name                           = module.edp_aggregator_bucket.storage_bucket.name
  terraform_service_account                     = var.terraform_service_account
  function_name                                 = var.cloud_function_configs.data_watcher.function_name
  entry_point                                   = var.cloud_function_configs.data_watcher.entry_point
  extra_env_vars                                = var.cloud_function_configs.data_watcher.extra_env_vars
  secret_mappings                               = var.cloud_function_configs.data_watcher.secret_mappings
  uber_jar_path                                 = var.cloud_function_configs.data_watcher.uber_jar_path
}

module "requisition_fetcher_cloud_function" {
  source    = "../http-cloud-function"

  http_cloud_function_service_account_name  = var.requisition_fetcher_service_account_name
  terraform_service_account                 = var.terraform_service_account
  function_name                             = var.cloud_function_configs.requisition_fetcher.function_name
  entry_point                               = var.cloud_function_configs.requisition_fetcher.entry_point
  extra_env_vars                            = var.cloud_function_configs.requisition_fetcher.extra_env_vars
  secret_mappings                           = var.cloud_function_configs.requisition_fetcher.secret_mappings
  uber_jar_path                             = var.cloud_function_configs.requisition_fetcher.uber_jar_path
}

module "event_group_sync_cloud_function" {
  source    = "../http-cloud-function"

  http_cloud_function_service_account_name  = var.event_group_sync_service_account_name
  terraform_service_account                 = var.terraform_service_account
  function_name                             = var.cloud_function_configs.event_group_sync.function_name
  entry_point                               = var.cloud_function_configs.event_group_sync.entry_point
  extra_env_vars                            = var.cloud_function_configs.event_group_sync.extra_env_vars
  secret_mappings                           = var.cloud_function_configs.event_group_sync.secret_mappings
  uber_jar_path                             = var.cloud_function_configs.event_group_sync.uber_jar_path
}

resource "google_secret_manager_secret_iam_member" "secret_accessor" {
  for_each = local.secret_access_map
  secret_id = local.all_secrets[each.value.secret_key].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${local.service_accounts[each.value.function_name]}"

}

module "result_fulfiller_queue" {
  source   = "../pubsub"

  topic_name              = var.requisition_fulfiller_config.queue.topic_name
  subscription_name       = var.requisition_fulfiller_config.queue.subscription_name
  ack_deadline_seconds    = var.requisition_fulfiller_config.queue.ack_deadline_seconds
}

resource "google_pubsub_topic_iam_member" "publisher" {
  topic  = module.result_fulfiller_queue.pubsub_topic.id
  role   = "roles/pubsub.publisher"
  member = var.pubsub_iam_service_account_member
}

module "result_fulfiller_tee_app" {
  source   = "../mig"

  depends_on = [module.secrets]

  instance_template_name        = var.requisition_fulfiller_config.worker.instance_template_name
  base_instance_name            = var.requisition_fulfiller_config.worker.base_instance_name
  managed_instance_group_name   = var.requisition_fulfiller_config.worker.managed_instance_group_name
  subscription_id               = module.result_fulfiller_queue.pubsub_subscription.name
  mig_service_account_name      = var.requisition_fulfiller_config.worker.mig_service_account_name
  single_instance_assignment    = var.requisition_fulfiller_config.worker.single_instance_assignment
  min_replicas                  = var.requisition_fulfiller_config.worker.min_replicas
  max_replicas                  = var.requisition_fulfiller_config.worker.max_replicas
  app_args                      = var.requisition_fulfiller_config.worker.app_args
  machine_type                  = var.requisition_fulfiller_config.worker.machine_type
  docker_image                  = var.requisition_fulfiller_config.worker.docker_image
  mig_distribution_policy_zones = var.requisition_fulfiller_config.worker.mig_distribution_policy_zones
  terraform_service_account     = var.terraform_service_account
  secrets_to_mount              = local.result_fulfiller_secrets_to_mount
}

resource "google_storage_bucket_iam_member" "result_fulfiller_storage_viewer" {
  bucket = module.edp_aggregator_bucket.storage_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${module.result_fulfiller_tee_app.mig_service_account.email}"
}

resource "google_storage_bucket_iam_member" "result_fulfiller_storage_creator" {
  bucket = module.edp_aggregator_bucket.storage_bucket.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${module.result_fulfiller_tee_app.mig_service_account.email}"
}

resource "google_storage_bucket_iam_binding" "aggregator_storage_admin" {
  bucket = module.edp_aggregator_bucket.storage_bucket.name
  role   = "roles/storage.objectAdmin"
  members = [
    "serviceAccount:${module.requisition_fetcher_cloud_function.cloud_function_service_account.email}",
    "serviceAccount:${module.event_group_sync_cloud_function.cloud_function_service_account.email}",
  ]
}

resource "google_storage_bucket_iam_member" "requisition_fetcher_config_storage_viewer" {
  bucket = module.config_files_bucket.storage_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${module.requisition_fetcher_cloud_function.cloud_function_service_account.email}"
}

resource "google_storage_bucket_iam_member" "data_watcher_config_storage_viewer" {
  bucket = module.config_files_bucket.storage_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${module.data_watcher_cloud_function.cloud_function_service_account.email}"
}

resource "google_storage_bucket_iam_member" "results_fulfiller_config_storage_viewer" {
  bucket = module.config_files_bucket.storage_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${module.result_fulfiller_tee_app.mig_service_account.email}"
}

resource "google_cloud_run_service_iam_member" "event_group_sync_invoker" {
  service  = var.event_group_sync_function_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${module.data_watcher_cloud_function.cloud_function_service_account.email}"
}
