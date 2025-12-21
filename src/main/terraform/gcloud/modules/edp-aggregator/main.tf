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

data "google_client_config" "default" {}
data "google_project" "project" {}

locals {

  # IP addresses for private.googleapis.com (Private Google Access default VIPs)
  # Reference: https://cloud.google.com/vpc/docs/configure-private-google-access#ip-addr-defaults
  private_googleapis_ipv4 = ["199.36.153.8","199.36.153.9","199.36.153.10","199.36.153.11"]

  common_secrets_to_access = [
    {
      secret_id  = var.edpa_tee_app_tls_key.secret_id
      version    = "latest"
    },
    {
      secret_id  = var.edpa_tee_app_tls_pem.secret_id
      version    = "latest"
    },
    {
      secret_id  = var.secure_computation_root_ca.secret_id
      version    = "latest"
    },
    {
      secret_id  = var.trusted_root_ca_collection.secret_id
      version    = "latest"
    },
    {
      secret_id  = var.metadata_storage_root_ca.secret_id
      version    = "latest"
    },
  ]

  edp_secrets_to_access = flatten([
    for edp_name, certs in var.edps_certs : [
      {
        secret_id  = certs.cert_der.secret_id
        version    = "latest"
      },
      {
        secret_id  = certs.private_der.secret_id
        version    = "latest"
      },
      {
        secret_id  = certs.enc_private.secret_id
        version    = "latest"
      },
      {
        secret_id  = certs.tls_key.secret_id
        version    = "latest"
      },
      {
        secret_id  = certs.tls_pem.secret_id
        version    = "latest"
      },
    ]
  ])

  result_fulfiller_secrets_to_access = concat(
    local.common_secrets_to_access,
    local.edp_secrets_to_access,
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
    { data_availability_tls_key                     = var.data_availability_tls_key },
    { data_availability_tls_pem                     = var.data_availability_tls_pem },
    { requisition_fetcher_tls_pem                   = var.requisition_fetcher_tls_pem },
    { requisition_fetcher_tls_key                   = var.requisition_fetcher_tls_key },
    { secure_computation_root_ca                    = var.secure_computation_root_ca },
    { metadata_storage_root_ca                      = var.metadata_storage_root_ca },
    { trusted_root_ca_collection                    = var.trusted_root_ca_collection },
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

  data_availability_sync_secrets_access = concat(
    [
      "metadata_storage_root_ca",
      "trusted_root_ca_collection",
      "data_availability_tls_key",
      "data_availability_tls_pem",
    ],
    local.edp_tls_keys
  )

  requisition_fetcher_secrets_access = concat(
    [
      "trusted_root_ca_collection",
      "requisition_fetcher_tls_pem",
      "requisition_fetcher_tls_key",
      "metadata_storage_root_ca"
    ],
    local.edp_tls_keys
  )

  event_group_sync_secrets_access = concat(
    ["trusted_root_ca_collection"],
    local.edp_tls_keys
  )

  cloud_function_secret_pairs = tomap({
    data_watcher            = local.data_watcher_secrets_access,
    requisition_fetcher     = local.requisition_fetcher_secrets_access,
    event_group_sync        = local.event_group_sync_secrets_access,
    data_availability_sync  = local.data_availability_sync_secrets_access,
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
    "data_watcher"              = module.data_watcher_cloud_function.cloud_function_service_account.email
    "requisition_fetcher"       = module.requisition_fetcher_cloud_function.cloud_function_service_account.email
    "event_group_sync"          = module.event_group_sync_cloud_function.cloud_function_service_account.email
    "data_availability_sync"    = module.data_availability_sync_cloud_function.cloud_function_service_account.email
  }

  otel_metadata = {
    "tee-env-OTEL_SERVICE_NAME"                     = "edpa.results_fulfiller",
    "tee-env-OTEL_METRICS_EXPORTER"                 = "google_cloud_monitoring",
    "tee-env-OTEL_TRACES_EXPORTER"                  = "google_cloud_trace",
    "tee-env-OTEL_LOGS_EXPORTER"                    = "logging",
    "tee-env-OTEL_SERVICE_NAME"                     = "edpa.results_fulfiller",
    "tee-env-OTEL_EXPORTER_GOOGLE_CLOUD_PROJECT_ID" = data.google_project.project.project_id
    "tee-env-OTEL_METRIC_EXPORT_INTERVAL"           = "60000"
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

resource "google_storage_bucket_object" "upload_edps_config" {
  name   = var.edps_config.destination
  bucket = module.config_files_bucket.storage_bucket.name
  source = var.edps_config.local_path
}

resource "google_storage_bucket_object" "upload_results_fulfiller_proto_descriptors" {
  name   = var.results_fulfiller_event_descriptor.destination
  bucket = module.config_files_bucket.storage_bucket.name
  source = var.results_fulfiller_event_descriptor.local_path
}

resource "google_storage_bucket_object" "upload_results_fulfiller_population_spec" {
  name   = var.results_fulfiller_population_spec.destination
  bucket = module.config_files_bucket.storage_bucket.name
  source = var.results_fulfiller_population_spec.local_path
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

module "requisition_fetcher_cloud_scheduler" {
  source                        = "../cloud-scheduler"
  terraform_service_account     = var.terraform_service_account
  scheduler_config              = var.requisition_fetcher_scheduler_config
  depends_on                    = [module.requisition_fetcher_cloud_function]
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

module "data_availability_sync_cloud_function" {
  source    = "../http-cloud-function"

  http_cloud_function_service_account_name  = var.data_availability_sync_service_account_name
  terraform_service_account                 = var.terraform_service_account
  function_name                             = var.cloud_function_configs.data_availability_sync.function_name
  entry_point                               = var.cloud_function_configs.data_availability_sync.entry_point
  extra_env_vars                            = var.cloud_function_configs.data_availability_sync.extra_env_vars
  secret_mappings                           = var.cloud_function_configs.data_availability_sync.secret_mappings
  uber_jar_path                             = var.cloud_function_configs.data_availability_sync.uber_jar_path
}

resource "google_secret_manager_secret_iam_member" "secret_accessor" {
  depends_on = [module.secrets]
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
  machine_type                  = var.requisition_fulfiller_config.worker.machine_type
  java_tool_options             = var.requisition_fulfiller_config.worker.java_tool_options
  docker_image                  = var.requisition_fulfiller_config.worker.docker_image
  mig_distribution_policy_zones = var.requisition_fulfiller_config.worker.mig_distribution_policy_zones
  terraform_service_account     = var.terraform_service_account
  secrets_to_access             = local.result_fulfiller_secrets_to_access
  tee_cmd                       = var.requisition_fulfiller_config.worker.app_flags
  disk_image_family             = var.results_fulfiller_disk_image_family
  config_storage_bucket         = module.config_files_bucket.storage_bucket.name
  subnetwork_name               = google_compute_subnetwork.private_subnetwork.name
  # TODO(world-federation-of-advertisers/cross-media-measurement#2924): Rename `results_fulfiller` into `results-fulfiller`
  tee_signed_image_repo         = "ghcr.io/world-federation-of-advertisers/edp-aggregator/results_fulfiller"
  extra_metadata                = local.otel_metadata
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

resource "google_storage_bucket_iam_member" "data_availability_storage_viewer" {
  depends_on = [module.data_availability_sync_cloud_function]
  bucket = module.edp_aggregator_bucket.storage_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${module.data_availability_sync_cloud_function.cloud_function_service_account.email}"
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
  depends_on = [module.event_group_sync_cloud_function]
  service  = var.event_group_sync_function_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${module.data_watcher_cloud_function.cloud_function_service_account.email}"
}

resource "google_cloud_run_service_iam_member" "data_availability_sync_invoker" {
  depends_on = [module.data_availability_sync_cloud_function]
  service  = var.data_availability_sync_function_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${module.data_watcher_cloud_function.cloud_function_service_account.email}"
}

resource "google_compute_subnetwork" "private_subnetwork" {
  name                     = var.private_subnetwork_name
  region                   = data.google_client_config.default.region
  network                  = var.private_subnetwork_network
  ip_cidr_range            = var.private_subnetwork_cidr_range
  private_ip_google_access = true
}


# Cloud Router for NAT gateway
resource "google_compute_router" "nat_router" {
  name    = var.private_router_name
  region  = data.google_client_config.default.region
  network = "default"
}

# Cloud NAT configuration
resource "google_compute_router_nat" "nat_gateway" {
  name                               = var.nat_name
  router                             = google_compute_router.nat_router.name
  region                             = google_compute_router.nat_router.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "LIST_OF_SUBNETWORKS"

  subnetwork {
    name                    = google_compute_subnetwork.private_subnetwork.self_link
    source_ip_ranges_to_nat = ["ALL_IP_RANGES"]
  }

  enable_endpoint_independent_mapping = true

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

# DNS configuration for storage.googleapis.com
# Configures DNS so storage.googleapis.com resolves to the private endpoint
# All GCS bucket access will automatically use this private path
resource "google_dns_managed_zone" "private_gcs" {
  name        = var.dns_managed_zone_name
  dns_name    = "googleapis.com."
  description = "Private DNS zone for Google APIs via PSC"

  visibility = "private"

  private_visibility_config {
    networks {
      network_url = "projects/${data.google_project.project.project_id}/global/networks/default"
    }
  }
}

# A record points storage.googleapis.com to our PSC endpoint IP
# Instances in this VPC will resolve GCS to 10.0.0.100 instead of public IPs
resource "google_dns_record_set" "gcs_a_record" {
  name         = "private.googleapis.com."
  type         = "A"
  ttl          = 300
  managed_zone = google_dns_managed_zone.private_gcs.name
  rrdatas      = local.private_googleapis_ipv4
}

# Wildcard CNAME so any *.googleapis.com goes to the private VIPs
resource "google_dns_record_set" "googleapis_wildcard_cname" {
  name         = "*.googleapis.com."
  type         = "CNAME"
  ttl          = 300
  managed_zone = google_dns_managed_zone.private_gcs.name
  rrdatas      = ["private.googleapis.com."]
}

module "edp_aggregator_internal" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "internal-edp-aggregator-server"
  iam_service_account_name        = var.edp_aggregator_service_account_name
  iam_service_account_description = "Edp Aggregator internal API server."
}

resource "google_project_iam_member" "edp_aggregator_internal_metric_writer" {
  project = data.google_project.project.name
  role    = "roles/monitoring.metricWriter"
  member  = module.edp_aggregator_internal.iam_service_account.member
}

resource "google_spanner_database" "edp_aggregator" {
  instance         = var.spanner_instance.name
  name             = var.spanner_database_name
  database_dialect = "GOOGLE_STANDARD_SQL"
}

resource "google_spanner_database_iam_member" "edp_aggregator_internal" {
  instance = google_spanner_database.edp_aggregator.instance
  database = google_spanner_database.edp_aggregator.name
  role     = "roles/spanner.databaseUser"
  member   = module.edp_aggregator_internal.iam_service_account.member

  lifecycle {
    replace_triggered_by = [google_spanner_database.edp_aggregator.id]
  }
}

resource "google_compute_address" "edp_aggregator_api_server" {
  name    = "edp-aggregator-system"
  address = var.edp_aggregator_api_server_ip_address
}

resource "google_project_iam_member" "telemetry_log_writer" {
  for_each = local.service_accounts
  project  = data.google_project.project.project_id
  role     = "roles/logging.logWriter"
  member   = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "telemetry_metric_writer" {
  for_each = local.service_accounts
  project  = data.google_project.project.project_id
  role     = "roles/monitoring.metricWriter"
  member   = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "telemetry_trace_agent" {
  for_each = local.service_accounts
  project  = data.google_project.project.project_id
  role     = "roles/cloudtrace.agent"
  member   = "serviceAccount:${each.value}"
}
