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

variable "requisition_fulfiller_config" {
  description = "Config for a single Pub/Sub queue and its corresponding MIG worker"
  type = object({
    queue = object({
      subscription_name     = string
      topic_name            = string
      ack_deadline_seconds  = number
    })
    worker = object({
      instance_template_name        = string
      base_instance_name            = string
      managed_instance_group_name   = string
      mig_service_account_name      = string
      single_instance_assignment    = number
      min_replicas                  = number
      max_replicas                  = number
      machine_type                  = string
      java_tool_options             = optional(string)
      docker_image                  = string
      mig_distribution_policy_zones = list(string)
      app_flags                     = list(string)
    })
  })
}

variable "edpa_tee_app_tls_key" {
  description = "EDPA tls key"
  type = object({
    secret_id         = string
    secret_local_path = string
    is_binary_format  = bool
  })
}

variable "edpa_tee_app_tls_pem" {
  description = "EDPA tls pem"
  type = object({
    secret_id         = string
    secret_local_path = string
    is_binary_format  = bool
  })
}

variable "data_watcher_tls_key" {
  description = "Data Watcher tls key"
  type = object({
    secret_id         = string
    secret_local_path = string
    is_binary_format  = bool
  })
}

variable "data_watcher_tls_pem" {
  description = "Data Watcher tls pem"
  type = object({
    secret_id         = string
    secret_local_path = string
    is_binary_format  = bool
  })
}

variable "data_availability_tls_key" {
  description = "Data Availability tls key"
  type = object({
    secret_id         = string
    secret_local_path = string
    is_binary_format  = bool
  })
}

variable "data_availability_tls_pem" {
  description = "Data Availability tls pem"
  type = object({
    secret_id         = string
    secret_local_path = string
    is_binary_format  = bool
  })
}

variable "secure_computation_root_ca" {
  description = "Secure Computation root CA"
  type = object({
    secret_id         = string
    secret_local_path = string
    is_binary_format  = bool
  })
}

variable "metadata_storage_root_ca" {
  description = "Secure Computation root CA"
  type = object({
    secret_id         = string
    secret_local_path = string
    is_binary_format  = bool
  })
}

variable "trusted_root_ca_collection" {
  description = "Collection of certificates for each Duchy and the Kingdom"
  type = object({
    secret_id         = string
    secret_local_path = string
    is_binary_format  = bool
  })
}

variable "edps_certs" {
  description = "Map of EDPs and their certificates"
  type = map(object({
    cert_der = object({
      secret_id         = string
      secret_local_path = string
      is_binary_format  = bool
    })
    private_der = object({
      secret_id         = string
      secret_local_path = string
      is_binary_format  = bool
    })
    enc_private = object({
      secret_id         = string
      secret_local_path = string
      is_binary_format  = bool
    })
    tls_key = object({
      secret_id         = string
      secret_local_path = string
      is_binary_format  = bool
    })
    tls_pem = object({
      secret_id         = string
      secret_local_path = string
      is_binary_format  = bool
    })
  }))
}

variable "pubsub_iam_service_account_member" {
  description = "IAM `google_service_account` for public api to access pubsub."
  type        = string
  default     = "secure-computation-pubsub"
  nullable    = false
}

variable "edp_aggregator_bucket_name" {
  description = "Name of the Google Cloud Storage bucket used by the Edp Aggregator."
  type        = string
  nullable    = false
}

variable "config_files_bucket_name" {
  description = "Name of the Google Cloud Storage bucket used to store configuration."
  type        = string
  nullable    = false
}

variable "data_watcher_config" {
  description = "An object containing the local path of the data watcher config file and its destination path in Cloud Storage."
  type = object({
    local_path  = string
    destination = string
  })
}

variable "requisition_fetcher_config" {
  description = "An object containing the local path of the requisition fetcher config file and its destination path in Cloud Storage."
  type = object({
    local_path  = string
    destination = string
  })
}

variable "edps_config" {
  description = "An object containing the local path of the edps config file and its destination path in Cloud Storage."
  type = object({
    local_path  = string
    destination = string
  })
}

variable "results_fulfiller_event_descriptor" {
  description = "An object containing the local path of the results fulfiller event descriptor file and its destination path in Cloud Storage."
  type = object({
    local_path  = string
    destination = string
  })
}

variable "results_fulfiller_population_spec" {
  description = "An object containing the local path of the results fulfiller population spec file and its destination path in Cloud Storage."
  type = object({
    local_path  = string
    destination = string
  })
}

variable "edp_aggregator_buckets_location" {
  description = "Location of the Storage buckets used by the Edp Aggregator."
  type        = string
  nullable    = false
}

variable "data_watcher_service_account_name" {
  description = "Name of the DataWatcher service account."
  type        = string
  nullable    = false
}

variable "data_watcher_trigger_service_account_name" {
  description = "The name of the service account used to trigger the Cloud Function."
  type        = string
  nullable    = false
}

variable "terraform_service_account" {
  description = "Service account used by terraform that needs to attach the MIG service account to the VM."
  type        = string
  nullable    = false
}

variable "requisition_fetcher_service_account_name" {
  description = "Name of the RequisitionFetcher service account."
  type        = string
  nullable    = false
}

variable "event_group_sync_service_account_name" {
  description = "Name of the EventGroupSync service account."
  type        = string
  nullable    = false
}

variable "data_availability_sync_service_account_name" {
  description = "Name of the DataAvailabilitySync service account."
  type        = string
  nullable    = false
}

variable "event_group_sync_function_name" {
  description = "Name of the EventGroupSync cloud function."
  type        = string
  nullable    = false
}

variable "data_availability_sync_function_name" {
  description = "Name of the DataAvailabilitySync cloud function."
  type        = string
  nullable    = false
}

variable "cloud_function_configs" {
  type = map(object({
    function_name       = string
    entry_point         = string
    extra_env_vars      = string
    secret_mappings     = string
    uber_jar_path       = string
  }))
}

variable "results_fulfiller_disk_image_family" {
  description = "The boot disk image family."
  type        = string
  default     = "confidential-space"
}

variable "private_subnetwork_name" {
  description = "The name of the subnetwork for the MIG instances."
  type        = string
  default     = "private-subnet"
}

variable "private_router_name" {
  description = "The name for the Cloud Router for the private network."
  type        = string
  default     = "nat-router"
}

variable "nat_name" {
  description = "The name for the Cloud NAT gateway."
  type        = string
  default     = "nat-gateway"
}

variable "dns_managed_zone_name" {
  description = "The name for Google DNS Managed Zone."
  type        = string
  default     = "nat-gateway"
}

variable "requisition_fetcher_scheduler_config" {
  description = "Configuration for Google Cloud Scheduler to trigger the RequisitionFetcher"
  type = object({
    schedule                    = string
    time_zone                   = string
    name                        = string
    function_url                = string
    scheduler_sa_display_name   = string
    scheduler_sa_description    = string
    scheduler_job_description   = string
  })
  nullable = false
}

variable "private_subnetwork_cidr_range" {
  description = "The range of IP addresses belonging to this subnetwork."
  type        = string
  default     = "192.168.0.0/16"
}

variable "private_subnetwork_network" {
  description = "The network this subnet belongs to"
  type        = string
  default     = "default"
}

variable "edp_aggregator_service_account_name" {
    description = "Name of the EdpAggregator service account."
    type        = string
    nullable    = false
}

variable "spanner_instance" {
  description = "`google_spanner_instance` for the system."
  type = object({
    name = string
  })
  nullable = false
}

variable "spanner_database_name" {
  description = "Name of the Spanner database for Edp Aggregator."
  type        = string
  default     = "edp-aggregator"
  nullable    = false
}

variable "requisition_fetcher_tls_key" {
  description = "Requisition Fetcher tls key"
  type = object({
    secret_id         = string
    secret_local_path = string
    is_binary_format  = bool
  })
}

variable "requisition_fetcher_tls_pem" {
  description = "Requisition Fetcher tls pem"
  type = object({
    secret_id         = string
    secret_local_path = string
    is_binary_format  = bool
  })
}

variable "edp_aggregator_api_server_ip_address" {
  description = "IP address for edp aggregator public API server"
  type        = string
  nullable    = true
  default     = null
}
