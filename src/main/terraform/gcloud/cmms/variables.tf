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

variable "cluster_location" {
  description = "Location of Kubernetes clusters. Defaults to provider zone."
  type        = string
  default     = null
}

variable "cluster_release_channel" {
  description = "Release channel for GKE clusters."
  type        = string
  default     = "REGULAR"
  nullable    = false
}

variable "key_ring_name" {
  description = "Name of the KMS key ring."
  type        = string
}

variable "key_ring_location" {
  description = "Location of the KMS key ring. Defaults to provider region."
  type        = string
  default     = null
}

variable "spanner_instance_name" {
  description = "Google Cloud Spanner instance name."
  type        = string
  default     = "halo-cmms"
  nullable    = false
}

variable "spanner_instance_config" {
  description = "Google Cloud Spanner instance configuration name."
  type        = string
  default     = "regional-us-central1"
  nullable    = false
}

variable "spanner_processing_units" {
  description = "Number of processing units allocated to the Spanner instance."
  type        = number
  default     = 100
  nullable    = false
}

variable "storage_bucket_name" {
  description = "Name of Google Cloud Storage bucket."
  type        = string
  nullable    = false
}

variable "storage_bucket_location" {
  description = "Location of the Google Cloud Storage bucket. Defaults to provider region."
  type        = string
  default     = null
}

variable "postgres_instance_name" {
  description = "Name of the PostgreSQL Cloud SQL instance."
  type        = string
  default     = "halo"
  nullable    = false
}

variable "postgres_instance_tier" {
  description = "Tier (machine type) of the PostgreSQL Cloud SQL instance."
  type        = string
  default     = "db-f1-micro"
  nullable    = false
}

variable "postgres_password" {
  description = "Password for postgres user."
  type        = string
  sensitive   = true
  nullable    = false
}

variable "secure_computation_storage_bucket_name" {
  description = "Name of Google Cloud Storage bucket for Secure Computation."
  type        = string
  nullable    = false
}

variable "edpa_config_files_bucket_name" {
  description = "Name of Google Cloud Storage bucket for Edp Aggregator cloud functions' configs."
  type        = string
  nullable    = false
}

variable "terraform_service_account" {
  description = "Service account used by terraform that needs to attach the MIG service account to the VM."
  type        = string
  nullable    = false
}

variable "data_watcher_config_file_path" {
  description = "Path to the data watcher config file."
  type        = string
  nullable    = false
}

variable "requisition_fetcher_config_file_path" {
  description = "Path to the requisition fetcher config file."
  type        = string
  nullable    = false
}

variable "event_data_provider_configs_file_path" {
  description = "Path to the event data provider config file for the ResultsFulfiller TEE app."
  type        = string
  nullable    = false
}

variable "results_fulfiller_population_spec_file_path" {
  description = "Path to the requisition fetcher population spec file."
  type        = string
  nullable    = false
}

variable "kingdom_public_api_target" {
  description = "Kingdom public api target"
  type        = string
}

variable "secure_computation_public_api_target" {
  description = "Secure Computation public api target"
  type        = string
}

variable "metadata_storage_public_api_target" {
  description = "Metadata storage public api target"
  type        = string
}

variable "image_tag" {
  description = "Tag of container images"
  type        = string
}

variable "data_watcher_env_var" {
  description = "DataWatcher extra env variables"
  type        = string
}

variable "data_watcher_secret_mapping" {
  description = "DataWatcher secret mapping"
  type        = string
}

variable "requisition_fetcher_env_var" {
  description = "RequisitionFetcher extra env variables"
  type        = string
}

variable "requisition_fetcher_secret_mapping" {
  description = "RequisitionFetcher secret mapping"
  type        = string
}

variable "event_group_env_var" {
  description = "EventGroupSync extra env variables"
  type        = string
}

variable "event_group_secret_mapping" {
  description = "EventGroupSync secret mapping"
  type        = string
}

variable "data_availability_env_var" {
  description = "DataAvailabilitySync extra env variables"
  type        = string
}

variable "data_availability_secret_mapping" {
  description = "DataAvailabilitySync secret mapping"
  type        = string
}

variable "data_watcher_uber_jar_path" {
  description = "Path to DataWatcher uber jar."
  type = string
}

variable "requisition_fetcher_uber_jar_path" {
  description = "Path to RequisitionFetcher uber jar."
  type = string
}

variable "event_group_uber_jar_path" {
  description = "Path to EventGroupSync uber jar."
  type = string
}

variable "data_availability_uber_jar_path" {
  description = "Path to DataAvailability uber jar."
  type = string
}

variable "results_fulfiller_event_proto_descriptor_path" {
  description = "Serialized FileDescriptorSet path for EventTemplate metadata types."
  type = string
}

variable "results_fulfiller_event_proto_descriptor_blob_uri" {
  description = "GCS blob uri of the FileDescriptorSet containing the event message descriptor."
  type = string
}

variable "results_fulfiller_event_template_type_name" {
  description = "Fully qualified name of the event template proto message"
  type = string
}

variable "results_fulfiller_population_spec_blob_uri" {
  description = "GCS blob uri of the Results Fulfiller population spec."
  type = string
}

variable "edpa_model_line_map" {
  description = "Mapping of available model line for the Results Fulfiller"
  type = string
}

variable "duchy_worker1_id" {
  description = "ID of duchy worker 1."
  type = string
}

variable "duchy_worker1_target" {
  description = "Target of duchy worker 1."
  type = string
}

variable "duchy_worker2_id" {
  description = "ID of duchy worker 2."
  type = string
}

variable "duchy_worker2_target" {
  description = "Target of duchy worker 2."
  type = string
}

variable "results_fulfiller_trusted_root_ca_collection_file_path" {
  description = "Trusted root CA used by the Results Fulfiller."
  type = string
}

variable "duchy_aggregator_computations_service_target" {
  description = "Target of aggregator computations service."
  type = string
}

variable "kingdom_system_api_target" {
  description = "Target of the kingdom system api"
  type = string
}

variable "duchy_aggregator_cert_id" {
  description = "Aggregator's certificate id"
  type = string
}

variable "edp_simulator_names" {
  description = "A list of names for the EDP simulators to create."
  type        = list(string)
  default     = []
}

variable "trusted_image_signing_fingerprint" {
  description = "The trusted signing fingerprint for images by the simulators."
  type        = string
  nullable    = false
}