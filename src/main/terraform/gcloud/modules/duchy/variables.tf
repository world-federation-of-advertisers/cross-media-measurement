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

variable "name" {
  description = "Name (external ID) of the Duchy."
  type        = string
  nullable    = false
}

variable "database_name" {
  description = "Name of the database. Defaults to <name>-duchy."
  type        = string
}

variable "storage_bucket" {
  description = "`google_storage_bucket` for the system."
  type = object({
    name = string
  })
}

variable "spanner_instance" {
  description = "`google_spanner_instance` for the system."
  type = object({
    name = string
  })
  nullable = false
}

variable "v2alpha_ip_address" {
  description = "IP address for v2alpha public API"
  type        = string
  nullable    = true
  default     = null
}

variable "system_v1alpha_ip_address" {
  description = "IP address for v1alpha system API"
  type        = string
  nullable    = true
  default     = null
}

variable "internal_ip_address" {
  description = "IP address for internal API"
  type        = string
  nullable    = true
  default     = null
}

variable "dashboard_json_files" {
  description = "List of filenames of dashboard json templates"
  type        = list(string)
  nullable    = true
  default     = [
    "duchy_dashboard_1.json.tmpl",
    "duchy_dashboard_2.json.tmpl",
  ]
}

variable "trustee_mill_subnetwork_network" {
  description = "The network this subnet belongs to"
  type        = string
  default     = "default"
}

variable "trustee_mill_subnetwork_cidr_range" {
  description = "The range of IP addresses belonging to this subnetwork."
  type        = string
  default     = "10.127.0.0/24"
}

variable "trustee_config" {
  description = "Config for TrusTEE MIG mill"
  type = object({
    instance_template_name        = string
    base_instance_name            = string
    managed_instance_group_name   = string
    mig_service_account_name      = string
    replicas                      = number
    machine_type                  = string
    docker_image                  = string
    signed_image_repo             = string
    mig_distribution_policy_zones = list(string)
    terraform_service_account     = string
    disk_image_family             = string

    aggregator_tls_cert           = object({
                                      secret_id         = string
                                      secret_local_path = string
                                      is_binary_format  = bool
                                    })
    aggregator_tls_key            = object({
                                      secret_id         = string
                                      secret_local_path = string
                                      is_binary_format  = bool
                                    })
    aggregator_cert_collection    = object({
                                      secret_id         = string
                                      secret_local_path = string
                                      is_binary_format  = bool
                                    })
    aggregator_cs_cert            = object({
                                      secret_id         = string
                                      secret_local_path = string
                                      is_binary_format  = bool
                                    })
    aggregator_cs_private         = object({
                                      secret_id         = string
                                      secret_local_path = string
                                      is_binary_format  = bool
                                    })

    app_flags                     = list(string)
  })
  default = null
}
