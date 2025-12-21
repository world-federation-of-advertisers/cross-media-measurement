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

variable "instance_template_name" {
  description = "The name of the instance template."
  type        = string
  default     = null
}

variable "base_instance_name" {
  description = "The base instance name to use for instances in this group."
  type        = string
  nullable    = false
}

variable "single_instance_assignment" {
  description = "The amount of undelivered messages a single instance can handle. Used by the autoscaler to determine the number of instances needed based on the total number of undelivered messages."
  type        = number
  default     = null
}

variable "mig_service_account_name" {
  description = "IAM `google_service_account.name`."
  type        = string
  nullable    = false
}

variable "mig_distribution_policy_zones" {
  description = "Availability zones for MIG"
  type        = list(string)
}

variable "secrets_to_access" {
  description = "List of secrets to access from the VM"
  type = list(object({
    secret_id  = string
    version    = string
  }))
  default = []
}

variable "subscription_id" {
  description = "The ID of the Pub/Sub subscription to which the service account will be granted access."
  type        = string
  default     = null
}

variable "managed_instance_group_name" {
  description = "The name of the Managed Instance Group."
  type        = string
  nullable    = false
}

variable "max_replicas" {
  description = "The maximum number of instances that can be deployed in this Managed Instance Group (MIG). This defines the upper limit for autoscaling."
  type        = number
  nullable    = false
}

variable "min_replicas" {
  description = "The minimum number of instances that must always be running in this Managed Instance Group (MIG), even during low-load periods."
  type        = number
  nullable    = false
}

variable "machine_type" {
  description = "The machine type to create."
  type        = string
  nullable    = false
}

variable "docker_image" {
  description = "The docker image to be deployed."
  type        = string
  nullable    = false
}

variable "terraform_service_account" {
  description = "Service account used by terraform that needs to attach the MIG service account to the VM."
  type        = string
  nullable    = false
}

variable "tee_cmd" {
  description = "The list of flags and values for the TEE application"
  type        = list(string)
}

variable "disk_image_family" {
  description = "The boot disk image family."
  type        = string
  nullable    = false
}

variable "config_storage_bucket" {
  description = "Configuration storage bucket."
  type        = string
  default     = null
}

variable "subnetwork_name" {
  description = "The name of the subnetwork for the MIG instances."
  type        = string
  nullable    = false
}

variable "tee_signed_image_repo" {
  description = "Trusted container image repository for Confidential Space attestation."
  type        = string
}

variable "java_tool_options" {
  description = "Java tool options to be passed to the TEE container via JAVA_TOOL_OPTIONS environment variable (e.g., '-Xmx96G' for heap size based on machine type)."
  type        = string
  default     = null
}

variable "extra_metadata" {
  description = "Extra metadata for the instance template."
  type        = map(string)
  default     = {}
}
