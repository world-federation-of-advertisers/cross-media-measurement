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

variable "key_ring_name" {
  description = "Name of the KMS key ring."
  type        = string
  nullable    = false
}

variable "key_ring_location" {
  description = "Location of the KMS key ring."
  type        = string
  nullable    = false
}

variable "kms_key_name" {
  description = "Name of the KMS KEK."
  type        = string
  nullable    = false
}

variable "queue_worker_configs" {
  description = "Combined config for each Pub/Sub queue and its corresponding MIG worker"
  type = map(object({
    queue = object({
      subscription_name     = string
      topic_name            = string
      ack_deadline_seconds  = number
    })
    worker = object({
      instance_template_name      = string
      base_instance_name          = string
      managed_instance_group_name = string
      mig_service_account_name    = string
      single_instance_assignment  = number
      min_replicas                = number
      max_replicas                = number
      app_args                    = list(string)
      machine_type                = string
      docker_image                = string
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

variable "edp_aggregator_bucket_location" {
  description = "Location of the Storage bucket used by the Edp Aggregator."
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

variable "data_watcher_private_key_id" {
  type        = string
  description = "The ID of data watcher private key"
}

variable "data_watcher_private_key_path" {
  type        = string
  description = "The path of the data watcher private key"
}