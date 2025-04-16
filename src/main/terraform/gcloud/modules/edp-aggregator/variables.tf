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

variable "queue_configs" {
  description = "Naming config for the PubSub and related MIG"
  type = map(object({
    instance_template_name      = string
    base_instance_name          = string
    managed_instance_group_name = string
    subscription_name           = string
    topic_name                  = string
    mig_service_account_name    = string
    single_instance_assignment  = number
    min_replicas                = number
    max_replicas                = number
    app_args                    = list(string)
    machine_type                = string
    kms_key_id                  = string
    docker_image                = string
  }))
}

variable "artifacts_registry_repo_name" {
  description = "The name of Artifact Registry where approved TEE app are stored."
  type        = string
  nullable    = false
}

variable "pubsub_iam_service_account_member" {
  description = "IAM `google_service_account` for public api to access pubsub."
  type        = string
  default     = "secure-computation-pubsub"
  nullable    = false
}