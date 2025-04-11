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

variable "secure_computation_api_server_ip_address" {
  description = "IP address for public API server"
  type        = string
  nullable    = true
  default     = null
}

variable "iam_service_account_name" {
  description = "IAM `google_service_account.name`."
  type        = string
  default     = "secure-computation-internal"
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
  description = "Name of the Spanner database."
  type        = string
  default     = "secure-computation"
  nullable    = false
}

variable "secure_computation_bucket_name" {
  description = "Name of the Google Cloud Storage bucket used by the SecureComputation module."
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

variable "requisition_fulfiller_topic_name" {
  description = "Name of the Pub/Sub topic"
  type        = string
  nullable    = false
}

variable "requisition_fulfiller_subscription_name" {
  description = "Name of the Pub/Sub subscription"
  type        = string
  nullable    = false
}

variable "ack_deadline_seconds" {
  description = "The time (in seconds) allowed for subscribers to acknowledge messages. If the acknowledgment period is not extended or the message is not acknowledged within this time, the message will be re-delivered."
  type        = number
  default     = null
}

variable "single_instance_assignment" {
  description = "The amount of undelivered messages a single instance can handle. Used by the autoscaler to determine the number of instances needed based on the total number of undelivered messages."
  type        = number
  nullable    = false
}

variable "mig_service_account_name" {
  description = "IAM `google_service_account.name`."
  type        = string
  nullable    = false
}

variable "app_args" {
  description = "Arguments to pass to the application"
  type        = list(string)
  default     = []
}

variable "subscription_id" {
  description = "The subscription used to determine the amount of undelivered messages."
  type        = number
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

variable "subscription_id" {
  description = "The pubsub subscription id to grant access to."
  type        = string
  nullable    = false
}

variable "kms_key_id" {
  description = "The kms key id to grant access to."
  type        = string
  nullable    = false
}

variable "mig_names" {
  description = "Naming config for the MIG"
  type = map(object({
    instance_template_name      = string
    base_instance_name          = string
    managed_instance_group_name = string
    subscription_id             = string
    mig_service_account_name    = string
    single_instance_assignment  = number
    min_replicas                = number
    max_replicas                = number
    app_args                    = list(string)
    machine_type                = string
    kms_key_id                  = string
  }))
}

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