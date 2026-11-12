# Copyright 2026 The Cross-Media Measurement Authors
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

variable "input_kek_uri" {
  type     = string
  nullable = false
}
variable "project_number" {
  type     = string
  nullable = false
}
variable "trustee_key_name" {
  type     = string
  nullable = false
}
variable "service_account_name" {
  type     = string
  nullable = false
}
variable "workload_identity_pool_id" {
  type     = string
  nullable = false
}
variable "workload_service_account_names" {
  type     = set(string)
  nullable = false
  validation {
    condition     = length(var.workload_service_account_names) > 0
    error_message = "Specify the approved workload service accounts."
  }
}
variable "additional_input_key_members" {
  type     = set(string)
  nullable = false
}
variable "workloads" {
  type = map(object({
    project_id                   = string
    provider_id                  = string
    image_signature_fingerprints = set(string)
  }))
  nullable = false
  validation {
    condition = toset(keys(var.workloads)) == toset(["dev", "head", "qa"]) && alltrue([
      for workload in values(var.workloads) : length(workload.image_signature_fingerprints) > 0 && alltrue([
        for fingerprint in workload.image_signature_fingerprints : can(regex("^ECDSA_P256_SHA256:[0-9a-f]{64}$", fingerprint))
      ])
    ])
    error_message = "Configure dev, head, and qa with approved SHA-256 image-signing fingerprints."
  }
}

variable "legacy_federation_enabled" {
  description = "Retain legacy shared federation during the staged per-job rollout."
  type        = bool
  nullable    = false
}

variable "project_iam_admin_members" {
  description = "Authoritative project IAM administrators; runtime identities must not be members."
  type        = set(string)
  nullable    = false
  validation {
    condition     = length(var.project_iam_admin_members) > 0 && !contains(var.project_iam_admin_members, "serviceAccount:${var.service_account_name}@${split("/", trimprefix(var.input_kek_uri, "gcp-kms://"))[1]}.iam.gserviceaccount.com")
    error_message = "Retain trusted administrators and remove the shared runtime account's project IAM administration."
  }
}
