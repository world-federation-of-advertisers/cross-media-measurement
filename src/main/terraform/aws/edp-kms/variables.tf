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
variable "region" {
  type     = string
  nullable = false
}
variable "role_name" {
  type     = string
  nullable = false
}
variable "audience" {
  type     = string
  nullable = false
}
variable "key_alias" {
  type     = string
  nullable = false
}
variable "oidc_thumbprints" {
  type     = list(string)
  nullable = false
}
variable "workload_project_ids" {
  type     = set(string)
  nullable = false
}
variable "image_signature_key_ids" {
  type     = set(string)
  nullable = false
  validation {
    condition     = length(var.image_signature_key_ids) > 0 && alltrue([for key_id in var.image_signature_key_ids : can(regex("^[0-9a-f]{64}$", key_id))])
    error_message = "Specify approved SHA-256 image-signing key IDs."
  }
}
variable "export_role_name" {
  type     = string
  nullable = false
}
variable "export_google_subjects" {
  type     = set(string)
  nullable = false
}

variable "legacy_federation_enabled" {
  description = "Retain legacy shared federation during the staged per-job rollout."
  type        = bool
  nullable    = false
}
