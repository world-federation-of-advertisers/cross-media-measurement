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

variable "workloads" {
  description = "Job identities. Each pool admits one job, with explicit launch identity, signer and repository constraints."
  type = map(object({
    service_account_name         = string
    workload_identity_pool_id    = string
    workload_project_ids         = set(string)
    workload_service_accounts    = set(string)
    image_signature_fingerprints = set(string)
    image_repositories           = set(string)
    aws_role_name                = string
    google_key_permissions       = map(set(string))
    aws_key_permissions          = map(set(string))
  }))
  nullable = false
  validation {
    condition = length(var.workloads) > 0 && alltrue([for w in values(var.workloads) :
      length(w.workload_project_ids) > 0 && length(w.workload_service_accounts) > 0 &&
      length(w.image_signature_fingerprints) > 0 && length(w.image_repositories) > 0 &&
      alltrue([for fp in w.image_signature_fingerprints : can(regex("^ECDSA_P256_SHA256:[0-9a-f]{64}$", fp))]) &&
      alltrue([for repo in w.image_repositories : can(regex("^[a-z0-9][a-z0-9.-]*(:[0-9]+)?/[a-z0-9][a-z0-9._/-]*[a-z0-9]$", repo)) && !strcontains(repo, "//")])
    ])
    error_message = "Specify projects, launch accounts, ECDSA DER fingerprints, and untagged image repositories for every job."
  }
  validation {
    condition     = length(distinct([for w in values(var.workloads) : w.service_account_name])) == length(var.workloads) && length(distinct([for w in values(var.workloads) : w.workload_identity_pool_id])) == length(var.workloads) && length(distinct([for w in values(var.workloads) : w.aws_role_name])) == length(var.workloads)
    error_message = "Each job must have its own service account, pool and AWS role."
  }
  validation {
    condition = alltrue(flatten([for a, w in var.workloads : [for b, other in var.workloads :
      a == b || length(setintersection(w.image_signature_fingerprints, other.image_signature_fingerprints)) == 0
    ]]))
    error_message = "Different jobs must not share workload signing keys."
  }
  validation {
    condition = alltrue(flatten([for w in values(var.workloads) : [for permissions in values(w.google_key_permissions) :
      length(permissions) > 0 && alltrue([for role in permissions : contains(["roles/cloudkms.cryptoKeyEncrypter", "roles/cloudkms.cryptoKeyDecrypter"], role)])
      ]])) && alltrue(flatten([for w in values(var.workloads) : [for permissions in values(w.aws_key_permissions) :
      length(permissions) > 0 && alltrue([for action in permissions : contains(["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey"], action)])
    ]]))
    error_message = "Grant only explicit data operations to workloads."
  }
}

variable "google_keys" {
  description = "Purpose-specific Google KEKs. existing_key_id adopts access grants without owning or replacing the existing key."
  type        = map(object({ key_ring_id = string, key_name = string, existing_key_id = string, rotation_period = string }))
  nullable    = false
}
variable "aws_keys" {
  description = "Purpose-specific AWS KEKs. existing_key_arn is null for new keys."
  type        = map(object({ alias = string, description = string, existing_key_arn = string, enable_key_rotation = bool, deletion_window_in_days = number }))
  nullable    = false
}
variable "aws_account_id" {
  description = "AWS account that owns the KEKs."
  type        = string
  nullable    = false
}
variable "key_administrator_arns" {
  description = "Explicit administrators of newly created AWS KEKs."
  type        = set(string)
  nullable    = false
  validation {
    condition     = length(var.key_administrator_arns) > 0
    error_message = "Specify at least one key administrator."
  }
}

variable "google_project_id" { type = string }
variable "aws_region" { type = string }
