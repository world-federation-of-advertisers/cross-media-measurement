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

variable "workload_service_account_names" {
  type     = set(string)
  nullable = false
  validation {
    condition     = length(var.workload_service_account_names) > 0
    error_message = "Specify the approved workload service accounts."
  }
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
