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

variable "simulator_name" {
  description = "The unique name for the simulator, used as a prefix for all resources."
  type        = string
  nullable    = false
}

variable "key_ring_location" {
  description = "Location of the KMS key ring."
  type        = string
}

variable "rotation_period" {
  description = "Rotation period for the KMS crypto key."
  type        = string
  default     = "63072000s" # 2 years
}

variable "issuer_uri" {
  description = "The OIDC issuer URI for the Workload Identity Pool Provider."
  type        = string
  default     = "https://confidentialcomputing.googleapis.com"
}

variable "allowed_audiences" {
  description = "The allowed audiences for the OIDC provider."
  type        = list(string)
  default     = ["https://sts.googleapis.com"]
}

variable "tee_image_signature_fingerprint" {
  description = "The image signature fingerprint to enforce for the TEE workload. Format: 'ALGORITHM:KEY_ID'."
  type        = string
  nullable    = false
}
