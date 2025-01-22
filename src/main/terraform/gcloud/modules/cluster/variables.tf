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
  description = "Name of the cluster."
  type        = string
  nullable    = false
}

variable "location" {
  description = "Location of cluster."
  type        = string
}

variable "secret_key" {
  description = "`google_kms_crypto_key` for cluster secret encryption."
  type = object({
    name = string
    id   = string
  })
}

variable "release_channel" {
  description = "`release_channel.channel` for the cluster"
  type        = string
  nullable    = false
}

variable "autoscaling_profile" {
  description = "Autoscaling profile"
  type        = string
  default     = null
}

variable "deletion_protection" {
  description = "Whether deletion protection is enabled"
  type        = bool
  default     = null
}
