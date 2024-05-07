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
  description = "Name (external ID) of the Duchy."
  type        = string
  nullable    = false
}

variable "database_name" {
  description = "Name of the database. Defaults to <name>-duchy."
  type        = string
}

variable "storage_bucket" {
  description = "`google_storage_bucket` for the system."
  type = object({
    name = string
  })
}

variable "spanner_instance" {
  description = "`google_spanner_instance` for the system."
  type = object({
    name = string
  })
  nullable = false
}

variable "v2alpha_ip_address" {
  description = "IP address for v2alpha public API"
  type        = string
  nullable    = true
  default     = null
}

variable "system_v1alpha_ip_address" {
  description = "IP address for v1alpha system API"
  type        = string
  nullable    = true
  default     = null
}
