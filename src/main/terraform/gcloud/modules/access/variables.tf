# Copyright 2024 The Cross-Media Measurement Authors
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
  default     = "access"
  nullable    = false
}

variable "api_server_ip_address" {
  description = "IP address for public API server"
  type        = string
  nullable    = true
  default     = null
}
