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
  description = "Name of the node pool."
  type        = string
}

variable "cluster" {
  description = "`google_container_cluster` that the node pool is for."
  type = object({
    id = string
  })
  nullable = false
}

variable "service_account" {
  description = "`google_service_account` for GKE clusters."
  type = object({
    email = string
  })
  nullable = false
}

variable "machine_type" {
  description = "Machine type of the node pool."
  type        = string
  nullable    = false
}

variable "max_node_count" {
  type     = number
  nullable = false
}

variable "spot" {
  description = "Whether the pool uses spot VMs. If true, a NoSchedule taint will be added."
  type        = bool
  default     = false
  nullable    = false
}
