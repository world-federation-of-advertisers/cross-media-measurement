# Copyright 2020 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

data "google_client_config" "current" {}
data "google_project" "project" {}

variable "cluster_info" {
  type = object({
    account_name = string
    service_account_name = string
    primary_name = string
  })
  description = "Attributes for generating the k8s cluster"
}

variable "db_info" {
  type = object({
    instance_name = string
    db_name = string
  })
  description = "Attributes for generating the spanner db"
}

variable "path_to_cmm" {
  type = string
  description = "relative path to cross-media-management repo"
}

variable "image_paths" {
  type = list(string)
  default = [
    "src/main/docker/push_kingdom_data_server_image",
    "src/main/docker/push_kingdom_system_api_server_image",
    "src/main/docker/push_kingdom_v2alpha_public_api_server_image"
  ]
  description = "list of paths for bazel images to build and push to gcp container registry"
}

variable "kms_data" {
  type = object({
    key_ring_name = string
    key_ring_exists = bool
    key_id = string
    key_exists = bool
  })
  description = "key ring metadata and booleans if they exist or should be created"
}

