# Copyright 2020 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
/*

*/
locals {
  project = "halo"
  tags = {
    Environment     = var.env
  }
  prefix      = "${var.env}-${local.project}"
  prefix_path = "${var.env}/${local.project}"
}

variable projectName {
  type = string
  default = "halo"
  description = "Infrastructure as Code for Halo "
}

variable node_version {
  type = string
  default = "1.10.6-gke.11"
  description = "version of the GKE cluster"
}

variable min_master_version {
  type = string
  default = "1.10.9-gke.5"
  description = "version of the GKE cluster"
}

variable gke_service_account_name {
  type = string
  default = "gke-cluster"
  description = "gke service account name"
}

variable db_user {
  type = string
  default = "Admin"
  description = "DB user name"
}

variable db_password {
  type = string
  default = "test"
  description = "password"
}

variable env {
  type = string
  default = "dev"
  description = "Represents the environment used."
}

variable region {
  type = string
  default = "us-central1"
  description = "Represents the environment used."
}

variable project {
  type = string
  default = "halo-cmm-sandbox"
  description = "The project ID"
}

variable "project_id" {
  type = string
  default = "halo-cmm-sandbox"
  description = "Project ID"
}
variable "zone" {
  type = string
  default = "us-east1-b"
  description = "zone for resoruces"
}
variable "ring_name" {
  type = string
  default = "kingdom-key-ring-2"
  description = "KMS key ring name"
}
variable "ring_location" {
  type = string
  default ="us-east1"
  description = "Key ring location "
}



