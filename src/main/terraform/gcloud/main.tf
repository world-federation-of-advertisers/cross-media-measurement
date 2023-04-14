
# Copyright 2023 The Cross-Media Measurement Authors
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

provider "google" {
  project = var.project
}

terraform {
  backend "gcs" {
    bucket  = "terraform-state-halo-cmm"
    prefix  = "cmm"
  }
}

module "kingdom" {
  source = "./kingdom"
  env = var.env
  project = var.project
  service_account = var.service_account
}

module "worker-1" {
  source = "./duchy"
  env = var.env
  project = var.project
  component = "worker1"
}

module "worker-2" {
  source = "./duchy"
  env = var.env
  project = var.project
  component = "worker2"
}

module "worker-3" {
  source = "./duchy"
  env = var.env
  project = var.project
  component = "aggregator"
}
