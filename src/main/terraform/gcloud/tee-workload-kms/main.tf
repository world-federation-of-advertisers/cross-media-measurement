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

terraform {
  required_version = ">= 1.7"
  backend "gcs" { prefix = "terraform/state/edp-kms/workloads" }
  required_providers {
    google = { source = "hashicorp/google", version = "~> 7.0" }
    aws    = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}
provider "google" { project = var.google_project_id }
provider "aws" {
  region              = var.aws_region
  allowed_account_ids = [var.aws_account_id]
}
variable "google_project_id" { type = string }
variable "google_key_ring_id" { type = string }
variable "google_input_key_id" { type = string }
variable "aws_account_id" { type = string }
variable "aws_region" { type = string }
variable "aws_input_key_arn" { type = string }
variable "environments" {
  type = map(object({
    project_id = string
    jobs = map(object({
      launch_service_account_name  = string
      image_repositories           = set(string)
      image_signature_fingerprints = set(string)
      key_permissions              = map(set(string))
    }))
  }))
}
locals {
  purposes     = toset(["subpool_map", "rank_index", "labeled_impressions", "trustee_frequency_vector"])
  google_roles = { encrypt = "roles/cloudkms.cryptoKeyEncrypter", decrypt = "roles/cloudkms.cryptoKeyDecrypter" }
  aws_actions  = { encrypt = "kms:Encrypt", decrypt = "kms:Decrypt" }
}
module "environment" {
  for_each               = var.environments
  source                 = "../modules/tee-workload-kms"
  aws_account_id         = var.aws_account_id
  key_administrator_arns = ["arn:aws:iam::${var.aws_account_id}:root"]
  workloads = { for job, w in each.value.jobs : job => {
    service_account_name         = "tee-${each.key}-${job}"
    workload_identity_pool_id    = "tee-${each.key}-${job}"
    workload_project_ids         = [each.value.project_id]
    workload_service_accounts    = ["${w.launch_service_account_name}@${each.value.project_id}.iam.gserviceaccount.com"]
    image_signature_fingerprints = w.image_signature_fingerprints
    image_repositories           = w.image_repositories
    aws_role_name                = "tee-${each.key}-${job}"
    google_key_permissions       = { for purpose, ops in w.key_permissions : purpose => [for op in ops : local.google_roles[op]] }
    aws_key_permissions          = { for purpose, ops in w.key_permissions : purpose => concat([for op in ops : local.aws_actions[op]], contains(ops, "encrypt") ? ["kms:GenerateDataKey"] : []) }
  } }
  google_keys = merge({ raw_input = { key_ring_id = var.google_key_ring_id, key_name = "", existing_key_id = var.google_input_key_id, rotation_period = null } }, { for purpose in local.purposes : purpose => {
    key_ring_id     = var.google_key_ring_id
    key_name        = "tee-${each.key}-${replace(purpose, "_", "-")}"
    existing_key_id = null
    rotation_period = "2592000s"
  } })
  aws_keys = merge({ raw_input = { alias = "", description = "", existing_key_arn = var.aws_input_key_arn, enable_key_rotation = null, deletion_window_in_days = null } }, { for purpose in local.purposes : purpose => {
    alias                   = "alias/tee-${each.key}-${replace(purpose, "_", "-")}"
    description             = "${each.key} ${purpose} envelope encryption"
    existing_key_arn        = null
    enable_key_rotation     = true
    deletion_window_in_days = 30
  } })
}
output "environments" { value = { for env, m in module.environment : env => { workloads = m.workloads, google_kek_uris = m.google_kek_uris, aws_kek_uris = m.aws_kek_uris } } }
