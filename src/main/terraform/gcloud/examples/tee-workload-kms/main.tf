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
module "workload_kms" {
  source                 = "../../modules/tee-workload-kms"
  workloads              = var.workloads
  google_keys            = var.google_keys
  aws_keys               = var.aws_keys
  aws_account_id         = var.aws_account_id
  key_administrator_arns = var.key_administrator_arns
}
output "workloads" { value = module.workload_kms.workloads }
output "google_kek_uris" { value = module.workload_kms.google_kek_uris }
output "aws_kek_uris" { value = module.workload_kms.aws_kek_uris }
output "attribute_conditions" { value = module.workload_kms.attribute_conditions }
