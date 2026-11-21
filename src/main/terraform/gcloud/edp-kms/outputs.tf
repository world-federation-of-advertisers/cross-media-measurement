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

output "kek_uri" {
  value = module.edp_kms.kek_uri
}
output "trustee_kek_uri" {
  value = "gcp-kms://${google_kms_crypto_key.trustee.id}"
}
output "kms_audiences" {
  value = { for env, workload in var.workloads : env => "//iam.googleapis.com/${local.provider_prefix}/${workload.provider_id}" }
}
output "attribute_conditions" {
  value = local.attribute_conditions
}
