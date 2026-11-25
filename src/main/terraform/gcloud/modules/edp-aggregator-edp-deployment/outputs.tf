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
  description = "Tink URI of the EDP-owned key."
  value       = "gcp-kms://${google_kms_crypto_key.edp_kek.id}"
}

output "crypto_key_id" {
  description = "KMS crypto key resource name for additional Terraform IAM grants."
  value       = google_kms_crypto_key.edp_kek.id
}

output "key_ring_id" {
  description = "Key ring for an optional separate TrusTEE re-encryption key."
  value       = google_kms_key_ring.edp_key_ring.id
}

output "service_account_email" {
  description = "Federated service account email for Halo."
  value       = module.workload_identity.service_account_email
}

output "workload_identity_provider_name" {
  description = "Full workload identity provider resource name."
  value       = module.workload_identity.workload_identity_provider_name
}

output "kms_audience" {
  description = "Audience for Halo's Google Cloud KMS configuration."
  value       = module.workload_identity.kms_audience
}
