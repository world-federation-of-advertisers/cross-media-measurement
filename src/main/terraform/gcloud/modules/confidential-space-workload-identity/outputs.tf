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

output "service_account_email" {
  description = "Federated service account for the EDP KMS configuration."
  value       = google_service_account.attested_workload.email
}

output "service_account_unique_id" {
  description = "Numeric service account ID used as the AWS bridge audience and subject."
  value       = google_service_account.attested_workload.unique_id
}

output "workload_identity_provider_name" {
  description = "Full workload identity provider resource name."
  value       = google_iam_workload_identity_pool_provider.oidc.name
}

output "kms_audience" {
  description = "Audience for the Halo Google Cloud federation configuration."
  value       = "//iam.googleapis.com/${google_iam_workload_identity_pool_provider.oidc.name}"
}
