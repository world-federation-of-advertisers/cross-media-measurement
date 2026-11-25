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

resource "google_service_account" "attested_workload" {
  account_id   = var.service_account_name
  display_name = var.service_account_display_name
  description  = "Service account for attested Confidential Space workloads"
}

resource "google_iam_workload_identity_pool" "pool" {
  workload_identity_pool_id = var.workload_identity_pool_id
  display_name              = var.wip_display_name
  description               = "EDP workload identity pool."
}

resource "google_iam_workload_identity_pool_provider" "oidc" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.pool.workload_identity_pool_id
  workload_identity_pool_provider_id = var.workload_identity_pool_provider_id
  display_name                       = var.workload_identity_pool_provider_id
  attribute_mapping                  = { "google.subject" = "assertion.sub" }
  attribute_condition                = var.attribute_condition

  oidc {
    issuer_uri        = "https://confidentialcomputing.googleapis.com/"
    allowed_audiences = var.allowed_audiences
  }
}

resource "google_service_account_iam_member" "federated_access" {
  service_account_id = google_service_account.attested_workload.name
  role               = var.federated_service_account_role
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.pool.name}/*"
}

resource "google_service_account_iam_member" "self_id_token" {
  count              = var.federated_service_account_role == "roles/iam.serviceAccountTokenCreator" ? 1 : 0
  service_account_id = google_service_account.attested_workload.name
  role               = "roles/iam.serviceAccountOpenIdTokenCreator"
  member             = "serviceAccount:${google_service_account.attested_workload.email}"
}
