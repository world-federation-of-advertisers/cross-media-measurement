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

resource "google_iam_workload_identity_pool_provider" "deployment" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.deployment.workload_identity_pool_id
  workload_identity_pool_provider_id = "edp-kms"
  attribute_mapping                  = { "google.subject" = "assertion.sub", "attribute.repository" = "assertion.repository" }
  attribute_condition                = "assertion.repository == 'world-federation-of-advertisers/cross-media-measurement' && ((has(assertion.job_workflow_ref) && assertion.job_workflow_ref == 'world-federation-of-advertisers/cross-media-measurement/.github/workflows/deploy-edp-kms.yml@refs/heads/main') || (!has(assertion.job_workflow_ref) && assertion.workflow_ref == 'world-federation-of-advertisers/cross-media-measurement/.github/workflows/deploy-edp-kms.yml@refs/heads/main' && assertion.ref == 'refs/heads/main'))"
  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account_iam_member" "deployment_federation" {
  service_account_id = "projects/${local.project_id}/serviceAccounts/terraform@${local.project_id}.iam.gserviceaccount.com"
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/projects/${var.project_number}/locations/global/workloadIdentityPools/edp-kms-deployment/attribute.repository/world-federation-of-advertisers/cross-media-measurement"
  depends_on         = [google_iam_workload_identity_pool_provider.deployment]
}

resource "google_service_account_iam_member" "deployment_account_admin" {
  for_each = toset(["terraform", var.service_account_name])

  service_account_id = "projects/${local.project_id}/serviceAccounts/${each.value}@${local.project_id}.iam.gserviceaccount.com"
  role               = "roles/iam.serviceAccountAdmin"
  member             = "serviceAccount:terraform@${local.project_id}.iam.gserviceaccount.com"
}

resource "google_iam_workload_identity_pool" "deployment" {
  workload_identity_pool_id = "edp-kms-deployment"
  display_name              = "EDP KMS deployment"
}

resource "google_project_iam_member" "deployment_job_identity_admin" {
  project = local.project_id
  role    = "roles/iam.serviceAccountAdmin"
  member  = "serviceAccount:terraform@${local.project_id}.iam.gserviceaccount.com"
}
