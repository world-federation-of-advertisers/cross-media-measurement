# Copyright 2023 The Cross-Media Measurement Authors
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

locals {
  workload_pool       = "${data.google_project.project.name}.svc.id.goog"
  member              = "serviceAccount:${local.workload_pool}[${var.cluster_namespace}/${var.k8s_service_account_name}]"
  iam_service_account = try(google_service_account.iam[0], var.iam_service_account)
}

data "google_project" "project" {}

resource "google_service_account" "iam" {
  count = var.iam_service_account == null ? 1 : 0

  account_id  = var.iam_service_account_name
  description = var.iam_service_account_description
}

resource "kubernetes_service_account" "k8s" {
  metadata {
    name = var.k8s_service_account_name
    annotations = {
      "iam.gke.io/gcp-service-account" : local.iam_service_account.email
    }
    namespace = var.cluster_namespace
  }
}

resource "google_service_account_iam_member" "workload_identity_user" {
  service_account_id = local.iam_service_account.name
  role               = "roles/iam.workloadIdentityUser"
  member             = local.member
}
