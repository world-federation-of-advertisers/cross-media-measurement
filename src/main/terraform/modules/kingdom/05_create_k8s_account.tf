# Copyright 2020 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

resource "null_resource" "create_service_account" {
  depends_on = [
    google_container_cluster.primary_cluster
  ]

  provisioner "local-exec" {
    command = <<EOF
      gcloud container clusters get-credentials ${var.cluster_info.service_account_name} --region ${data.google_client_config.current.region}
      kubectl create serviceaccount ${var.cluster_info.account_name}
      gcloud iam service-accounts add-iam-policy-binding \
        ${google_service_account.cluster_service_account.email} \
        --role roles/iam.workloadIdentityUser \
        --member "serviceAccount:halo-cmm-dev.svc.id.goog[default/${var.cluster_info.service_account_name}]"
      kubectl annotate serviceaccount ${var.cluster_info.service_account_name} \
        iam.gke.io/gcp-service-account=${google_service_account.cluster_service_account.email}
    EOF
  }
}
