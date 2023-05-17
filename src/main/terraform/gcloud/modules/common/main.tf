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

data "google_project" "project" {}

resource "google_kms_key_ring" "key_ring" {
  name     = var.key_ring_name
  location = var.key_ring_location

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_kms_crypto_key" "k8s" {
  name     = "k8s-secret"
  key_ring = google_kms_key_ring.key_ring.id

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_kms_crypto_key_iam_member" "k8s_encrypter_decrypter" {
  crypto_key_id = google_kms_crypto_key.k8s.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.google_project.project.number}@container-engine-robot.iam.gserviceaccount.com"
}

resource "google_service_account" "gke_cluster" {
  account_id  = "gke-cluster"
  description = "Limited service account for GKE clusters, to be used in place of the Compute Engine default service account."
}

resource "google_project_iam_member" "node_service_account" {
  project = data.google_project.project.name
  role    = "roles/container.nodeServiceAccount"
  member  = google_service_account.gke_cluster.member
}
