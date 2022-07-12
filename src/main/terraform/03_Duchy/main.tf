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

module "duchy" {
  source = "../modules/duchy"
  providers = {
    google = google.duchy
  }

  cluster_info = {
    account_name = "tf-test-gke-duchy-cluster"
    service_account_name = "tf-test-gke-duchy-cluster"
    primary_name = "tf-test-halo-cmm-dev-worker1-duchy"
  }
  db_info = {
    instance_name = "tf-test-dev-duchy-instance"
    db_name = "tf-test-duchy-kingdom"
  }
  path_to_cmm = "../../cross-media-measurement"

  bucket_name = "tf-test-duchy-bucket"

  image_paths = [
    "src/main/docker/push_duchy_async_computation_control_server_image",
    "src/main/docker/push_duchy_computation_control_server_image",
    "src/main/docker/push_duchy_requisition_fulfillment_server_image",
    "src/main/docker/push_duchy_spanner_computations_server_image",
    "src/main/docker/push_duchy_herald_daemon_image",
    "src/main/docker/push_duchy_liquid_legions_v2_mill_daemon_image"
  ]

  kms_data = {
    key_ring_name = "tf-test-key-ring"
    key_ring_exists = true
    key_id = "tf-test-k8s-secret-2"
    key_exists = true
  }
}
