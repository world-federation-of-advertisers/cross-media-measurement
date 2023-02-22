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

module "kingdom" {
  source = "../modules/kingdom"
  providers = {
    google = google.kingdom
  }

  cluster_info = {
    account_name = "tf-test-gke-cluster"
    service_account_name = "tf-test-internal-server2"
    primary_name = "tf-test-halo-cmm-kingdom-demo-cluster"
  }
  db_info = {
    instance_name = "tf-test-dev-instance"
    db_name = "tf-test-kingdom"
  }

  kms_data = {
    key_ring_name = "tf-test-key-ring"
    key_ring_exists = true
    key_id = "tf-test-k8s-secret-2"
    key_exists = true
  }
}

