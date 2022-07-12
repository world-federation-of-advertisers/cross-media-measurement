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

module "duchy_k8s_config" {
  source = "../modules/duchy_k8s_config"

  path_to_cmm = "../../cross-media-measurement"
  path_to_secrets = "~/TFTest/secrets"
  path_to_configmap = "./authority_key_identifier_to_principal_map.textproto"
  manifest_info = {
    cert_id = "SVVse4xWHL0"
    bucket_id = "tf-test-duchy-bucket"
  }
}
