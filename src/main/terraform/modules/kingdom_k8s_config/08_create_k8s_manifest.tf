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

resource "null_resource" "create_k8s_manifest" {
  depends_on = [
    null_resource.create_k8s_secrets
  ]

  provisioner "local-exec" {
    working_dir = "${var.path_to_cmm}"
    command = <<EOF
      str=$(kubectl get secrets | grep "certs-and-configs-")
      regex="(certs-and-configs-\S*)"
      [[ $str =~ $regex ]]
      secret_name=$${BASH_REMATCH[0]}

      echo "command: bazel build //src/main/k8s/dev:kingdom_gke --define=k8s_kingdom_secret_name=$secret_name"      
      bazel build //src/main/k8s/dev:kingdom_gke --define=k8s_kingdom_secret_name=$secret_name
    EOF
    interpreter = ["bash", "-c"]
  }
}
