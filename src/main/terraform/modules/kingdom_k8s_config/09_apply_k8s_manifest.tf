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

resource "null_resource" "apply_k8s_manifest" {
  depends_on = [
    null_resource.create_k8s_manifest
  ]

  provisioner "local-exec" {
    command = "kubectl apply -f ${var.path_to_cmm}/bazel-bin/src/main/k8s/dev/kingdom_gke.yaml"
  }
}
