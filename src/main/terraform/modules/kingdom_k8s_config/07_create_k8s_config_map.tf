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

# Configmap is initially empty
resource "null_resource" "create_k8s_config_map" {
  provisioner "local-exec" {
    command = <<EOF
      touch ${var.path_to_configmap}
      kubectl create configmap config-files --from-file=${var.path_to_configmap}
      rm ${var.path_to_configmap}
    EOF
  }
}
