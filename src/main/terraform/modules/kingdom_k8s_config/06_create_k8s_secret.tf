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

resource "null_resource" "create_k8s_secrets" {
#  provisioner "local-exec" {
#    working_dir = var.path_to_cmm
#    command = <<EOF
#      bazel run //src/main/k8s/testing/secretfiles:apply_kustomization
#    EOF
#  }

  provisioner "local-exec" {
    command = <<EOF
      cp -r ${var.path_to_cmm}/src/main/k8s/testing/secretfiles ${var.path_to_secrets}

      cat ${var.path_to_secrets}/*_root.pem > ${var.path_to_secrets}/all_root_certs.pem

      echo "secretGenerator:" >> ${var.path_to_secrets}/kustomization.yaml
      echo "- name: certs-and-configs" >> ${var.path_to_secrets}/kustomization.yaml
      echo "  files:" >> ${var.path_to_secrets}/kustomization.yaml
      echo "  - all_root_certs.pem" >> ${var.path_to_secrets}/kustomization.yaml
      echo "  - kingdom_tls.key" >> ${var.path_to_secrets}/kustomization.yaml
      echo "  - kingdom_tls.pem" >> ${var.path_to_secrets}/kustomization.yaml
      echo "  - health_probe_tls.pem" >> ${var.path_to_secrets}/kustomization.yaml
      echo "  - health_probe_tls.key" >> ${var.path_to_secrets}/kustomization.yaml
      echo "  - duchy_cert_config.textproto" >> ${var.path_to_secrets}/kustomization.yaml
      echo "  - llv2_protocol_config_config.textproto" >> ${var.path_to_secrets}/kustomization.yaml

      kubectl apply -k ${var.path_to_secrets}
    EOF
  }
}

