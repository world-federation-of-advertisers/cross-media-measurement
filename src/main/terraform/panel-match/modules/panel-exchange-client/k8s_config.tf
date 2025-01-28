# Copyright 2022 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#                     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

resource "null_resource" "configure_local_k8s_context" {
  provisioner "local-exec" {
    command = <<EOF
aws eks update-kubeconfig --region ${data.aws_region.current.name} --name ${aws_eks_cluster.cluster.name}
sleep 15
    EOF
  }
}

resource "null_resource" "collect_k8s_secrets" {
  provisioner "local-exec" {
    command = <<EOF
cat ${var.path_to_secrets}/*_root.pem > ${var.path_to_secrets}/trusted_certs.pem
    EOF
  }

  provisioner "local-exec" {
    command = <<EOF
echo "secretGenerator:" > ${var.path_to_secrets}/kustomization.yaml
echo "- name: certs-and-configs" >> ${var.path_to_secrets}/kustomization.yaml
echo "  Files:" >> ${var.path_to_secrets}/kustomization.yaml
echo "  - trusted_certs.pem" >> ${var.path_to_secrets}/kustomization.yaml
    EOF
  }
}

resource "null_resource" "configure_cluster" {
  depends_on = [
    aws_ecr_repository.edp_image,
    null_resource.collect_k8s_secrets
  ]

  # login to Docker on AWS
  provisioner "local-exec" {
    command = <<EOF
aws ecr get-login-password --region ${data.aws_region.current.name} | \
  docker login --username AWS --password-stdin ${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com
    EOF
  }

  # update the CUE file to have the right AWS KMS key and CA arn
  provisioner "local-exec" {
    command = <<EOF
sed -i -E 's|serviceAccountName: ".*"|serviceAccountName: "${var.k8s_account_service_name}"|' ${var.path_to_cue}
sed -i -E 's|containerPrefix: ".*"|containerPrefix: "${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com"|' ${var.path_to_cue}
sed -i -E 's|tinkKeyUri: ".*"|tinkKeyUri: "${aws_kms_key.k8s_key.arn}"|' ${var.path_to_cue}
sed -i -E 's|region: ".*"|region: "${data.aws_region.current.name}"|' ${var.path_to_cue}
sed -i -E 's|storageBucket: ".*"|storageBucket: "hello-storage"|' ${var.path_to_cue}
sed -i -E 's|kingdomApi: ".*"|kingdomApi: "${var.kingdom_endpoint}"|' ${var.path_to_cue}
sed -i -E 's|certArn: ".*"|certArn: "${aws_acmpca_certificate_authority.root_ca.arn}"|' ${var.path_to_cue}
sed -i -E 's|commonName: ".*"|commonName: "${var.ca_common_name}"|' ${var.path_to_cue}
sed -i -E 's|orgName: ".*"|orgName: "${var.ca_org_name}"|' ${var.path_to_cue}
sed -i -E 's|dns: ".*"|dns: "${var.ca_dns}"|' ${var.path_to_cue}
    EOF
  }

  # update re-config script
  provisioner "local-exec" {
    command = <<EOF
sed -i -E 's|cert_arn=".*"|cert_arn="${aws_acmpca_certificate_authority.root_ca.arn}"|' cert_helper.sh
sed -i -E 's|path_to_cue_file_wext=".*"|path_to_cue_file_wext="${var.path_to_cue}"|' cert_helper.sh
sed -i -E 's|path_to_secrets=".*"|path_to_secrets="${var.path_to_secrets}"|' cert_helper.sh
    EOF
  }

  # build and push the Docker image to ECR
  provisioner "local-exec" {
    working_dir = "../../../"
    command     = "bazel run src/main/docker/${var.image_name} -c opt --define container_registry=${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com"
  }

  # create a k8s service account
  provisioner "local-exec" {
    command = "kubectl create serviceaccount ${var.k8s_account_service_name}"
  }

  # build and apply secrets
  provisioner "local-exec" {
    command     = <<EOF
str=$(kubectl apply -k ${var.path_to_secrets})

regex="(certs-and-configs-\S*)"
[[ $str =~ $regex ]]
secret_name=$${BASH_REMATCH[0]}

bazel build //src/main/k8s/dev:${var.build_target_name} --define=mp_name=dataProviders/c-8OD6eW4x8 --define=mp_k8s_secret_name=$secret_name

kubectl apply -f ../../../bazel-bin/src/main/k8s/dev/${var.manifest_name}
    EOF
    interpreter = ["bash", "-c"]
  }
}
