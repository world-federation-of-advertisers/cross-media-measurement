# Copyright 2022 The Cross-Media Measurement Authors
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

resource "null_resource" "configure_local_k8s_context" {
  provisioner "local-exec" {
    command = "aws eks update-kubeconfig --region ${data.aws_region.current.name} --name ${var.cluster_name}"
  }
}

resource "null_resource" "collect_k8s_secrets" {
  count = var.use_test_secrets ? 0 : 1
  provisioner "local-exec" {
    command = <<EOF
cp -r ../k8s/testing/secretfiles ${var.path_to_secrets}

cat ${var.path_to_secrets}/*_root.pem > ${var.path_to_secrets}/all_root_certs.pem
    EOF
  }

  provisioner "local-exec" {
    command = <<EOF
echo "secretGenerator:" > ${var.path_to_secrets}/kustomization.yaml
echo "- name: certs-and-configs" >> ${var.path_to_secrets}/kustomization.yaml
echo "  Files:" >> ${var.path_to_secrets}/kustomization.yaml
echo "  - trusted_certs.pem" >> ${var.path_to_secrets}/kustomization.yaml
echo "  - edp1_tls.pem" >> ${var.path_to_secrets}/kustomization.yaml
echo "  - edp1_tls.key" >> ${var.path_to_secrets}/kustomization.yaml

aws eks update-kubeconfig --region ${data.aws_region.current.name} --name tftest-cluster

kubectl apply -k ${var.path_to_secrets}
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
     command = "aws ecr get-login-password --region ${data.aws_region.current.name} | docker login --username AWS --password-stdin ${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com"
  }

  # update the CUE file to have the right AWS KMS key and CA arn
  provisioner "local-exec" {
    command = <<EOF
sed -i 's|#ContainerRegistryPrefix: "{account_id}.dkr.ecr.{region}.amazonaws.com"|#ContainerRegistryPrefix: "${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com"|' ${var.path_to_edp_cue_base}
sed -i 's|tinkKeyUri: ""|tinkKeyUri: "aws-kms://arn:aws:kms:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:key/${var.kms_key_id}"|' ${var.path_to_edp_cue}
sed -i 's|"--certificate-authority-arn="|"--certificate-authority-arn=${var.ca_arn}"|' ${var.path_to_edp_cue_base}
    EOF
  }

  # build and push the Docker image to ECR
  provisioner "local-exec" {
    working_dir = "../../../"
    command = "bazel run src/main/docker/${var.image_name} -c opt --define container_registry=${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com"
  }

  # create a k8s service account
  provisioner "local-exec" {
    command = "kubectl create serviceaccount ${var.k8s_account_service_name}"
  }

  # build and apply secrets
  provisioner "local-exec" {
    command = <<EOF
if [[ var.use_test_secrets -eq 1 ]]
then
  str=$(kubectl apply -k ${var.path_to_secrets})
else
  str=$(bazel run //src/main/k8s/testing/secretfiles:apply_kustomization)
fi

regex="(certs-and-configs-\S*)"
[[ $str =~ $regex ]]
secret_name=$${BASH_REMATCH[0]}

bazel build //src/main/k8s/dev:${var.build_target_name} --define=edp_name=dataProviders/c-8OD6eW4x8 --define=edp_k8s_secret_name=$secret_name

kubectl apply -f ../../../bazel-bin/src/main/k8s/dev/${var.manifest_name}
    EOF
    interpreter = ["bash", "-c"]
  }
}
