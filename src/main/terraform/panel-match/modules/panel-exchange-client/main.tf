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

module "aws_eks_cluster" {
  source = "../eks"

  availability_zones_count = var.cluster_config.availability_zones_count
  project = var.cluster_config.project
  vpc_cidr = var.cluster_config.vpc_cidr
  subnet_cidr_bits = var.cluster_config.subnet_cidr_bits
}

module "docker_config" {
  source = "../eks_config"

  use_test_secrets = var.k8s_config.use_test_secrets
  image_name = var.k8s_config.image_name
  build_target_name = var.k8s_config.build_target_name
  manifest_name = var.k8s_config.manifest_name
  repository_name = var.k8s_config.repository_name
  path_to_secrets = var.k8s_config.path_to_secrets
  k8s_account_service_name = var.k8s_config.k8s_account_service_name
  cluster_name = module.aws_eks_cluster.cluster_name
  kms_key_id = aws_kms_key.k8s_key.key_id
  ca_arn = aws_acmpca_certificate_authority.subordinate.arn
}
