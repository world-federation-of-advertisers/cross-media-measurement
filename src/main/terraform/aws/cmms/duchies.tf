# Copyright 2023 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

data "aws_availability_zones" "available" {}
data "aws_caller_identity" "current" {}

module "clusters" {
  source = "../modules/eks-cluster"

  for_each = local.duchy_names

  aws_region               = var.aws_region
  cluster_name             = "${each.key}-duchy"
  cluster_version          = "1.29"
  kms_key_administrators   = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
  control_plane_subnet_ids = module.vpc.intra_subnets
  subnet_ids               = module.vpc.private_subnets
  vpc_id                   = module.vpc.vpc_id
  default_instance_types   = ["m5.large"]
  default_max_node_count   = 2
  high_perf_instance_types = ["c5.xlarge"]
  high_perf_max_node_count = 20
}

# IAM role to be used by aws_load_balancer_controller addons across all clusters
module "load_balancer_controller_irsa_role" {
  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"

  role_name                              = "load-balancer-controller"
  attach_load_balancer_controller_policy = true

  oidc_providers = {
    for k, cluster in module.clusters :
    k => {
      provider_arn = cluster.oidc_provider_arn
      namespace_service_accounts = [
        "kube-system:aws-load-balancer-controller"
      ]
    }
  }
}

provider "kubernetes" {
  alias                  = "worker2"
  host                   = module.clusters["worker2"].cluster_endpoint
  cluster_ca_certificate = base64decode(module.clusters["worker2"].cluster_certificate_authority_data)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.clusters["worker2"].cluster_name]
  }
}

provider "helm" {
  alias = "worker2"
  kubernetes {
    host                   = module.clusters["worker2"].cluster_endpoint
    cluster_ca_certificate = base64decode(module.clusters["worker2"].cluster_certificate_authority_data)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.clusters["worker2"].cluster_name]
    }
  }
}

module "worker2_cluster_addons" {
  source = "../modules/eks-cluster-addons"

  for_each = local.duchy_names

  providers = {
    kubernetes = kubernetes.worker2
    helm       = helm.worker2
  }

  aws_region                             = var.aws_region
  cluster_name                           = module.clusters[each.key].cluster_name
  eks_oidc_provider_arn                  = module.clusters[each.key].oidc_provider_arn
  eks_oidc_provider                      = module.clusters[each.key].oidc_provider
  load_balancer_controller_irsa_role_arn = module.load_balancer_controller_irsa_role.iam_role_arn
  vpc_id                                 = module.vpc.vpc_id
}

module "worker2_duchy" {
  source = "../modules/duchy"
  providers = {
    kubernetes = kubernetes.worker2
  }

  account_id              = data.aws_caller_identity.current.account_id
  aws_region              = var.aws_region
  eks_oidc_provider_arn   = module.clusters["worker2"].oidc_provider_arn
  s3_bucket_arn           = module.storage.s3_bucket.arn
  vpc_public_subnet_count = local.az_count
}
