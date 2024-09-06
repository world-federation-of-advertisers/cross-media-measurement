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


terraform {
  backend "s3" {
    key = "terraform.tfstate"
  }
}

data "aws_availability_zones" "available" {}
data "aws_caller_identity" "current" {}

locals {
  az_count = 2
  azs      = slice(data.aws_availability_zones.available.names, 0, local.az_count)
  vpc_cidr = "10.0.0.0/16"
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.1.1"

  name = var.vpc_name
  cidr = local.vpc_cidr

  azs              = local.azs
  private_subnets  = [for k, v in local.azs : cidrsubnet(local.vpc_cidr, 8, k + 4)]
  public_subnets   = [for k, v in local.azs : cidrsubnet(local.vpc_cidr, 8, k + 8)]
  database_subnets = [for k, v in local.azs : cidrsubnet(local.vpc_cidr, 8, k + 12)]
  intra_subnets    = [for k, v in local.azs : cidrsubnet(local.vpc_cidr, 8, k + 16)]

  create_database_subnet_group = true

  enable_nat_gateway = true
  single_nat_gateway = true
  create_igw         = true

  public_subnet_tags = {
    "kubernetes.io/role/elb"      = 1,
    "kubernetes.io/cluster/duchy" = "shared"
  }

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = 1,
    "kubernetes.io/cluster/duchy"     = "shared"
  }
}

module "storage" {
  source = "../../modules/s3-bucket"

  bucket_name = var.bucket_name
}

module "postgres" {
  source = "../../modules/rds-postgres"

  name              = var.postgres_instance_name
  subnet_group_name = module.vpc.database_subnet_group
  vpc_cidr_block    = module.vpc.vpc_cidr_block
  vpc_id            = module.vpc.vpc_id
  instance_class    = var.postgres_instance_tier
}

module "cluster" {
  source = "../../modules/eks-cluster"

  aws_region               = var.aws_region
  cluster_name             = var.duchy_name
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

module "load_balancer_controller_irsa_role" {
  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"

  role_name                              = "load-balancer-controller"
  attach_load_balancer_controller_policy = true

  oidc_providers = {
    provider_arn = module.cluster.oidc_provider_arn
    namespace_service_accounts = [
      "kube-system:aws-load-balancer-controller"
    ]
  }
}

module "cluster_addons" {
  source = "../../modules/eks-cluster-addons"

  providers = {
    kubernetes = kubernetes.duchy
    helm       = helm.duchy
  }

  aws_region                             = var.aws_region
  cluster_name                           = module.cluster.cluster_name
  eks_oidc_provider_arn                  = module.cluster.oidc_provider_arn
  load_balancer_controller_irsa_role_arn = module.load_balancer_controller_irsa_role.iam_role_arn
  vpc_id                                 = module.vpc.vpc_id
}

module "duchy" {
  source = "../../modules/duchy"
  providers = {
    kubernetes = kubernetes.duchy
  }

  account_id              = data.aws_caller_identity.current.account_id
  aws_region              = var.aws_region
  eks_oidc_provider_arn   = module.cluster.oidc_provider_arn
  s3_bucket_arn           = module.storage.s3_bucket.arn
  vpc_public_subnet_count = local.az_count
}


provider "kubernetes" {
  alias                  = "duchy"
  host                   = module.cluster.cluster_endpoint
  cluster_ca_certificate = base64decode(module.cluster.cluster_certificate_authority_data)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.cluster.cluster_name]
  }
}

provider "helm" {
  alias = "duchy"
  kubernetes {
    host                   = module.cluster.cluster_endpoint
    cluster_ca_certificate = base64decode(module.cluster.cluster_certificate_authority_data)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.cluster.cluster_name]
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Terraform   = "true"
      Environment = "halo-cmm-dev"
    }
  }
}
