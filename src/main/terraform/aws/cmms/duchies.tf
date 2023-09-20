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

locals {
  az_count     = 2
  azs          = slice(data.aws_availability_zones.available.names, 0, local.az_count)
  vpc_cidr     = "10.0.0.0/16"
  duchy_name   = "worker2-duchy"
  vpc_name     = "${local.duchy_name}-vpc"
  cluster_name = "${local.duchy_name}-cluster"
  bucket_name  = "${local.duchy_name}-storage"
  db_name      = "${local.duchy_name}-postgres"
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.1.1"

  name = local.vpc_name
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
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
    "kubernetes.io/role/elb"                      = 1
  }

  private_subnet_tags = {
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
    "kubernetes.io/role/internal-elb"             = 1
  }
}

resource "aws_eip" "eip" {
  count  = local.az_count
  domain = "vpc"
}

module "storage" {
  source = "../modules/s3-bucket"

  bucket_name = local.bucket_name
}

module "postgres" {
  source = "../modules/rds-postgres"

  name              = local.db_name
  subnet_group_name = module.vpc.database_subnet_group
  vpc_cidr_block    = module.vpc.vpc_cidr_block
  vpc_id            = module.vpc.vpc_id
}

module "eks" {
  source = "../modules/eks-cluster"

  aws_region               = var.aws_region
  cluster_name             = local.cluster_name
  control_plane_subnet_ids = module.vpc.intra_subnets
  subnet_ids               = module.vpc.private_subnets
  vpc_id                   = module.vpc.vpc_id
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
  }
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
    }
  }
}

module "worker2-duchy" {
  source = "../modules/duchy"

  eks_oidc_provider_arn = module.eks.oidc_provider_arn
  s3_bucket_arn         = module.storage.s3_bucket_arn
}
