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

locals {
  az_count        = 2
  azs             = slice(data.aws_availability_zones.available.names, 0, local.az_count)
  vpc_cidr        = "10.0.0.0/16"
  duchy_names     = toset(["worker2"])
  vpc_subnet_tags = { for name in local.duchy_names : "kubernetes.io/cluster/${name}-duchy" => "shared" }
  vpc_name        = var.aws_project_env
  bucket_name     = var.aws_s3_bucket
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

  public_subnet_tags = merge(local.vpc_subnet_tags, {
    "kubernetes.io/role/elb" = 1
  })

  private_subnet_tags = merge(local.vpc_subnet_tags, {
    "kubernetes.io/role/internal-elb" = 1
  })
}

module "storage" {
  source = "../modules/s3-bucket"

  bucket_name = local.bucket_name
}

module "postgres" {
  source = "../modules/rds-postgres"

  name              = var.postgres_instance_name
  subnet_group_name = module.vpc.database_subnet_group
  vpc_cidr_block    = module.vpc.vpc_cidr_block
  vpc_id            = module.vpc.vpc_id
  instance_class    = var.postgres_instance_tier
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Terraform   = "true"
      Environment = var.aws_project_env
    }
  }
}
