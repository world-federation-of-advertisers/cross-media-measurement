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

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 19.16.0"

  cluster_name    = var.cluster_name
  cluster_version = var.cluster_version

  vpc_id                   = var.vpc_id
  subnet_ids               = var.subnet_ids
  control_plane_subnet_ids = var.control_plane_subnet_ids

  cluster_endpoint_private_access = true
  cluster_endpoint_public_access  = true
  enable_irsa                     = true

  cluster_addons = {
    coredns = {
      most_recent = true
    }
    kube-proxy = {
      most_recent = true
    }
    vpc-cni = {
      most_recent = true
    }
    amazon-cloudwatch-observability = {
      most_recent = true
    }
  }

  eks_managed_node_group_defaults = {
    ami_type = "AL2_x86_64"
  }

  eks_managed_node_groups = {
    default = {
      min_size     = 1
      max_size     = var.default_max_node_count
      desired_size = 1

      instance_types = var.default_instance_types
      capacity_type  = "ON_DEMAND"

      update_config = {
        max_unavailable_percentage = 1 # Minimum is 1
      }
    }
    high_perf = {
      min_size     = 1
      max_size     = var.high_perf_max_node_count
      desired_size = 1

      instance_types = var.high_perf_instance_types
      capacity_type  = "SPOT"

      update_config = {
        max_unavailable_percentage = 1 # Minimum is 1
      }

      taints = [
        {
          key    = "high_perf_spot_node"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      ]
    }
  }

  kms_key_administrators  = var.kms_key_administrators
  create_kms_key          = true
  enable_kms_key_rotation = true
  cluster_encryption_config = {
    "resources" : [
      "secrets"
    ]
  }
}

resource "aws_iam_role_policy_attachment" "cloudwatch_policy_attachment" {
  for_each = module.eks.eks_managed_node_groups

  role       = each.value.iam_role_name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}
