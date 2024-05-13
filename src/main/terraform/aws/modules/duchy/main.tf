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


# internal_server user IAM & service account
module "internal_server_iam_policy" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-policy"
  version = "~> 5.30.0"

  name        = "internal-server"
  path        = "/"
  description = "Policy for duchy internal server"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:*",
        ]
        Resource = "${var.s3_bucket_arn}/*"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "secretsmanager:GetResourcePolicy",
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret",
          "secretsmanager:ListSecretVersionIds"
        ],
        "Resource" : [
          "arn:aws:secretsmanager:${var.aws_region}:${var.account_id}:secret:rds!*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : "secretsmanager:ListSecrets",
        "Resource" : "*"
      }
    ]
  })
}

module "internal_server_eks_role" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.30.0"

  role_name = "internal-server"

  role_policy_arns = {
    policy = module.internal_server_iam_policy.arn
  }

  oidc_providers = {
    main = {
      provider_arn               = var.eks_oidc_provider_arn
      namespace_service_accounts = ["default:internal-server"]
    }
  }
}

resource "kubernetes_service_account" "internal-server-service-account" {
  metadata {
    name      = "internal-server"
    namespace = "default"

    annotations = {
      "eks.amazonaws.com/role-arn" = module.internal_server_eks_role.iam_role_arn
    }
  }
}

# storage user IAM & service account
module "storage_iam_policy" {
  source = "terraform-aws-modules/iam/aws//modules/iam-policy"

  name        = "storage"
  path        = "/"
  description = "Policy for accessing duchy storage blobs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:*",
        ]
        Resource = "${var.s3_bucket_arn}/*"
      }
    ]
  })
}

module "storage_eks_role" {
  source    = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  role_name = "storage"

  role_policy_arns = {
    policy = module.storage_iam_policy.arn
  }

  oidc_providers = {
    main = {
      provider_arn               = var.eks_oidc_provider_arn
      namespace_service_accounts = ["default:storage"]
    }
  }
}

resource "kubernetes_service_account" "storage-service-account" {
  metadata {
    name      = "storage"
    namespace = "default"

    annotations = {
      "eks.amazonaws.com/role-arn" = module.storage_eks_role.iam_role_arn
    }
  }
}

resource "aws_eip" "v2alpha" {
  count = var.vpc_public_subnet_count

  domain = "vpc"
  tags = {
    "Name" : "duchy-v2alpha-${count.index}"
  }
}

resource "aws_eip" "system_v1alpha" {
  count = var.vpc_public_subnet_count

  domain = "vpc"
  tags = {
    "Name" : "duchy-system-v1alpha-${count.index}"
  }
}

