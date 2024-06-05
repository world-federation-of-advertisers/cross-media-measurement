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

resource "aws_iam_role" "cluster_role" {
  name = "${var.project}-Cluster-Role"

  assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "eks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy_attachment" "cluster_AmazonEKSClusterPolicy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role = aws_iam_role.cluster_role.name
}


resource "aws_iam_role" "node_role" {
  name = "${var.project}-Worker-Role"

  assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy_attachment" "node_AmazonCloudWatchPolicy" {
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
  role = aws_iam_role.node_role.name
}

resource "aws_iam_role_policy_attachment" "node_AmazonEKSWorkerNodePolicy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role = aws_iam_role.node_role.name
}

resource "aws_iam_role_policy_attachment" "node_AmazonEKS_CNI_Policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role = aws_iam_role.node_role.name
}

resource "aws_iam_role_policy_attachment" "node_AmazonEC2ContainerRegistryReadOnly" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role = aws_iam_role.node_role.name
}

resource "aws_iam_policy" "panel_exchange_aws_resource_access_policy" {
  name        = "PanelExchangeAwsResourceAccessPolicy"
  path        = "/"
  description = "Additional policies for the panel exchange service to access AWS resources."
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "kms:*",
        ]
        Effect   = "Allow"
        Resource = aws_kms_key.k8s_key.arn
      },
      {
        Action = [
          "s3:*",
        ]
        Effect   = "Allow"
        Resource = [
          "${aws_s3_bucket.blob_storage.arn}/",
          "${aws_s3_bucket.blob_storage.arn}/*",
        ]
      },
      {
        Action = [
          "emr-serverless:*",
        ]
        Effect   = "Allow"
        Resource = [
          "arn:aws:emr-serverless:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:/*"
        ]
      },
      {
        Action = [
          "acm-pca:*",
        ]
        Effect   = "Allow"
        Resource = [
          "arn:aws:acm-pca:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:certificate-authority/*"
        ]
      },
      {
        "Effect": "Allow",
        "Action": [
          "iam:PassRole"
        ],
        "Resource": [
          aws_iam_role.emr_serverless_access_role.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role" "service_access_role" {
  assume_role_policy = data.aws_iam_policy_document.pod_assume_role_policy.json
  name               = "ServiceAccessRole"
}

resource "aws_iam_role_policy_attachment" "pod_sa_access_role_panel_exchange_aws_resource_access_policy" {
  role       = aws_iam_role.service_access_role.name
  policy_arn = aws_iam_policy.panel_exchange_aws_resource_access_policy.arn
}

resource "aws_iam_role_policy_attachment" "node_role_panel_exchange_aws_resource_access_policy" {
  policy_arn = aws_iam_role.service_access_role.name
  role = aws_iam_role.node_role.name
}
