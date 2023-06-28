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

resource "aws_eks_cluster" "cluster" {
  name = "${var.project}-cluster-3"
  role_arn = "${var.project_arn}/tftest-Cluster-Role"

  vpc_config {
    subnet_ids = flatten([
      aws_subnet.public_subnet[*].id,
      aws_subnet.private_subnet[*].id,
    ])
    endpoint_private_access = true
    endpoint_public_access = true
    public_access_cidrs = ["0.0.0.0/0"]
  }

#  depends_on = [
#    aws_iam_role_policy_attachment.cluster_AmazonEKSClusterPolicy
#  ]
}

#resource "aws_iam_role" "cluster_role" {
#  name = "${var.project}-Cluster-Role"
#
#  assume_role_policy = <<POLICY
#{
#  "Version": "2012-10-17",
#  "Statement": [
#    {
#      "Effect": "Allow",
#      "Principal": {
#        "Service": "eks.amazonaws.com"
#      },
#      Action: "sts:AssumeRole"
#    }
#  ]
#}
#POLICY
#}

#resource "aws_iam_role_policy_attachment" "cluster_AmazonEKSClusterPolicy" {
#  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
#  role = aws_iam_role.cluster_role.name
#}

resource "aws_security_group" "eks_cluster" {
  name = "${var.project}-cluster-sg"
  description = "Cluster communication with worker nodes."
  vpc_id = aws_vpc.vpc.id
}

resource "aws_security_group_rule" "cluster_inbound" {
  description = "Allow worker nodes to communicate with the cluster API Server"
  from_port = 443
  protocol = "tcp"
  security_group_id = aws_security_group.eks_cluster.id
  source_security_group_id = aws_security_group.eks_nodes.id
  to_port = 443
  type = "ingress"
}

resource "aws_security_group_rule" "cluster_outbound" {
  description              = "Allow cluster API Server to communicate with the worker nodes"
  from_port                = 1024
  protocol                 = "tcp"
  security_group_id        = aws_security_group.eks_cluster.id
  source_security_group_id = aws_security_group.eks_nodes.id
  to_port                  = 65535
  type                     = "egress"
}

data "tls_certificate" "example" {
  url = aws_eks_cluster.cluster.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "cluster_oidc" {
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [
    data.tls_certificate.example.certificates[0].sha1_fingerprint
  ]
  url = aws_eks_cluster.cluster.identity[0].oidc[0].issuer
}

data "aws_iam_policy" "cluster_assume_role_policy" {
  name        = "csi_assume_role_policy"
  path        = "/"
  description = "policy to allow OIDC federation in the cluster."

  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Actions = ["sts:AssumeRoleWithWebIdentity"]
        effect  = "Allow"

        Condition = {
          StringEquals = {
            variable = "${replace(aws_iam_openid_connect_provider.cluster_oidc.url, "https://", "")}:sub"
            values   = ["system:serviceaccount:kube-system:aws-node"]
          }
          Principals = {
            identifiers = [aws_iam_openid_connect_provider.cluster_oidc.arn]
            type        = "Federated"
          }
        }
      }]
  })
}

resource "aws_iam_policy" "csi_policy" {
  name        = "csi_policy"
  path        = "/"
  description = "policy to allow use of volume management in the cluster."

  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "elasticfilesystem:DescribeAccessPoints",
          "elasticfilesystem:DescribeFileSystems",
          "elasticfilesystem:DescribeMountTargets",
          "ec2:DescribeAvailabilityZones"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "elasticfilesystem:CreateAccessPoint"
        ],
        Resource  = "*",
        Condition = {
          StringLike = {
            "aws:RequestTag/efs.csi.aws.com/cluster" = "true"
          }
        }
      },
      {
        Effect = "Allow",
        Action = [
          "elasticfilesystem:TagResource"
        ],
        Resource  = "*",
        Condition = {
          StringLike = {
            "aws:ResourceTag/efs.csi.aws.com/cluster" = "true"
          }
        }
      },
      {
        Effect    = "Allow",
        Action    = "elasticfilesystem:DeleteAccessPoint",
        Resource  = "*",
        Condition = {
          StringEquals = {
            "aws:ResourceTag/efs.csi.aws.com/cluster" = "true"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "cluster_csi_driver_attachement" {
  assume_role_policy = aws_iam_policy.csi_policy
  name               = "cluster_csi_role"
}

resource "aws_iam_role_policy_attachment" "csi_driver" {
  policy_arn = aws_iam_policy.csi_policy.arn
  role       = "${var.project}-Worker-Role"
  # aws_iam_role.cluster_csi_driver.name
}

#resource "aws_eks_addon" "aws_csi" {
#  addon_name               = "aws-ebs-csi-driver"
#  cluster_name             = aws_eks_cluster.cluster.name
#  service_account_role_arn = "arn:aws:iam::010295286036:role/tftest-Worker-Role"
#  # aws_iam_role.cluster_csi_driver.arn
#}