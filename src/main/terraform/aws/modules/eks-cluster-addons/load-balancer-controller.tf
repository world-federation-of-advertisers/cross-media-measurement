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

module "load_balancer_controller_targetgroup_binding_only_irsa_role" {
  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"

  role_name                                                       = "load-balancer-controller-targetgroup-binding-only"
  attach_load_balancer_controller_targetgroup_binding_only_policy = true

  oidc_providers = {
    main = {
      provider_arn               = var.eks_oidc_provider_arn
      namespace_service_accounts = ["kube-system:${var.load_balancer_controller_sa_name}"]
    }
  }
}

resource "kubernetes_service_account" "load-balancer-controller-service-account" {
  metadata {
    name      = var.load_balancer_controller_sa_name
    namespace = "kube-system"

    annotations = {
      "eks.amazonaws.com/role-arn" = var.load_balancer_controller_irsa_role_arn
    }
  }
}

resource "helm_release" "aws_load_balancer_controller" {
  name            = "aws-load-balancer-controller"
  chart           = "aws-load-balancer-controller"
  version         = "1.6.1"
  repository      = "https://aws.github.io/eks-charts"
  namespace       = "kube-system"
  cleanup_on_fail = true

  set {
    name  = "clusterName"
    value = var.cluster_name
  }

  set {
    name  = "region"
    value = var.aws_region
  }

  set {
    name  = "vpcId"
    value = var.vpc_id
  }

  set {
    name  = "serviceAccount.create"
    value = "false"
  }

  set {
    name  = "serviceAccount.name"
    value = var.load_balancer_controller_sa_name
  }
}
