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

variable "iam_service_account_name" {
  description = "Name of the IAM service account."
  type        = string
  nullable    = false
}

variable "iam_service_account_description" {
  description = "Description of the IAM service account."
  type        = string
}

variable "k8s_service_account_name" {
  description = "Name of the Kubernetes service account."
  type        = string
  nullable    = false
}

variable "cluster_namespace" {
  description = "Kubernetes cluster namespace."
  type        = string
  nullable    = false
  default     = "default"
}
