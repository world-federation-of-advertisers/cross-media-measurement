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

variable "aws_region" {
  description = "AWS region"
  type        = string
  nullable    = false
}

variable "cluster_name" {
  description = "Name of the EKS cluster"
  type        = string
  nullable    = false
}

variable "cluster_version" {
  description = "Version of the EKS cluster"
  type        = string
  nullable    = false
}

variable "vpc_id" {
  description = "Vpc id"
  type        = string
  nullable    = false
}

variable "subnet_ids" {
  description = "List of subnet ids"
  type        = list(string)
  nullable    = false
}

variable "control_plane_subnet_ids" {
  description = "List of subnet ids used by control plane"
  type        = list(string)
  nullable    = false
}

variable "kms_key_administrators" {
  description = "List of kms key administrators ARNs"
  type        = list(string)
  default     = null
}

variable "default_instance_types" {
  description = "List of instance types used by default node group"
  type        = list(string)
  nullable    = false
}

variable "default_max_node_count" {
  description = "Maximum number of nodes that can be created in default node group"
  type        = number
  nullable    = false
}

variable "high_perf_instance_types" {
  description = "List of instance types used by high_perf node group"
  type        = list(string)
  nullable    = false
}

variable "high_perf_max_node_count" {
  description = "Maximum number of nodes that can be created in high_perf node group"
  type        = number
  nullable    = false
}
