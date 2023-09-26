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
  description = "name of the eks cluster"
  type        = string
  nullable    = false
}

variable "vpc_id" {
  description = "vpc id"
  type        = string
  nullable    = false
}

variable "subnet_ids" {
  description = "list of subnet ids"
  type        = list(string)
  nullable    = false
}

variable "control_plane_subnet_ids" {
  description = "list of subnet ids used by control plane"
  type        = list(string)
  nullable    = false
}
