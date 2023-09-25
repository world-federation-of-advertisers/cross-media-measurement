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

variable "name" {
  description = "name of the rds postgres instance"
  type        = string
  nullable    = false
}

variable "vpc_id" {
  description = "vpc id"
  type        = string
  nullable    = false
}

variable "vpc_cidr_block" {
  description = "vpc cidr block"
  type        = string
  nullable    = false
}

variable "subnet_group_name" {
  description = "name of the subnet group"
  type        = string
  nullable    = false
}

variable "username" {
  description = "name of the db user"
  type        = string
  default     = "postgres"
  nullable    = false
}

variable "instance_class" {
  description = "postgres instance class"
  type        = string
  nullable    = false
}
