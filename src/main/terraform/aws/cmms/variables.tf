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

variable "aws_project_env" {
  description = "The environments name of the project (e.g. halo-cmm-dev), will be used as prefix to distinguish the resource names."
  type        = string
  nullable    = false
}

variable "aws_s3_bucket" {
  description = "The name of the S3 bucket."
  type        = string
  nullable    = false
}

variable "postgres_instance_name" {
  description = "Name of the RDS PostgreSQL instance."
  type        = string
  default     = "halo"
  nullable    = false
}

variable "postgres_instance_tier" {
  description = "Tier (machine type) of the RDS PostgreSQL instance."
  type        = string
  default     = "db.t3.micro"
  nullable    = false
}
