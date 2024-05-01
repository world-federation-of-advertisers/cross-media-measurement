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

variable "account_id" {
  description = "ID of the target AWS account"
  type        = string
  nullable    = false
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  nullable    = false
}

variable "eks_oidc_provider_arn" {
  description = "ARN of the EKS cluster's OIDC provider"
  type        = string
  nullable    = false
}

variable "s3_bucket_arn" {
  description = "ARN of the s3 bucket"
  type        = string
  nullable    = false
}

variable "vpc_public_subnet_count" {
  description = "Number of public subnets in the VPC"
  type        = number
  nullable    = false
}
