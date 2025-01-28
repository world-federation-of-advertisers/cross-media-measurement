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

data "aws_region" "current" {}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

# EKS vars
variable "availability_zones_count" {
  description = "The number of AZs."
  type        = number
  default     = 2
}

variable "project" {
  description = "Name to be used on all the resources asan identifier."
  type        = string
  default     = "tftest"
}

variable "vpc_cidr" {
  description = "The CIDR block for the VPC. Default is valid, but should be overridden."
  type        = string
  default     = "10.0.0.0/16"
}

variable "subnet_cidr_bits" {
  description = "The number of subnet bits for the CIDR."
  type        = number
  default     = 8
}

# EKS config vars
variable "use_test_secrets" {
  description = "Whether or not to use the test secrets. They should not be used outside of testing purposes."
  type        = bool
  default     = false
}

variable "image_name" {
  description = "The name of the image to build, push, and deploy."
  type        = string
}

variable "build_target_name" {
  description = "The name of the bazel target to run."
  type        = string
}

variable "manifest_name" {
  description = "The name of the manifest to apply."
  type        = string
}

variable "repository_name" {
  description = "The name of the respository you want to create."
  type        = string
}

variable "path_to_secrets" {
  type = string
}

variable "k8s_account_service_name" {
  type = string
}

variable "path_to_cue" {
  type    = string
  default = "../k8s/dev/example_mp_daemon_aws.cue"
}

variable "ca_common_name" {
  type = string
}

variable "ca_org_name" {
  type = string
}

variable "ca_dns" {
  type = string
}

variable "kingdom_endpoint" {
  type = string
}

# Other vars
variable "bucket_name" {
  type = string
}

variable "kms_alias_name" {
  type = string
}
