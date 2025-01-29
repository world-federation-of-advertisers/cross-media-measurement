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

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

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

variable "cluster_name" {
  type = string
}

variable "kms_key_id" {
  type = string
}

variable "path_to_edp_cue" {
  type    = string
  default = "../k8s/dev/example_edp_daemon_aws.cue"
}

variable "path_to_edp_cue_base" {
  type    = string
  default = "../k8s/dev/example_daemon_aws.cue"
}

variable "ca_arn" {
  type = string
}
