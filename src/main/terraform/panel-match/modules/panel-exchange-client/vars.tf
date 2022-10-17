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

data "aws_partition" "current" {}

variable "cluster_config" {
  type = object({
    availability_zones_count = number
    project = string
    vpc_cidr = string
    subnet_cidr_bits = number
  })
}

variable "resource_config" {
  type = object({
    bucket_name = string
    kms_alias_name = string
    ca_org_name = string
    ca_common_name = string
  })
}

variable "k8s_config" {
  type = object({
    use_test_secrets = bool
    image_name = string
    build_target_name = string
    manifest_name = string
    repository_name = string
    path_to_secrets = string
    k8s_account_service_name = string
  })
}
