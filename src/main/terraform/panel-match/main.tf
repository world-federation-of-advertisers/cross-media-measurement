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

module "panel_exchange_client" {
  source = "./modules/panel-exchange-client"

  # EKS vars
  availability_zones_count = 2
  project                  = "tftest"
  vpc_cidr                 = "10.0.0.0/16"
  subnet_cidr_bits         = 8

  # General MP vars
  bucket_name    = "tf-test-blob-storage"
  kms_alias_name = "my-key-alias"
  ca_org_name    = "WFA"
  ca_common_name = "WFA AWS MP CA"
  ca_dns         = "example.com"

  # EKS Config vars
  use_test_secrets         = true
  image_name               = "push_aws_example_daemon_image"
  build_target_name        = "example_mp_daemon_aws"
  manifest_name            = "example_mp_daemon_aws.yaml"
  repository_name          = "panel-exchange/aws-example-daemon"
  path_to_secrets          = "../k8s/testing/secretfiles"
  path_to_cue              = "../k8s/dev/example_mp_daemon_aws.cue"
  k8s_account_service_name = "mp-workflow"
  kingdom_endpoint         = "public.kingdom.dev.halo-cmm.org:8443"
}
