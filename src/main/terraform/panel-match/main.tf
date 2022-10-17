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

  cluster_config = {
    availability_zones_count = 2
    project = "tftest"
    vpc_cidr = "10.0.0.0/16"
    subnet_cidr_bits = 8
  }

  resource_config = {
    bucket_name = "tf-test-blob-storage"
    kms_alias_name = "my-key-alias"
    ca_org_name = "WFA"
    ca_common_name = "WFA AWS MP CA"
  }

  k8s_config = {
    use_test_secrets = true
    image_name = "push_aws_example_daemon_image"
    build_target_name = "example_edp_daemon_aws"
    manifest_name = "example_edp_daemon_aws.yaml"
    repository_name = "example-panel-exchange-daemon"
    path_to_secrets = "../k8s/testing/secretfiles"
    k8s_account_service_name = "edp-workflow"
  }
}
