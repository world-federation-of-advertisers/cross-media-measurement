# Copyright 2026 The Cross-Media Measurement Authors
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


mock_provider "google" {}

variables {
  http_cloud_function_service_account_name = "example-function"
  terraform_service_account                = "terraform@example-project.iam.gserviceaccount.com"
  function_name                            = "example-function"
  entry_point                              = "example.Function"
  extra_env_vars                           = ""
  secret_mappings                          = ""
  uber_jar_path                            = "/tmp/example/Function.jar"
}

run "existing_default" {
  command = plan
  assert {
    condition     = terraform_data.deploy_http_cloud_function.triggers_replace == ["/tmp/example/Function.jar", "", "", "", var.timeout_seconds, var.max_instances, var.concurrency]
    error_message = "Leaving memory unset must preserve preexisting deployment replacement keys."
  }
}

run "configured_memory_redeploys" {
  command = plan
  variables { memory = "4GiB" }
  assert {
    condition     = contains(terraform_data.deploy_http_cloud_function.triggers_replace, "4GiB")
    error_message = "Changing memory must replace the deployment action."
  }
}

run "reject_invalid_memory" {
  command = plan
  variables { memory = "large" }
  expect_failures = [var.memory]
}
