# Copyright 2025 The Cross-Media Measurement Authors
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

data "google_client_config" "default" {}

resource "terraform_data" "deploy_gcs_cloud_function" {

  triggers_replace = concat([var.uber_jar_path], var.deployment_dependencies)

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    environment = {
      FUNCTION_NAME           = var.function_name
      ENTRY_POINT             = var.entry_point
      CLOUD_REGION            = data.google_client_config.default.region
      RUN_SERVICE_ACCOUNT     = var.service_account_email
      TRIGGER_BUCKET          = var.trigger_bucket_name
      TRIGGER_SERVICE_ACCOUNT = var.trigger_service_account_email
      EXTRA_ENV_VARS          = var.extra_env_vars
      SECRET_MAPPINGS         = var.secret_mappings
      UBER_JAR_DIRECTORY      = dirname(var.uber_jar_path)
      TRIGGER_EVENT_TYPE      = var.trigger_event_type
    }
    command = <<-EOT
      #!/bin/bash
      set -euo pipefail

      args=(
        "functions" "deploy" "$FUNCTION_NAME"
        "--gen2"
        "--runtime=java17"
        "--entry-point=$ENTRY_POINT"
        "--memory=512MB"
        "--region=$CLOUD_REGION"
        "--run-service-account=$RUN_SERVICE_ACCOUNT"
        "--source=$UBER_JAR_DIRECTORY"
        "--trigger-event-filters=type=$TRIGGER_EVENT_TYPE"
        "--trigger-event-filters=bucket=$TRIGGER_BUCKET"
        "--trigger-service-account=$TRIGGER_SERVICE_ACCOUNT"
        "--no-allow-unauthenticated"
      )

      if [[ -n "$EXTRA_ENV_VARS" ]]; then
        args+=("--set-env-vars=$EXTRA_ENV_VARS")
      fi

      if [[ -n "$SECRET_MAPPINGS" ]]; then
        args+=("--set-secrets=$SECRET_MAPPINGS")
      fi

      gcloud "$${args[@]}"
    EOT
  }
}
