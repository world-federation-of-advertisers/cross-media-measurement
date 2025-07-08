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

data "google_project" "project" {}
data "google_client_config" "default" {}

resource "google_service_account" "cloud_function_service_account" {
  account_id   = var.cloud_function_service_account_name
  display_name = "Service account for Cloud Function"
}

resource "google_service_account" "cloud_function_trigger_service_account" {
  account_id   = var.cloud_function_trigger_service_account_name
  display_name = "Trigger Service Account for Cloud Function"
}

resource "google_service_account_iam_member" "allow_terraform_to_use_cloud_function_service_account" {
  service_account_id = google_service_account.cloud_function_service_account.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}

resource "google_service_account_iam_member" "allow_terraform_to_use_data_watcher_trigger_service_account" {
  service_account_id = google_service_account.cloud_function_trigger_service_account.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}

resource "google_storage_bucket_iam_member" "cloud_function_object_viewer" {
  bucket = var.trigger_bucket_name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.cloud_function_service_account.email}"
}

resource "google_storage_bucket_iam_member" "cloud_function_object_creator" {
  bucket = var.trigger_bucket_name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.cloud_function_service_account.email}"
}

resource "google_project_iam_member" "trigger_event_receiver" {
  project = data.google_project.project.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.cloud_function_trigger_service_account.email}"
}

resource "google_project_iam_member" "trigger_run_invoker" {
  project = data.google_project.project.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.cloud_function_trigger_service_account.email}"
}

resource "terraform_data" "deploy_data_watcher" {

  depends_on = [
    google_service_account.cloud_function_service_account,
    google_service_account.cloud_function_trigger_service_account,
    google_service_account_iam_member.allow_terraform_to_use_cloud_function_service_account,
    google_service_account_iam_member.allow_terraform_to_use_data_watcher_trigger_service_account,
    google_storage_bucket_iam_member.cloud_function_object_viewer,
    google_storage_bucket_iam_member.cloud_function_object_creator,
    google_project_iam_member.trigger_event_receiver,
    google_project_iam_member.trigger_run_invoker,
  ]

  # force recreation (and thus re-run of the provisioner) if any of these values change
  triggers_replace = [
    var.bazel_target_label,
    var.extra_env_vars,
    var.secret_mappings,
  ]

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    environment = {
      FUNCTION_NAME           = var.function_name
      ENTRY_POINT             = var.entry_point
      CLOUD_REGION            = data.google_client_config.default.region
      RUN_SERVICE_ACCOUNT     = google_service_account.cloud_function_service_account.email
      TRIGGER_BUCKET          = var.trigger_bucket_name
      TRIGGER_SERVICE_ACCOUNT = google_service_account.cloud_function_trigger_service_account.email
      EXTRA_ENV_VARS          = var.extra_env_vars
      SECRET_MAPPINGS         = var.secret_mappings
      BAZEL_TARGET_LABEL      = var.bazel_target_label
    }
    command = <<-EOT
        #!/bin/bash
        set -euo pipefail

        bazel build "$BAZEL_TARGET_LABEL"

        JAR=$(bazel cquery "$BAZEL_TARGET_LABEL" --output=starlark --starlark:expr="target.files.to_list()[0].path")
        JAR=$(realpath "$JAR")
        echo "The path is: $JAR"
        TEMP_DIR=$(mktemp -d)
        cp "$JAR" "$TEMP_DIR/"

        GCLOUD_CMD="gcloud functions deploy \"$FUNCTION_NAME\" \
          --gen2 \
          --runtime=java17 \
          --entry-point=\"$ENTRY_POINT\" \
          --memory=512MB \
          --region=\"$CLOUD_REGION\" \
          --run-service-account=\"$RUN_SERVICE_ACCOUNT\" \
          --source=\"$TEMP_DIR\" \
          --trigger-event-filters=type=google.cloud.storage.object.v1.finalized \
          --trigger-event-filters=bucket=$TRIGGER_BUCKET \
          --trigger-service-account=\"$TRIGGER_SERVICE_ACCOUNT\""

        if [[ -n "$EXTRA_ENV_VARS" ]]; then
          GCLOUD_CMD+=" --set-env-vars=\"$EXTRA_ENV_VARS\""
        fi
        if [[ -n "$SECRET_MAPPINGS" ]]; then
          GCLOUD_CMD+=" --set-secrets=\"$SECRET_MAPPINGS\""
        fi

        eval "$GCLOUD_CMD"
      EOT
  }
}
