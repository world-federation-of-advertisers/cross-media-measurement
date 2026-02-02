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

# Moved blocks for refactoring google_secret_manager_secret_iam_member resources
# from edp-aggregator module into gcs-bucket-cloud-function and http-cloud-function modules.
# See: https://developer.hashicorp.com/terraform/language/modules/develop/refactoring

# =============================================================================
# data_watcher_cloud_function (gcs-bucket-cloud-function module)
# =============================================================================

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["data_watcher:secure_computation_root_ca"]
  to   = module.data_watcher_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.secure_computation_root_ca.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["data_watcher:data_watcher_tls_key"]
  to   = module.data_watcher_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.data_watcher_tls_key.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["data_watcher:data_watcher_tls_pem"]
  to   = module.data_watcher_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.data_watcher_tls_pem.secret_id]
}

# =============================================================================
# requisition_fetcher_cloud_function (http-cloud-function module)
# =============================================================================

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["requisition_fetcher:trusted_root_ca_collection"]
  to   = module.requisition_fetcher_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.trusted_root_ca_collection.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["requisition_fetcher:requisition_fetcher_tls_pem"]
  to   = module.requisition_fetcher_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.requisition_fetcher_tls_pem.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["requisition_fetcher:requisition_fetcher_tls_key"]
  to   = module.requisition_fetcher_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.requisition_fetcher_tls_key.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["requisition_fetcher:metadata_storage_root_ca"]
  to   = module.requisition_fetcher_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.metadata_storage_root_ca.secret_id]
}

# =============================================================================
# event_group_sync_cloud_function (http-cloud-function module)
# =============================================================================

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["event_group_sync:trusted_root_ca_collection"]
  to   = module.event_group_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.trusted_root_ca_collection.secret_id]
}

# =============================================================================
# data_availability_sync_cloud_function (http-cloud-function module)
# =============================================================================

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["data_availability_sync:metadata_storage_root_ca"]
  to   = module.data_availability_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.metadata_storage_root_ca.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["data_availability_sync:trusted_root_ca_collection"]
  to   = module.data_availability_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.trusted_root_ca_collection.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["data_availability_sync:data_availability_tls_key"]
  to   = module.data_availability_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.data_availability_tls_key.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["data_availability_sync:data_availability_tls_pem"]
  to   = module.data_availability_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.data_availability_tls_pem.secret_id]
}

# =============================================================================
# EDP-specific secrets (dynamically generated based on var.edps_certs)
# =============================================================================

# -----------------------------------------------------------------------------
# requisition_fetcher EDP secrets
# -----------------------------------------------------------------------------

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["requisition_fetcher:edp7_tls_key"]
  to   = module.requisition_fetcher_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edp7"].tls_key.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["requisition_fetcher:edp7_tls_pem"]
  to   = module.requisition_fetcher_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edp7"].tls_pem.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["requisition_fetcher:edp7_enc_private"]
  to   = module.requisition_fetcher_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edp7"].enc_private.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["requisition_fetcher:edpa_meta_tls_key"]
  to   = module.requisition_fetcher_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edpa_meta"].tls_key.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["requisition_fetcher:edpa_meta_tls_pem"]
  to   = module.requisition_fetcher_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edpa_meta"].tls_pem.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["requisition_fetcher:edpa_meta_enc_private"]
  to   = module.requisition_fetcher_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edpa_meta"].enc_private.secret_id]
}

# -----------------------------------------------------------------------------
# event_group_sync EDP secrets
# -----------------------------------------------------------------------------

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["event_group_sync:edp7_tls_key"]
  to   = module.event_group_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edp7"].tls_key.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["event_group_sync:edp7_tls_pem"]
  to   = module.event_group_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edp7"].tls_pem.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["event_group_sync:edp7_enc_private"]
  to   = module.event_group_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edp7"].enc_private.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["event_group_sync:edpa_meta_tls_key"]
  to   = module.event_group_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edpa_meta"].tls_key.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["event_group_sync:edpa_meta_tls_pem"]
  to   = module.event_group_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edpa_meta"].tls_pem.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["event_group_sync:edpa_meta_enc_private"]
  to   = module.event_group_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edpa_meta"].enc_private.secret_id]
}

# -----------------------------------------------------------------------------
# data_availability_sync EDP secrets
# -----------------------------------------------------------------------------

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["data_availability_sync:edp7_tls_key"]
  to   = module.data_availability_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edp7"].tls_key.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["data_availability_sync:edp7_tls_pem"]
  to   = module.data_availability_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edp7"].tls_pem.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["data_availability_sync:edp7_enc_private"]
  to   = module.data_availability_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edp7"].enc_private.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["data_availability_sync:edpa_meta_tls_key"]
  to   = module.data_availability_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edpa_meta"].tls_key.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["data_availability_sync:edpa_meta_tls_pem"]
  to   = module.data_availability_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edpa_meta"].tls_pem.secret_id]
}

moved {
  from = google_secret_manager_secret_iam_member.secret_accessor["data_availability_sync:edpa_meta_enc_private"]
  to   = module.data_availability_sync_cloud_function.google_secret_manager_secret_iam_member.secret_accessor[var.edps_certs["edpa_meta"].enc_private.secret_id]
}
