# Copyright 2020 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Container image specs."""

load("//build:variables.bzl", "IMAGE_REPOSITORY_SETTINGS")

_PREFIX = IMAGE_REPOSITORY_SETTINGS.repository_prefix

# List of specs for all Docker containers to push to a container registry.
# These are common to both local execution (e.g. in Kind) as well as on GKE.
COMMON_IMAGES = [
    struct(
        name = "duchy_async_computation_control_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/server:async_computation_control_server_image",
        repository = _PREFIX + "/duchy/async-computation-control",
    ),
    struct(
        name = "duchy_herald_daemon_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/daemon/herald:herald_daemon_image",
        repository = _PREFIX + "/duchy/herald",
    ),
    struct(
        name = "duchy_spanner_update_schema_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/spanner/tools:update_schema_image",
        repository = _PREFIX + "/duchy/spanner-update-schema",
    ),
    struct(
        name = "duchy_computations_cleaner_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/job:computations_cleaner_image",
        repository = _PREFIX + "/duchy/computations-cleaner",
    ),
    struct(
        name = "kingdom_data_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/kingdom/deploy/gcloud/server:gcp_kingdom_data_server_image",
        repository = _PREFIX + "/kingdom/data-server",
    ),
    struct(
        name = "kingdom_system_api_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/server:system_api_server_image",
        repository = _PREFIX + "/kingdom/system-api",
    ),
    struct(
        name = "kingdom_v2alpha_public_api_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/server:v2alpha_public_api_server_image",
        repository = _PREFIX + "/kingdom/v2alpha-public-api",
    ),
    struct(
        name = "kingdom_spanner_update_schema_image",
        image = "//src/main/kotlin/org/wfanet/measurement/kingdom/deploy/gcloud/spanner/tools:update_schema_image",
        repository = _PREFIX + "/kingdom/spanner-update-schema",
    ),
    struct(
        name = "resource_setup_runner_image",
        image = "//src/main/kotlin/org/wfanet/measurement/loadtest/resourcesetup:resource_setup_runner_image",
        repository = _PREFIX + "/loadtest/resource-setup",
    ),
    struct(
        name = "panel_match_resource_setup_runner_image",
        image = "//src/main/kotlin/org/wfanet/measurement/loadtest/panelmatchresourcesetup:panel_match_resource_setup_runner_image",
        repository = _PREFIX + "/loadtest/panel-match-resource-setup",
    ),
]

# List of specs for all Docker containers to push to a container registry.
# These are only used on GKE.
GKE_IMAGES = [
    struct(
        name = "duchy_computation_control_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/server:gcs_computation_control_server_image",
        repository = _PREFIX + "/duchy/computation-control",
    ),
    struct(
        name = "duchy_gcs_spanner_computations_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/server:gcs_spanner_computations_server_image",
        repository = _PREFIX + "/duchy/spanner-computations",
    ),
    struct(
        name = "duchy_requisition_fulfillment_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/server:gcs_requisition_fulfillment_server_image",
        repository = _PREFIX + "/duchy/requisition-fulfillment",
    ),
    struct(
        name = "duchy_liquid_legions_v2_mill_daemon_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/daemon/mill/liquidlegionsv2:gcs_liquid_legions_v2_mill_daemon_image",
        repository = _PREFIX + "/duchy/liquid-legions-v2-mill",
    ),
    struct(
        name = "gcs_frontend_simulator_runner_image",
        image = "//src/main/kotlin/org/wfanet/measurement/loadtest/frontend:gcs_frontend_simulator_runner_image",
        repository = _PREFIX + "/loadtest/frontend-simulator",
    ),
    struct(
        name = "gcs_edp_simulator_runner_image",
        image = "//src/main/kotlin/org/wfanet/measurement/loadtest/dataprovider:gcs_edp_simulator_runner_image",
        repository = _PREFIX + "/loadtest/edp-simulator",
    ),
]

# List of image build rules that are only used locally (e.g. in Kind).
LOCAL_IMAGES = [
    struct(
        name = "forwarded_storage_liquid_legions_v2_mill_daemon_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/daemon/mill/liquidlegionsv2:forwarded_storage_liquid_legions_v2_mill_daemon_image",
    ),
    struct(
        name = "forwarded_storage_computation_control_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/server:forwarded_storage_computation_control_server_image",
    ),
    struct(
        name = "forwarded_storage_spanner_computations_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/server:forwarded_storage_spanner_computations_server_image",
    ),
    struct(
        name = "forwarded_storage_requisition_fulfillment_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/server:forwarded_storage_requisition_fulfillment_server_image",
    ),
    struct(
        name = "forwarded_storage_frontend_simulator_runner_image",
        image = "//src/main/kotlin/org/wfanet/measurement/loadtest/frontend:forwarded_storage_frontend_simulator_runner_image",
    ),
    struct(
        name = "forwarded_storage_edp_simulator_runner_image",
        image = "//src/main/kotlin/org/wfanet/measurement/loadtest/dataprovider:forwarded_storage_edp_simulator_runner_image",
    ),
    struct(
        name = "fake_storage_server_image",
        image = "@wfa_common_jvm//src/main/kotlin/org/wfanet/measurement/storage/filesystem:server_image",
    ),
]

REPORTING_COMMON_IMAGES = [
    struct(
        name = "reporting_v1alpha_public_api_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/reporting/deploy/common/server:v1alpha_public_api_server_image",
        repository = _PREFIX + "/reporting/v1alpha-public-api",
    ),
]

REPORTING_LOCAL_IMAGES = [
    struct(
        name = "reporting_data_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/reporting/deploy/postgres/server:postgres_reporting_data_server_image",
    ),
    struct(
        name = "reporting_postgres_update_schema_image",
        image = "//src/main/kotlin/org/wfanet/measurement/reporting/deploy/postgres/tools:update_schema_image",
    ),
]

REPORTING_GKE_IMAGES = [
    struct(
        name = "gcloud_reporting_data_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/reporting/deploy/gcloud/postgres/server:gcloud_postgres_reporting_data_server_image",
        repository = _PREFIX + "/reporting/postgres-data-server",
    ),
    struct(
        name = "gcloud_reporting_postgres_update_schema_image",
        image = "//src/main/kotlin/org/wfanet/measurement/reporting/deploy/gcloud/postgres/tools:update_schema_image",
        repository = _PREFIX + "/reporting/postgres-update-schema",
    ),
]

ALL_GKE_IMAGES = COMMON_IMAGES + GKE_IMAGES + REPORTING_COMMON_IMAGES + REPORTING_GKE_IMAGES

ALL_LOCAL_IMAGES = COMMON_IMAGES + LOCAL_IMAGES + REPORTING_COMMON_IMAGES + REPORTING_LOCAL_IMAGES
