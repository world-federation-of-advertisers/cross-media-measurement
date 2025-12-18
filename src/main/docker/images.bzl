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
        name = "duchy_spanner_update_schema_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/spanner/tools:update_schema_image",
        repository = _PREFIX + "/duchy/spanner-update-schema",
    ),
    struct(
        name = "duchy_postgres_update_schema_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/postgres/tools:update_schema_image",
        repository = _PREFIX + "/duchy/postgres-update-schema",
    ),
    struct(
        name = "duchy_computations_cleaner_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/job:computations_cleaner_image",
        repository = _PREFIX + "/duchy/computations-cleaner",
    ),
    struct(
        name = "duchy_mill_job_scheduler_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/daemon/mill:job_scheduler_image",
        repository = _PREFIX + "/duchy/mill-job-scheduler",
    ),
    struct(
        name = "kingdom_data_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/kingdom/deploy/gcloud/server:gcp_kingdom_data_server_image",
        repository = _PREFIX + "/kingdom/data-server",
    ),
    struct(
        name = "kingdom_completed_measurements_deletion_image",
        image = "//src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/job:completed_measurements_deletion_image",
        repository = _PREFIX + "/kingdom/completed-measurements-deletion",
    ),
    struct(
        name = "kingdom_exchanges_deletion_image",
        image = "//src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/job:exchanges_deletion_image",
        repository = _PREFIX + "/kingdom/exchanges-deletion",
    ),
    struct(
        name = "kingdom_pending_measurements_cancellation_image",
        image = "//src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/job:pending_measurements_cancellation_image",
        repository = _PREFIX + "/kingdom/pending-measurements-cancellation",
    ),
    struct(
        name = "measurement_system_prober_image",
        image = "//src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/job:measurement_system_prober_image",
        repository = _PREFIX + "/prober/measurement-system-prober",
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
    struct(
        name = "edp_simulator_runner_image",
        image = "//src/main/kotlin/org/wfanet/measurement/loadtest/dataprovider:edp_simulator_runner_image",
        repository = _PREFIX + "/simulator/edp",
    ),
    struct(
        name = "legacy_metadata_edp_simulator_runner_image",
        image = "//src/main/kotlin/org/wfanet/measurement/loadtest/dataprovider:legacy_metadata_edp_simulator_runner_image",
        repository = _PREFIX + "/simulator/legacy-metadata-edp",
    ),
    struct(
        name = "access_public_api_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/access/deploy/common/server:public_api_server_image",
        repository = _PREFIX + "/access/public-api",
    ),
    struct(
        name = "access_internal_api_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/access/deploy/gcloud/spanner:internal_api_server_image",
        repository = _PREFIX + "/access/internal-api",
    ),
    struct(
        name = "access_update_schema_image",
        image = "//src/main/kotlin/org/wfanet/measurement/access/deploy/gcloud/spanner/tools:update_schema_image",
        repository = _PREFIX + "/access/update-schema",
    ),
    struct(
        name = "population_requisition_fulfiller_image",
        image = "//src/main/kotlin/org/wfanet/measurement/populationdataprovider:population_requisition_fulfiller_daemon_image",
        repository = _PREFIX + "/data-provider/population-requisition-fulfiller",
    ),
    struct(
        name = "edp_aggregator_system_api_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/common/server:system_api_server_image",
        repository = _PREFIX + "/edp-aggregator/system-api",
    ),
    struct(
        name = "edp_aggregator_internal_api_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/spanner:internal_api_server_image",
        repository = _PREFIX + "/edp-aggregator/internal-api",
    ),
    struct(
        name = "edp_aggregator_update_schema_image",
        image = "//src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/spanner/tools:update_schema_image",
        repository = _PREFIX + "/edp-aggregator/update-schema",
    ),
]

# List of specs for all Docker containers to push to a container registry.
# These are only used on GKE.
GKE_IMAGES = [
    struct(
        name = "gcs_herald_daemon_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/daemon/herald:gcs_herald_daemon_image",
        repository = _PREFIX + "/duchy/herald",
    ),
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
        name = "duchy_gcs_postgres_internal_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/server:gcs_postgres_internal_server_image",
        repository = _PREFIX + "/duchy/postgres-internal-server",
    ),
    struct(
        name = "duchy_requisition_fulfillment_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/server:gcs_requisition_fulfillment_server_image",
        repository = _PREFIX + "/duchy/requisition-fulfillment",
    ),
    struct(
        name = "duchy_liquid_legions_v2_mill_job_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/job/mill/liquidlegionsv2:gcs_liquid_legions_v2_mill_job_image",
        repository = _PREFIX + "/duchy/liquid-legions-v2-mill",
    ),
    struct(
        name = "duchy_honest_majority_share_shuffle_mill_job_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/job/mill/shareshuffle:gcs_honest_majority_share_shuffle_mill_job_image",
        repository = _PREFIX + "/duchy/honest-majority-share-shuffle-mill",
    ),
    struct(
        name = "duchy_trus_tee_mill_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/daemon/mill/trustee:gcs_trus_tee_mill_daemon_image",
        repository = _PREFIX + "/duchy/trus-tee-mill",
    ),
    struct(
        name = "duchy_gcloud_postgres_update_schema_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/postgres/tools:update_schema_image",
        repository = _PREFIX + "/duchy/gcloud-postgres-update-schema",
    ),
    struct(
        name = "kingdom_operational_metrics_export_image",
        image = "//src/main/kotlin/org/wfanet/measurement/kingdom/deploy/gcloud/job:operational_metrics_export_job_image",
        repository = _PREFIX + "/kingdom/bigquery-operational-metrics",
    ),
]

# List of specs for all Docker containers to push to a container registry.
# These are only used on EKS.
EKS_IMAGES = [
    struct(
        name = "s3_herald_daemon_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/aws/daemon/herald:s3_herald_daemon_image",
        repository = _PREFIX + "/duchy/aws-herald",
    ),
    struct(
        name = "duchy_s3_computation_control_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/aws/server:s3_computation_control_server_image",
        repository = _PREFIX + "/duchy/aws-computation-control",
    ),
    struct(
        name = "duchy_s3_postgres_duchy_data_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/aws/server:s3_postgres_duchy_data_server_image",
        repository = _PREFIX + "/duchy/aws-postgres-internal-server",
    ),
    struct(
        name = "duchy_s3_requisition_fulfillment_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/aws/server:s3_requisition_fulfillment_server_image",
        repository = _PREFIX + "/duchy/aws-requisition-fulfillment",
    ),
    struct(
        name = "duchy_s3_liquid_legions_v2_mill_job_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/aws/job/mill/liquidlegionsv2:s3_liquid_legions_v2_mill_job_image",
        repository = _PREFIX + "/duchy/aws-liquid-legions-v2-mill",
    ),
    struct(
        name = "duchy_s3_honest_majority_share_shuffle_mill_job_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/aws/job/mill/shareshuffle:s3_honest_majority_share_shuffle_mill_job_image",
        repository = _PREFIX + "/duchy/aws-honest-majority-share-shuffle-mill",
    ),
    struct(
        name = "duchy_aws_postgres_update_schema_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/aws/postgres/tools:update_schema_image",
        repository = _PREFIX + "/duchy/aws-postgres-update-schema",
    ),
]

# List of image build rules that are only used locally (e.g. in Kind).
LOCAL_IMAGES = [
    struct(
        name = "forwarded_storage_herald_daemon_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/daemon/herald:forwarded_storage_herald_daemon_image",
        repository = _PREFIX + "/duchy/local-herald",
    ),
    struct(
        name = "forwarded_storage_liquid_legions_v2_mill_job_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/job/mill/liquidlegionsv2:forwarded_storage_liquid_legions_v2_mill_job_image",
        repository = _PREFIX + "/duchy/local-liquid-legions-v2-mill",
    ),
    struct(
        name = "forwarded_storage_honest_majority_share_shuffle_mill_job_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/job/mill/shareshuffle:forwarded_storage_honest_majority_share_shuffle_mill_job_image",
        repository = _PREFIX + "/duchy/local-honest-majority-share-shuffle-mill",
    ),
    struct(
        name = "forwarded_storage_computation_control_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/server:forwarded_storage_computation_control_server_image",
        repository = _PREFIX + "/duchy/local-computation-control",
    ),
    struct(
        name = "forwarded_storage_spanner_computations_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/server:forwarded_storage_spanner_computations_server_image",
        repository = _PREFIX + "/duchy/local-spanner-computations",
    ),
    struct(
        name = "forwarded_storage_postgres_data_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/server:forwarded_storage_postgres_duchy_data_server_image",
        repository = _PREFIX + "/duchy/local-postgres-internal-server",
    ),
    struct(
        name = "forwarded_storage_requisition_fulfillment_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/server:forwarded_storage_requisition_fulfillment_server_image",
        repository = _PREFIX + "/duchy/local-requisition-fulfillment",
    ),
    struct(
        name = "fake_storage_server_image",
        image = "@wfa_common_jvm//src/main/kotlin/org/wfanet/measurement/storage/filesystem:server_image",
        repository = _PREFIX + "/emulator/local-storage",
    ),
]

REPORTING_V2_COMMON_IMAGES = [
    struct(
        name = "reporting_v2alpha_public_api_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/reporting/deploy/v2/common/server:v2alpha_public_api_server_image",
        repository = _PREFIX + "/reporting/v2/v2alpha-public-api",
    ),
    struct(
        name = "report_scheduling_image",
        image = "//src/main/kotlin/org/wfanet/measurement/reporting/deploy/v2/common/job:report_scheduling_job_executor_image",
        repository = _PREFIX + "/reporting/v2/report-scheduling",
    ),
    struct(
        name = "basic_reports_reports_image",
        image = "//src/main/kotlin/org/wfanet/measurement/reporting/deploy/v2/common/job:basic_reports_reports_job_executor_image",
        repository = _PREFIX + "/reporting/v2/basic-reports-reports",
    ),
    struct(
        name = "report_result_post_processor_image",
        image = "//src/main/python/wfa/measurement/reporting/deploy/v2/common/job:post_process_report_result_job_executor_image",
        repository = _PREFIX + "/reporting/v2/report-result-post-processor",
    ),
    struct(
        name = "reporting_spanner_update_schema_image",
        image = "//src/main/kotlin/org/wfanet/measurement/reporting/deploy/v2/gcloud/spanner/tools:update_schema_image",
        repository = _PREFIX + "/reporting/v2/spanner-update-schema",
    ),
    struct(
        name = "reporting_grpc_gateway_image",
        repository = _PREFIX + "/reporting/grpc-gateway",
        image = "//src/main/go/reporting:grpc_gateway_image",
    ),
]

REPORTING_V2_LOCAL_IMAGES = [
    struct(
        name = "internal_reporting_v2_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/reporting/deploy/v2/common/server:internal_reporting_server_image",
        repository = _PREFIX + "/reporting/v2/internal-server",
    ),
    struct(
        name = "reporting_v2_postgres_update_schema_image",
        image = "//src/main/kotlin/org/wfanet/measurement/reporting/deploy/v2/postgres/tools:update_schema_image",
        repository = _PREFIX + "/reporting/v2/postgres-update-schema",
    ),
]

REPORTING_V2_GKE_IMAGES = [
    struct(
        name = "gcloud_reporting_v2_internal_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/reporting/deploy/v2/gcloud/server:gcloud_internal_reporting_server_image",
        repository = _PREFIX + "/reporting/v2/gcloud-internal-server",
    ),
    struct(
        name = "gcloud_reporting_v2_postgres_update_schema_image",
        image = "//src/main/kotlin/org/wfanet/measurement/reporting/deploy/v2/gcloud/postgres/tools:update_schema_image",
        repository = _PREFIX + "/reporting/v2/gcloud-postgres-update-schema",
    ),
]

SECURE_COMPUTATION_COMMON_IMAGES = [
    struct(
        name = "secure_computation_public_api_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/common/server:public_api_server_image",
        repository = _PREFIX + "/secure-computation/public-api",
    ),
]

SECURE_COMPUTATION_GKE_IMAGES = [
    struct(
        name = "gcloud_secure_computation_internal_api_server_image",
        image = "//src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/gcloud/spanner:internal_api_server_image",
        repository = _PREFIX + "/secure-computation/internal-server",
    ),
    struct(
        name = "gcloud_secure_computation_update_schema_image",
        image = "//src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/gcloud/spanner/tools:update_schema_image",
        repository = _PREFIX + "/secure-computation/update-schema",
    ),
]

SECURE_COMPUTATION_TEE_APP_IMAGES = [
    struct(
        name = "gcloud_results_fulfiller_app",
        image = "//src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller:results_fulfiller_image",
        repository = _PREFIX + "/edp-aggregator/results_fulfiller",
    ),
]

ALL_SECURE_COMPUTATION_GKE_IMAGES = SECURE_COMPUTATION_COMMON_IMAGES + SECURE_COMPUTATION_GKE_IMAGES

ALL_GKE_IMAGES = COMMON_IMAGES + GKE_IMAGES + REPORTING_V2_COMMON_IMAGES + REPORTING_V2_GKE_IMAGES + ALL_SECURE_COMPUTATION_GKE_IMAGES

ALL_LOCAL_IMAGES = COMMON_IMAGES + LOCAL_IMAGES + REPORTING_V2_COMMON_IMAGES + REPORTING_V2_LOCAL_IMAGES

ALL_IMAGES = COMMON_IMAGES + LOCAL_IMAGES + GKE_IMAGES + REPORTING_V2_COMMON_IMAGES + REPORTING_V2_LOCAL_IMAGES + REPORTING_V2_GKE_IMAGES + EKS_IMAGES + SECURE_COMPUTATION_COMMON_IMAGES + SECURE_COMPUTATION_GKE_IMAGES

ALL_REPORTING_GKE_IMAGES = REPORTING_V2_COMMON_IMAGES + REPORTING_V2_GKE_IMAGES

ALL_EKS_IMAGES = COMMON_IMAGES + EKS_IMAGES

ALL_TEE_APP_GKE_IMAGES = SECURE_COMPUTATION_TEE_APP_IMAGES
