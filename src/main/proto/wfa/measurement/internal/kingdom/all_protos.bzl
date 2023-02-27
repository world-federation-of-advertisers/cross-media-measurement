# Copyright 2021 The Cross-Media Measurement Authors
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

"""Provides convenience lists of proto targets from this Bazel package."""

KINGDOM_INTERNAL_SERVICE_PROTOS = [
    "//src/main/proto/wfa/measurement/internal/kingdom:accounts_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:api_keys_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:certificates_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:computation_participants_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:data_providers_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:event_groups_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:event_group_metadata_descriptors_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:exchanges_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:exchange_step_attempts_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:exchange_steps_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:measurement_consumers_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:measurement_log_entries_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:measurements_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:model_providers_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:public_keys_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:recurring_exchanges_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:requisitions_service_kt_jvm_grpc_proto",
]

KINGDOM_INTERNAL_ENTITY_PROTOS = [
    "//src/main/proto/wfa/measurement/internal/kingdom:account_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:api_key_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:certificate_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:computation_participant_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:crypto_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:data_provider_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:differential_privacy_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:duchy_id_config_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:duchy_protocol_config_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:event_group_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:event_group_metadata_descriptor_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:exchange_details_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:exchange_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:exchange_step_attempt_details_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:exchange_step_attempt_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:exchange_step_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:exchange_workflow_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:measurement_consumer_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:measurement_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:measurement_log_entry_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:duchy_measurement_log_entry_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:state_transition_measurement_log_entry_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:model_provider_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:protocol_config_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/common:provider_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:recurring_exchange_details_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:recurring_exchange_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/kingdom:error_code_kt_jvm_proto",
]

KINGDOM_INTERNAL_PROTOS = KINGDOM_INTERNAL_ENTITY_PROTOS + KINGDOM_INTERNAL_SERVICE_PROTOS
