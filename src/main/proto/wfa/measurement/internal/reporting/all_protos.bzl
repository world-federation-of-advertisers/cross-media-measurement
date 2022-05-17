# Copyright 2022 The Cross-Media Measurement Authors
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

REPORTING_INTERNAL_SERVICE_PROTOS = [
    "//src/main/proto/wfa/measurement/internal/reporting:reporting_sets_service_kt_jvm_grpc_proto",
    "//src/main/proto/wfa/measurement/internal/reporting:reports_service_kt_jvm_grpc_proto",
]

REPORTING_INTERNAL_ENTITY_PROTOS = [
    "//src/main/proto/wfa/measurement/internal/reporting:reporting_set_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/reporting:metric_kt_jvm_proto",
    "//src/main/proto/wfa/measurement/internal/reporting:report_kt_jvm_proto",
]

REPORTING_INTERNAL_PROTOS = REPORTING_INTERNAL_ENTITY_PROTOS + REPORTING_INTERNAL_SERVICE_PROTOS
