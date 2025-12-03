# Copyright 2025 The Cross-Media Measurement Authors
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

# Copyright 2025 The Cross-Media Measurement Authors
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
"""A tool for fetching, correcting, and updating a report."""

from typing import TypeAlias
from collections.abc import Iterable

from absl import flags
from absl import logging

from wfa.measurement.internal.reporting.postprocessing import \
    report_summary_v2_pb2
from wfa.measurement.internal.reporting.postprocessing import \
    report_post_processor_result_pb2
from tools import report_conversion
from tools import post_process_report_summary_v2
from wfa.measurement.internal.reporting.v2 import report_result_pb2
from wfa.measurement.internal.reporting.v2 import report_results_service_pb2
from wfa.measurement.internal.reporting.v2 import report_results_service_pb2_grpc
from wfa.measurement.internal.reporting.v2 import reporting_set_pb2
from wfa.measurement.internal.reporting.v2 import reporting_sets_service_pb2
from wfa.measurement.internal.reporting.v2 import reporting_sets_service_pb2_grpc
from wfa.measurement.internal.reporting.v2 import result_group_pb2

ReportingSet = reporting_set_pb2.ReportingSet
ReportPostProcessorStatus = report_post_processor_result_pb2.ReportPostProcessorStatus
ReportPostProcessorResult = report_post_processor_result_pb2.ReportPostProcessorResult
ReportSummaryV2 = report_summary_v2_pb2.ReportSummaryV2
ReportSummarySetResult = ReportSummaryV2.ReportSummarySetResult
ReportSummaryWindowResult = ReportSummarySetResult.ReportSummaryWindowResult
ReportSummaryV2Processor = post_process_report_summary_v2.ReportSummaryV2Processor
ListReportingSetResultsRequest = report_results_service_pb2.ListReportingSetResultsRequest

MeasurementPolicy: TypeAlias = str
AddProcessedResultValuesRequest = report_results_service_pb2.AddProcessedResultValuesRequest
BasicMetricSet = result_group_pb2.ResultGroup.MetricSet.BasicMetricSet
BatchGetReportingSetsRequest = reporting_sets_service_pb2.BatchGetReportingSetsRequest
ReportingWindowEntry = AddProcessedResultValuesRequest.ProcessedReportingSetResult.ReportingWindowEntry
ReportingSetResult = report_result_pb2.ReportingSetResult


class PostProcessReportResult:
    """Correct a report result and write the processed results to the spanner.
    This class is responsible for:
    1. Fetching a report result from the `ReportResults` service.
    2. Converting the `ReportResult` into a list of `ReportSummaryV2` messages.
    3. Processing each `ReportSummaryV2`.
    4. Updating the `ReportSummaryV2` messages with the corrected measurement
       values.
    5. Write the processed results to the spanner.
    """

    def __init__(
        self,
        report_results_stub: report_results_service_pb2_grpc.ReportResultsStub,
        reporting_sets_stub: reporting_sets_service_pb2_grpc.ReportingSetsStub,
    ):
        self._report_results_stub = report_results_stub
        self._reporting_sets_stub = reporting_sets_stub

    def process(
        self,
        cmms_measurement_consumer_id: str,
        external_report_result_id: int,
    ) -> AddProcessedResultValuesRequest:
        """Executes the full post-processing workflow.
        Args:
            cmms_measurement_consumer_id: The Measurement Consumer ID.
            external_report_result_id: The external ID of the report result.
        """

        # TODO(@ple13): Provide the implementation.
        return None