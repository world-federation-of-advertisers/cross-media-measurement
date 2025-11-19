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

from src.main.proto.wfa.measurement.internal.reporting.postprocessing import \
    report_summary_v2_pb2
from src.main.proto.wfa.measurement.internal.reporting.postprocessing import \
    report_post_processor_result_pb2
from tools import report_conversion
from tools.post_process_report_summary_v2 import ReportSummaryV2Processor
from wfa.measurement.internal.reporting.v2 import report_results_service_pb2
from wfa.measurement.internal.reporting.v2 import report_results_service_pb2_grpc
from wfa.measurement.internal.reporting.v2 import reporting_sets_service_pb2
from wfa.measurement.internal.reporting.v2 import reporting_sets_service_pb2_grpc
from wfa.measurement.internal.reporting.v2 import result_group_pb2

ReportingSet = reporting_sets_service_pb2.ReportingSet
ReportPostProcessorStatus = report_post_processor_result_pb2.ReportPostProcessorStatus
ReportPostProcessorResult = report_post_processor_result_pb2.ReportPostProcessorResult
ReportSummaryV2 = report_summary_v2_pb2.ReportSummaryV2
GetNoisyResultValuesRequest = report_results_service_pb2.GetNoisyResultValuesRequest

MeasurementPolicy: TypeAlias = str
AddDenoisedResultValuesRequest = report_results_service_pb2.AddDenoisedResultValuesRequest
BatchGetReportingSetsRequest = reporting_sets_service_pb2.BatchGetReportingSetsRequest

FLAGS = flags.FLAGS

flags.DEFINE_boolean("debug", False, "Enable debug mode.")


class PostProcessReportResult:
    """Correct a report result and write the denoised results to the spanner.

    This class is responsible for:
    1. Fetching a report result from the `ReportResults` service.
    2. Converting the `ReportResult` into a list of `ReportSummaryV2` messages.
    3. Processing each `ReportSummaryV2`.
    4. Updating the `ReportSummaryV2` messages with the corrected measurement
       values.
    5. Write the denoised results to the spanner.
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
    ) -> None:
        """Executes the full post-processing workflow.

        Args:
            cmms_measurement_consumer_id: The Measurement Consumer ID.
            external_report_result_id: The external ID of the report result.
        """
        # Gets the report result.
        report_result = self._get_report_result(cmms_measurement_consumer_id,
                                                external_report_result_id)

        # Gets the set of the external_reporting_set_ids from the reporting sets.
        external_reporting_set_ids = {
            result.key.external_reporting_set_id
            for result in report_result.reporting_set_results
        }
        external_reporting_set_id_map = self._get_external_reporting_set_id_map(
            cmms_measurement_consumer_id, external_reporting_set_ids)

        # Converts report result to a list of report summary v2.
        report_summaries = report_conversion.get_report_summary_v2_from_report_result(
            report_result, external_reporting_set_id_map)

        if not report_summaries:
            logging.info("No report summaries were generated.")
            return

        # Runs report post processor on each report summary v2.
        all_updated_measurements = {}
        for summary in report_summaries:
            processor = ReportSummaryV2Processor(summary)
            result = processor.process()
            if result.status.status_code == ReportPostProcessorStatus.SUCCESS:
                all_updated_measurements.update(result.updated_measurements)
            else:
                logging.error(
                    "Noise correction failed for a report summary with status:"
                    f" {result.status}")

        # Creates AddDenoisedResultValuesRequest for each report summary.
        requests = self._create_add_denoised_result_values_requests(
            report_summaries, all_updated_measurements)

        # Writes the denoised results to the spanner.
        for request in requests:
            self._report_results_stub.AddDenoisedResultValues(request)

        logging.info("Successfully added all denoised results.")

    def _get_report_result(self, cmms_measurement_consumer_id: str,
                           external_report_result_id: int):
        """Fetches the report result using the internal API."""
        logging.info(f"Fetching report result {external_report_result_id}...")
        request = GetNoisyResultValuesRequest(
            cmms_measurement_consumer_id=cmms_measurement_consumer_id,
            external_report_result_id=external_report_result_id,
        )
        return self._report_results_stub.GetNoisyResultValues(request)

    def _get_external_reporting_set_id_map(
            self, cmms_measurement_consumer_id: str,
            external_reporting_set_ids: Iterable[str]) -> dict[str, set[str]]:
        """Fetches reporting sets and extracts primitive reporting set IDs.

        Args:
            cmms_measurement_consumer_id: The MC's ID.
            external_reporting_set_ids: A list of external reporting set IDs.

        Returns:
            A dictionary mapping each external reporting set ID to a set of its
            underlying primitive reporting set IDs.
        """
        if not external_reporting_set_ids:
            return {}
        request = BatchGetReportingSetsRequest(
            cmms_measurement_consumer_id=cmms_measurement_consumer_id,
            external_reporting_set_ids=external_reporting_set_ids,
        )
        response = self._reporting_sets_stub.BatchGetReportingSets(request)

        reporting_set_map = {
            reporting_set.external_reporting_set_id: reporting_set
            for reporting_set in response.reporting_sets
        }

        def _get_primitive_ids(reporting_set: ReportingSet) -> set[str]:
            """Recursively finds all primitive reporting set IDs."""
            # A reporting set is composed of a list of weighted subset unions,
            # each of which references a primitive reporting set. We extract
            # the external_reporting_set_id from each of these.
            primitive_ids = set()
            for weighted_subset_union in reporting_set.weighted_subset_unions:
                primitive_ids.add(
                    weighted_subset_union.external_reporting_set_id)
            return primitive_ids

        return {
            reporting_set_id:
            _get_primitive_ids(reporting_set_map[reporting_set_id])
            for reporting_set_id in external_reporting_set_ids
            if reporting_set_id in reporting_set_map
        }

    def _create_add_denoised_result_values_requests(
        self,
        report_summaries: list[ReportSummaryV2],
        updated_measurements: dict[str, int],
    ) -> list[AddDenoisedResultValuesRequest]:
        """Creates AddDenoisedResultValuesRequest messages from corrected measurements.

        Args:
            report_summaries: The list of report summaries that were processed.
            updated_measurements: A dictionary of corrected measurement values.

        Returns:
            A list of AddDenoisedResultValuesRequest messages.
        """
        if not updated_measurements:
            logging.warning(
                "No updated measurements were provided; skipping update.")
            return []

        requests = []
        for summary in report_summaries:
            request = AddDenoisedResultValuesRequest(
                cmms_measurement_consumer_id=summary.
                cmms_measurement_consumer_id,
                external_report_result_id=int(
                    summary.external_report_result_id),
            )

            for set_result in summary.report_summary_set_results:
                denoised_set_result = request.reporting_set_results[
                    set_result.external_reporting_set_id]

                # This assumes that the windows in cumulative_results and
                # non_cumulative_results can be uniquely identified by their
                # metric names, which are constructed to be unique.
                # We will group them by a generated window key.
                # TODO(lephi): Find a better way to get the window key.

                for window_result in set_result.cumulative_results:
                    if window_result.reach.metric in updated_measurements:
                        # For now, we only handle cumulative reach.
                        pass

                for window_result in set_result.non_cumulative_results:
                    metric_set = result_group_pb2.ResultGroup.MetricSet.BasicMetricSet(
                    )
                    if window_result.reach.metric in updated_measurements:
                        metric_set.reach_value = updated_measurements[
                            window_result.reach.metric]

            if request.reporting_set_results:
                requests.append(request)

        return requests
