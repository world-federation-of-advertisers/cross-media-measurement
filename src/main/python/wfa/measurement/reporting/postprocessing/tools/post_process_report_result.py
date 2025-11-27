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
ReportSummaryV2Processor = post_process_report_summary_v2.ReportSummaryV2Processor
ListReportingSetResultsRequest = report_results_service_pb2.ListReportingSetResultsRequest

MeasurementPolicy: TypeAlias = str
AddDenoisedResultValuesRequest = report_results_service_pb2.AddDenoisedResultValuesRequest
BatchGetReportingSetsRequest = reporting_sets_service_pb2.BatchGetReportingSetsRequest


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
    ) -> report_results_service_pb2.AddDenoisedResultValuesRequest:
        """Executes the full post-processing workflow.

        Args:
            cmms_measurement_consumer_id: The Measurement Consumer ID.
            external_report_result_id: The external ID of the report result.
        """
        # Gets the report result.
        reporting_set_results = self._get_report_result(
            cmms_measurement_consumer_id, external_report_result_id)

        # Gets the set of the external_reporting_set_ids from the reporting sets.
        external_reporting_set_ids = {
            result.dimension.external_reporting_set_id
            for result in reporting_set_results
        }
        external_reporting_set_id_map = self._get_external_reporting_set_id_map(
            cmms_measurement_consumer_id, external_reporting_set_ids)

        # Converts report result to a list of report summary v2.
        report_summaries = report_conversion.report_summaries_from_reporting_set_results(
            reporting_set_results, external_reporting_set_id_map)

        if not report_summaries:
            logging.info("No report summaries were generated.")
            return

        # Runs report post processor on each report summary v2.
        all_updated_measurements = {}
        for report_summary in report_summaries:
            result = ReportSummaryV2Processor(report_summary).process()
            if result.status.status_code in [
                    ReportPostProcessorStatus.SOLUTION_FOUND_WITH_HIGHS,
                    ReportPostProcessorStatus.SOLUTION_FOUND_WITH_OSQP,
                    ReportPostProcessorStatus.
                    PARTIAL_SOLUTION_FOUND_WITH_HIGHS,
                    ReportPostProcessorStatus.PARTIAL_SOLUTION_FOUND_WITH_OSQP,
            ]:
                all_updated_measurements.update(result.updated_measurements)
            else:
                raise ValueError(
                    "Noise correction failed for a report summary with status:"
                    f" {result.status.status_code}")

        # Creates AddDenoisedResultValuesRequest for each report summary.
        request = self._create_add_denoised_result_values_requests(
            report_summaries, all_updated_measurements)

        logging.info("Successfully added all denoised results.")

        return request

    def _get_report_result(self, cmms_measurement_consumer_id: str,
                           external_report_result_id: int):
        """Fetches all reporting set results for a report result."""
        logging.info(f"Fetching report result {external_report_result_id}...")
        request = ListReportingSetResultsRequest(
            cmms_measurement_consumer_id=cmms_measurement_consumer_id,
            external_report_result_id=external_report_result_id,
            view=report_result_pb2.ReportingSetResultView.
            REPORTING_SET_RESULT_VIEW_NOISY,
        )
        results = []
        response = self._report_results_stub.ListReportingSetResults(request)
        results.extend(response.reporting_set_results)

        # TODO(@ple13): Support pagination.

        return results

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
            if reporting_set.WhichOneof("value") == "primitive":
                return {reporting_set.external_reporting_set_id}
            else:
                primitive_ids = set()
                for weighted_subset_union in reporting_set.weighted_subset_unions:
                    for primitive_reporting_set_base in weighted_subset_union.primitive_reporting_set_bases:
                        primitive_ids.add(primitive_reporting_set_base.
                                          external_reporting_set_id)
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
    ) -> AddDenoisedResultValuesRequest:
        """Creates AddDenoisedResultValuesRequest messages from corrected measurements.

        Args:
            report_summaries: The list of report summaries that were processed.
            updated_measurements: A dictionary of corrected measurement values.

        Returns:
            An AddDenoisedResultValuesRequest message.
        """
        if not report_summaries:
            logging.warning("No report summaries were provided; skipping update.")
            return None

        if not updated_measurements:
            logging.warning(
                "No updated measurements were provided; skipping update.")
            return None

        print(updated_measurements)

        # Gets the cmms_measurement_consumer_id and external_report_result_id
        # from the first report summary.
        report_summary = report_summaries[0]
        request = AddDenoisedResultValuesRequest(
            cmms_measurement_consumer_id=report_summary.cmms_measurement_consumer_id,
            external_report_result_id=report_summary.external_report_result_id,
        )

        for report_summary in report_summaries:
            for reporting_summary_set_result in report_summary.report_summary_set_results:
                denoised_set_result = request.reporting_set_results[
                    reporting_summary_set_result.external_reporting_set_result_id]

                # Groups results by window key.
                results_by_window = {}
                for result in reporting_summary_set_result.cumulative_results:
                    if result.reach.metric in updated_measurements:
                        window_key_str = result.key.SerializeToString()
                        if window_key_str not in results_by_window:
                            results_by_window[window_key_str] = {
                                'key': result.key
                            }
                        results_by_window[window_key_str]['cumulative'] = (
                            updated_measurements[result.reach.metric])
                        for bin_label, bin_result in result.frequency.bins.items(
                        ):
                            if 'cumulative_frequency' not in results_by_window[
                                    window_key_str]:
                                results_by_window[window_key_str][
                                    'cumulative_frequency'] = {}
                            results_by_window[window_key_str][
                                'cumulative_frequency'][
                                    bin_label] = updated_measurements.get(
                                        f'{result.frequency.metric}_{bin_label}',
                                        bin_result.value)

                for result in reporting_summary_set_result.non_cumulative_results:
                    if result.reach.metric in updated_measurements:
                        window_key_str = result.key.SerializeToString()
                        if window_key_str not in results_by_window:
                            results_by_window[window_key_str] = {
                                'key': result.key
                            }
                        results_by_window[window_key_str][
                            'non_cumulative'] = updated_measurements[
                                result.reach.metric]
                        for bin_label, bin_result in result.frequency.bins.items(
                        ):
                            if 'non_cumulative_frequency' not in results_by_window[
                                    window_key_str]:
                                results_by_window[window_key_str][
                                    'non_cumulative_frequency'] = {}
                            results_by_window[window_key_str][
                                'non_cumulative_frequency'][
                                    bin_label] = updated_measurements.get(
                                        f'{result.frequency.metric}_{bin_label}',
                                        bin_result.value)

                if reporting_summary_set_result.whole_campaign_result.reach.metric in updated_measurements:
                    result = reporting_summary_set_result.whole_campaign_result
                    window_key_str = result.key.SerializeToString()
                    if window_key_str not in results_by_window:
                        results_by_window[window_key_str] = {'key': result.key}
                    results_by_window[window_key_str][
                        'cumulative'] = updated_measurements[
                            result.reach.metric]
                    for bin_label, bin_result in result.frequency.bins.items():
                        if 'cumulative_frequency' not in results_by_window[
                                window_key_str]:
                            results_by_window[window_key_str][
                                'cumulative_frequency'] = {}
                        results_by_window[window_key_str][
                            'cumulative_frequency'][
                                bin_label] = updated_measurements.get(
                                    f'{result.frequency.metric}_{bin_label}',
                                    bin_result.value)

                for window_data in results_by_window.values():
                    denoised_window_entry = denoised_set_result.reporting_window_results.add(
                    )
                    denoised_window_entry.key.CopyFrom(window_data['key'])
                    if 'cumulative' in window_data:
                        denoised_window_entry.value.cumulative_results.reach = round(
                            window_data['cumulative'])
                    if 'cumulative_frequency' in window_data:
                        sorted_freqs = sorted(
                            window_data['cumulative_frequency'].items())
                        freq_values = [value for _, value in sorted_freqs]
                        k_plus_reach_values = [
                            sum(freq_values[i:])
                            for i in range(len(freq_values))
                        ]
                        denoised_window_entry.value.cumulative_results.k_plus_reach.extend(
                            [round(val) for val in k_plus_reach_values])
                    if 'non_cumulative' in window_data:
                        denoised_window_entry.value.non_cumulative_results.reach = round(
                            window_data['non_cumulative'])
                    if 'non_cumulative_frequency' in window_data:
                        sorted_freqs = sorted(
                            window_data['non_cumulative_frequency'].items())
                        freq_values = [value for _, value in sorted_freqs]
                        k_plus_reach_values = [
                            sum(freq_values[i:])
                            for i in range(len(freq_values))
                        ]
                        denoised_window_entry.value.non_cumulative_results.k_plus_reach.extend(
                            [round(val) for val in k_plus_reach_values])

        return request
