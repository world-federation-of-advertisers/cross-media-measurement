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

from typing import Any
from typing import Optional
from typing import TypeAlias
from collections.abc import Iterable

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
    ) -> Optional[AddProcessedResultValuesRequest]:
        """Executes the post-processing workflow.

        A list of reporting set results is fetched and groups by their
        dimension. Each group forms a report summary and will be processed. At
        the end, the combined updated measurements will be used to generate an
        AddProcessedResultValuesRequest.

        Args:
            cmms_measurement_consumer_id: The Measurement Consumer ID.
            external_report_result_id: The external ID of the report result.

        Returns:
            An AddProcessedResultValuesRequest message or None if there is no
            reporting set results. If any of the report summaries is failed to
            be processed, an exception will be raised.
        """
        # Gets the report result.
        reporting_set_results: list[
            ReportingSetResult] = self._get_report_result(
                cmms_measurement_consumer_id, external_report_result_id)

        # Gets the set of the external_reporting_set_ids from the reporting sets.
        external_reporting_set_ids: set[str] = {
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
        # TODO(@ple13): Write the status to the output log.
        all_updated_measurements: dict[str, float] = {}
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

        # Creates AddProcessedResultValuesRequest for each report summary.
        request = self._create_add_processed_result_values_requests(
            report_summaries, all_updated_measurements)

        logging.info("Successfully added all processed results.")

        return request

    def _get_report_result(
            self, cmms_measurement_consumer_id: str,
            external_report_result_id: int) -> list[ReportingSetResult]:
        """Fetches all reporting set results for a given report result.

        Args:
            cmms_measurement_consumer_id: The Measurement Consumer ID.
            external_report_result_id: The external ID of the report result.

        Returns:
            A list of ReportingSetResult messages.
        """
        logging.info(f"Fetching report result {external_report_result_id}...")
        request = ListReportingSetResultsRequest(
            cmms_measurement_consumer_id=cmms_measurement_consumer_id,
            external_report_result_id=external_report_result_id,
            view=report_result_pb2.ReportingSetResultView.
            REPORTING_SET_RESULT_VIEW_UNPROCESSED,
        )
        results: list[ReportingSetResult] = []
        response = self._report_results_stub.ListReportingSetResults(request)
        results.extend(response.reporting_set_results)

        # TODO(@ple13): Support pagination when the internal API supports it.

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

        reporting_set_map: dict[str, ReportingSet] = {
            reporting_set.external_reporting_set_id: reporting_set
            for reporting_set in response.reporting_sets
        }

        def _get_primitive_ids(reporting_set: ReportingSet) -> set[str]:
            """Recursively finds all primitive reporting set IDs."""
            if reporting_set.WhichOneof("value") == "primitive":
                return {reporting_set.external_reporting_set_id}
            else:
                primitive_ids: set[str] = set()
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

    def _process_window_results(
        self,
        window_results: list[ReportSummaryWindowResult],
        prefix: str,
        updated_measurements: dict[str, int],
        results_by_window: dict[str, dict[str, Any]],
    ):
        """Processes a list of window results and groups them by window key.

        This helper function iterates through a list of
        `ReportSummaryWindowResult`, extracts the corrected measurements, and
        organizes them into the `results_by_window` dictionary.

        Args:
            window_results: A list of `ReportSummaryWindowResult` to process.
            prefix: The prefix to use for keys (e.g., 'cumulative').
            updated_measurements: A map of corrected measurement names to values.
            results_by_window: The dictionary to populate with grouped results.
        """
        for result in window_results:
            window_key_str = result.key.SerializeToString()
            if window_key_str not in results_by_window:
                results_by_window[window_key_str] = {'key': result.key}

            if result.HasField('reach'):
                results_by_window[window_key_str][f'{prefix}_reach'] = (
                    updated_measurements[result.reach.metric])
            if result.HasField('impression_count'):
                results_by_window[window_key_str][f'{prefix}_impressions'] = (
                    updated_measurements[result.impression_count.metric])
            if result.HasField('frequency'):
                frequency_key = f'{prefix}_frequency'
                if frequency_key not in results_by_window[window_key_str]:
                    results_by_window[window_key_str][frequency_key] = {}
                for bin_label, bin_result in result.frequency.bins.items():
                    name = f'{result.frequency.metric}-bin-{bin_label}'
                    results_by_window[window_key_str][frequency_key][
                        bin_label] = updated_measurements.get(
                            name, bin_result.value)

    def _group_results_by_window(
        self,
        reporting_summary_set_result: ReportSummarySetResult,
        updated_measurements: dict[str, int],
    ) -> dict[str, dict[str, Any]]:
        """Groups corrected results by their reporting window.

        Args:
            reporting_summary_set_result: The `ReportSummarySetResult` to
              process.
            updated_measurements: A map of corrected measurement names to values.

        Returns:
            A dictionary with results grouped by serialized window key.
        """
        results_by_window: dict[str, dict[str, Any]] = {}

        self._process_window_results(
            reporting_summary_set_result.cumulative_results, 'cumulative',
            updated_measurements, results_by_window)

        self._process_window_results(
            reporting_summary_set_result.non_cumulative_results,
            'non_cumulative', updated_measurements, results_by_window)

        if reporting_summary_set_result.HasField('whole_campaign_result'):
            self._process_window_results(
                [reporting_summary_set_result.whole_campaign_result],
                'cumulative', updated_measurements, results_by_window)

        return results_by_window

    def _create_add_processed_result_values_requests(
        self,
        report_summaries: list[ReportSummaryV2],
        updated_measurements: dict[str, int],
    ) -> Optional[AddProcessedResultValuesRequest]:
        """Creates AddProcessedResultValuesRequest messages from corrected measurements.

        Args:
            report_summaries: The list of report summaries that were processed.
            updated_measurements: A dictionary of corrected measurement values.

        Returns:
            An AddProcessedResultValuesRequest message.
        """
        # TODO(@ple13): When no report summary exists, write the error message to the
        # output log.
        if not report_summaries:
            logging.warning(
                "No report summaries were provided; skipping update.")
            return None

        if not updated_measurements:
            logging.warning(
                "No updated measurements were provided; skipping update.")
            return None

        # Gets the cmms_measurement_consumer_id and external_report_result_id
        # from the first report summary.
        report_summary = report_summaries[0]
        request = AddProcessedResultValuesRequest(
            cmms_measurement_consumer_id=report_summary.
            cmms_measurement_consumer_id,
            external_report_result_id=report_summary.external_report_result_id,
        )

        # Generates the AddProcessedResultValuesRequest from all the report
        # summaries.
        for report_summary in report_summaries:
            # Each ReportSummarySetResult corresponds to a ReportingSetResult.
            for reporting_summary_set_result in report_summary.report_summary_set_results:
                processed_set_result = request.reporting_set_results[
                    reporting_summary_set_result.
                    external_reporting_set_result_id]

                # Groups results by their reporting window.
                results_by_window = self._group_results_by_window(
                    reporting_summary_set_result, updated_measurements)

                # Generates the corresponding processed window entries.
                for window_data in results_by_window.values():
                    processed_window_entry = processed_set_result.reporting_window_results.add(
                    )
                    self._populate_processed_window_results(
                        window_data, report_summary.population,
                        processed_window_entry)

        return request

    def _populate_processed_window_results(
        self,
        window_data: dict,
        population: int,
        processed_window_entry: ReportingWindowEntry,
    ) -> None:
        """Populates a single processed window entry with updated metrics.

        Args:
            window_data: A dictionary containing the data for a single window.
            population: The total population for the demographic group.
            processed_window_entry: The `ReportingWindowEntry` to populate with
              updated metrics.
        """
        processed_window_entry.key.CopyFrom(window_data['key'])

        if population == 0:
            raise ValueError('Population must be a positive number.')

        self._populate_basic_metric_set(
            window_data,
            'cumulative',
            population,
            processed_window_entry.value.cumulative_results,
        )

        no_non_cumulative_values = True
        for key, val in window_data.items():
            if key.startswith('non_cumulative'):
                no_non_cumulative_values = False

        if not no_non_cumulative_values:
            self._populate_basic_metric_set(
                window_data,
                'non_cumulative',
                population,
                processed_window_entry.value.non_cumulative_results,
            )

    def _populate_basic_metric_set(
        self,
        window_data: dict[str, Any],
        prefix: str,
        population: int,
        basic_metric_set: BasicMetricSet,
    ) -> None:
        """Populates a BasicMetricSet with calculated and corrected metrics.

        Args:
            window_data: A dictionary containing the data for a single window.
            prefix: The prefix to use for keys (e.g., 'cumulative').
            population: The total population for the demographic group.
            basic_metric_set: The `BasicMetricSet` to populate.
        """
        reach_key = f'{prefix}_reach'
        impressions_key = f'{prefix}_impressions'
        frequency_key = f'{prefix}_frequency'

        reach = round(
            window_data[reach_key]) if reach_key in window_data else None

        impressions = round(window_data[impressions_key]
                            ) if impressions_key in window_data else None

        frequency_values = None

        if frequency_key in window_data:
            sorted_freqs = sorted(window_data[frequency_key].items())
            frequency_values = [val for _, val in sorted_freqs]

        computed_metrics = compute_basic_metric_set(reach, frequency_values,
                                                    impressions, population)

        basic_metric_set.CopyFrom(computed_metrics)


def compute_basic_metric_set(
    reach: int | None,
    frequency_values: list[int] | None,
    impressions: int | None,
    population: int,
) -> BasicMetricSet:
    """Computes a BasicMetricSet from reach, frequencies, and impressions.
 
    Args:
        reach: The reach value.
        frequency_values: The frequency histogram.
        impressions: The impressions.
        population: The total population for the demographic group.

    Returns:
        A populated BasicMetricSet message. If population is not provided, an
        exception will be raised.
    """
    if population <= 0:
        raise ValueError("Population must be a positive number.")

    basic_metric_set = BasicMetricSet()

    if reach:
        basic_metric_set.reach = reach
        basic_metric_set.percent_reach = reach / population * 100

    if impressions:
        basic_metric_set.impressions = impressions
        basic_metric_set.grps = impressions / population * 100
        if basic_metric_set.reach > 0:
            basic_metric_set.average_frequency = (impressions /
                                                  basic_metric_set.reach)

    if frequency_values is not None:
        k_plus_reach_values = [
            round(sum(frequency_values[i:]))
            for i in range(len(frequency_values))
        ]
        basic_metric_set.k_plus_reach.extend(k_plus_reach_values)
        basic_metric_set.percent_k_plus_reach.extend(
            [val / population * 100 for val in k_plus_reach_values])

    return basic_metric_set
