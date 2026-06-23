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


def _get_cmms_data_provider_id(edp_name: str) -> str:
    """Converts an EDP resource name to a raw CMMS DataProvider ID."""
    return edp_name.split("/")[-1]


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
        ami_mrc_exempted_edps: list[str] | None = None,
    ) -> Optional[AddProcessedResultValuesRequest]:
        """Executes the post-processing workflow.

        A list of reporting set results is fetched and groups by their
        dimension. Each group forms a report summary and will be processed. At
        the end, the combined updated measurements will be used to generate an
        AddProcessedResultValuesRequest.

        Args:
            cmms_measurement_consumer_id: The Measurement Consumer ID.
            external_report_result_id: The external ID of the report result.
            ami_mrc_exempted_edps: The list of EDPs for which
                AMI vs MRC consistency check is disabled.

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

        # Gets the AMI MRC exempted reporting set ids.
        ami_mrc_exempted_reporting_set_ids = self._get_ami_mrc_exempted_reporting_set_id(
            cmms_measurement_consumer_id,
            external_reporting_set_ids,
            ami_mrc_exempted_edps,
        )

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
            result = ReportSummaryV2Processor(
                report_summary,
                ami_mrc_exempted_reporting_set_ids
            ).process()
            if result.status.status_code in [
                    ReportPostProcessorStatus.SOLUTION_FOUND_WITH_HIGHS,
                    ReportPostProcessorStatus.SOLUTION_FOUND_WITH_OSQP,
                    ReportPostProcessorStatus.
                    PARTIAL_SOLUTION_FOUND_WITH_HIGHS,
                    ReportPostProcessorStatus.PARTIAL_SOLUTION_FOUND_WITH_OSQP,
            ]:
                if result.large_corrections:
                    raise ValueError(
                        "Noise correction produced large corrections for a"
                        f" report summary: {list(result.large_corrections)}")
                all_updated_measurements.update(result.updated_measurements)
            else:
                raise ValueError(
                    "Noise correction failed for a report summary with status:"
                    f" {result.status.status_code}")

        # Creates AddProcessedResultValuesRequest for each report summary.
        request = self._create_add_processed_result_values_requests(
            report_summaries, all_updated_measurements)

        # Snaps cross-window equality identities that may have been broken by
        # per-RSR rounding (e.g. whole_campaign.reach vs. last_cumulative_week
        # .reach).
        if request is not None:
            self._reconcile_cross_window_identities(request,
                                                    reporting_set_results)

        logging.info("Successfully added all processed results.")

        return request

    @staticmethod
    def _dimension_key_excluding_metric_frequency_spec(
            dimension: ReportingSetResult.Dimension) -> tuple:
        """Returns a hashable identity key for a Dimension, ignoring its
        metric_frequency_spec selector.

        Two Dimensions that differ only in metric_frequency_spec describe the
        same underlying slice (one whole-campaign, the other weekly cumulative)
        and must agree on the metric values their solver-declared identities
        require.
        """
        iqf_field = dimension.WhichOneof('impression_qualification_filter')
        if iqf_field == 'external_impression_qualification_filter_id':
            iqf_key = ('external',
                       dimension.external_impression_qualification_filter_id)
        elif iqf_field == 'custom':
            iqf_key = ('custom', dimension.custom)
        else:
            iqf_key = ('none', )

        grouping_key = tuple(
            sorted((path, value.SerializeToString())
                   for path, value in dimension.grouping.value_by_path.items()))

        event_filters_key = tuple(
            f.SerializeToString() for f in dimension.event_filters)

        return (
            dimension.external_reporting_set_id,
            dimension.venn_diagram_region_type,
            iqf_key,
            grouping_key,
            event_filters_key,
        )

    def _reconcile_cross_window_identities(
        self,
        request: AddProcessedResultValuesRequest,
        reporting_set_results: list[ReportingSetResult],
    ) -> None:
        """Snaps whole_campaign cumulative reach to match the last weekly
        cumulative reach when both come from the same underlying dimension.

        The QP solver constrains these two measurements to be equal (see
        report.py:_add_cumulative_whole_campaign_relations_to_spec). However,
        because each ReportingSetResult is rounded independently in
        post_process_report_summary_v2.process(), a residual of up to the
        solver TOLERANCE (0.1) can amplify into a 1-unit integer difference.

        Both sides are snapped to min(whole_campaign.reach,
        last_weekly_cumulative.reach). Picking the smaller value avoids
        re-breaking the per-window identity sum(k_plus_reach) <= impressions
        (Issue #4049 Rule 4), which a snap-upward could violate.

        After snapping, derived fields (percent_reach, percent_k_plus_reach,
        average_frequency, grps) are recomputed via _recompute_derived_fields
        -- the same helper compute_basic_metric_set uses -- so both
        derivation paths stay consistent.

        Finally, walks the weekly series newest-to-oldest and clamps any
        earlier window whose reach exceeds its successor's. The snap-down
        on last_weekly can otherwise introduce a Rule 1 (cumulative non-
        decreasing) violation on a report that previously satisfied it.
        """
        # Group reporting_set_result IDs by dimension (excluding the
        # weekly/total selector) so we can match a whole_campaign RSR to its
        # corresponding weekly RSR. If two RSRs share both the dim key and
        # the selector kind (e.g. two weekly cadences for the same
        # dimension), the pair is ambiguous -- log and skip the dimension
        # rather than mis-reconciling.
        dim_to_rsrs: dict[tuple, dict[str, int | None]] = {}
        # Population is needed for the derived-field recompute on each snap.
        population_by_rsr_id: dict[int, int] = {}
        for rsr in reporting_set_results:
            population_by_rsr_id[rsr.external_reporting_set_result_id] = (
                rsr.population_size)
            dim = rsr.dimension
            selector = dim.metric_frequency_spec.WhichOneof('selector')
            if selector not in ('total', 'weekly'):
                continue
            key = self._dimension_key_excluding_metric_frequency_spec(dim)
            bucket = dim_to_rsrs.setdefault(key, {})
            if selector in bucket:
                if bucket[selector] is not None:
                    # First collision for this (dim, selector): log with both
                    # real ids. Subsequent collisions for the same key add no
                    # information (the dim is already skipped) and would log
                    # `ids None and X` -- suppress them.
                    logging.warning(
                        "Multiple ReportingSetResults share dimension key "
                        "with selector=%s (ids %d and %d); skipping "
                        "cross-window reconciliation for this dimension.",
                        selector, bucket[selector],
                        rsr.external_reporting_set_result_id)
                    bucket[selector] = None
            else:
                bucket[selector] = rsr.external_reporting_set_result_id

        for selectors in dim_to_rsrs.values():
            total_id = selectors.get('total')
            weekly_id = selectors.get('weekly')
            if total_id is None or weekly_id is None:
                continue
            total = request.reporting_set_results.get(total_id)
            weekly = request.reporting_set_results.get(weekly_id)
            if total is None or weekly is None:
                continue
            if len(total.reporting_window_results) != 1:
                # A 'total' selector reports over the full reporting interval,
                # so it should have exactly one window. Anything else is an
                # upstream data-model violation -- log so it's traceable
                # rather than silently leaving the cross-window identity
                # unreconciled.
                logging.warning(
                    "Total-selector ReportingSetResult %d has %d reporting "
                    "windows (expected 1); skipping cross-window "
                    "reconciliation for this dimension.",
                    total_id, len(total.reporting_window_results))
                continue
            if not weekly.reporting_window_results:
                logging.warning(
                    "Weekly-selector ReportingSetResult %d has no reporting "
                    "windows; skipping cross-window reconciliation for this "
                    "dimension.", weekly_id)
                continue
            last_weekly = max(
                weekly.reporting_window_results,
                key=lambda w: (w.key.end.year, w.key.end.month, w.key.end.day),
            )
            whole = total.reporting_window_results[0]
            snapped_reach = min(whole.value.cumulative_results.reach,
                                last_weekly.value.cumulative_results.reach)
            total_population = population_by_rsr_id.get(total_id, 0)
            weekly_population = population_by_rsr_id.get(weekly_id, 0)
            if total_population <= 0 or weekly_population <= 0:
                logging.warning(
                    "Missing population_size for RSR (total=%d weekly=%d); "
                    "skipping cross-window reconciliation for this dimension.",
                    total_id, weekly_id)
                continue
            self._snap_cumulative_reach(whole.value.cumulative_results,
                                        snapped_reach, total_population)
            self._snap_cumulative_reach(last_weekly.value.cumulative_results,
                                        snapped_reach, weekly_population)
            # Rule 1 sweep: snapping last_weekly down can leave an earlier
            # weekly window with reach > last_weekly.reach -- a cumulative
            # non-decreasing violation (Issue #4049 Rule 1) the snap itself
            # would otherwise introduce. Walk the weekly windows newest-to-
            # oldest and clamp each earlier window down to its successor's
            # reach. _snap_cumulative_reach only lowers values, so Rule 4
            # (sum(k_plus_reach) <= impressions) stays intact and derived
            # fields stay consistent.
            sorted_weekly = sorted(
                weekly.reporting_window_results,
                key=lambda w: (w.key.end.year, w.key.end.month, w.key.end.day),
            )
            for i in range(len(sorted_weekly) - 2, -1, -1):
                later = sorted_weekly[i + 1].value.cumulative_results
                earlier = sorted_weekly[i].value.cumulative_results
                if earlier.reach > later.reach:
                    self._snap_cumulative_reach(earlier, later.reach,
                                                weekly_population)

    @staticmethod
    def _snap_cumulative_reach(
            cumulative_results: BasicMetricSet, snapped_reach: int,
            population: int) -> None:
        """Snaps reach (and k_plus_reach[0]) to snapped_reach on one side of a
        cross-window pair. Re-clamps the rest of k_plus_reach forward so that
        lowering k_plus_reach[0] does not break the non-increasing invariant
        established by compute_basic_metric_set (Issue #4049 Rule 3 +
        k_plus_reach monotonicity). The clamp only lowers buckets, so it
        cannot re-break sum(k_plus_reach) <= impressions (Rule 4) either.

        After mutating the raw values, derived fields (percent_reach,
        percent_k_plus_reach, average_frequency, grps) are recomputed from
        the same helper compute_basic_metric_set uses, so the two derivation
        paths stay in lock-step (Issue #4049).
        """
        cumulative_results.reach = snapped_reach
        k_plus_reach = cumulative_results.k_plus_reach
        if k_plus_reach:
            k_plus_reach[0] = snapped_reach
            for i in range(1, len(k_plus_reach)):
                if k_plus_reach[i] <= k_plus_reach[i - 1]:
                    break
                k_plus_reach[i] = k_plus_reach[i - 1]
        _recompute_derived_fields(cumulative_results, population)

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

    def _get_ami_mrc_exempted_reporting_set_id(
        self,
        cmms_measurement_consumer_id: str,
        external_reporting_set_ids: Iterable[str],
        ami_mrc_exempted_edps: Iterable[str],
    ) -> list[str]:
        """Gets the reporting set IDs that are exempted from AMI vs MRC check.

        Args:
            cmms_measurement_consumer_id: The MC's ID.
            external_reporting_set_ids: A list of external reporting set IDs.
            ami_mrc_exempted_edps: A list of exempted EDP names or raw IDs.

        Returns:
            A list of external reporting set IDs that belong to the exempted EDPs.
        """
        if not external_reporting_set_ids or not ami_mrc_exempted_edps:
            return []

        request = BatchGetReportingSetsRequest(
            cmms_measurement_consumer_id=cmms_measurement_consumer_id,
            external_reporting_set_ids=external_reporting_set_ids,
        )
        response = self._reporting_sets_stub.BatchGetReportingSets(request)
        # Gets a list of exempted cmms data provider ids from edp names.
        exempted_cmms_data_provider_ids = set(
            _get_cmms_data_provider_id(edp) for edp in ami_mrc_exempted_edps
        )
        exempted_reporting_set_ids = []

        for reporting_set in response.reporting_sets:
            if reporting_set.WhichOneof("value") != "primitive":
                continue
            for key in reporting_set.primitive.event_group_keys:
                if key.cmms_data_provider_id in exempted_cmms_data_provider_ids:
                    exempted_reporting_set_ids.append(
                        reporting_set.external_reporting_set_id)
                    break

        return exempted_reporting_set_ids

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

    if impressions:
        basic_metric_set.impressions = impressions

    if frequency_values is not None:
        k_plus_reach_values = [
            round(sum(frequency_values[i:]))
            for i in range(len(frequency_values))
        ]
        # The post-processor solver returns reach and the frequency
        # histogram as independent floats; rounding each separately can
        # break three algebraic identities consumers rely on:
        #
        # 1. k_plus_reach[0] (the "1+ reach") must equal reach.
        # 2. The weighted sum of frequency buckets must not exceed
        #    impressions (it is conceptually equal, but rounding can push
        #    it slightly above).
        # 3. k_plus_reach is non-increasing: k_plus_reach[i] >=
        #    k_plus_reach[i+1] ("N+ reach" cannot exceed "(N-1)+ reach").
        #
        # Snap to enforce all three. The reach value is the canonical 1+
        # reach, so overwrite k_plus_reach[0]; that overwrite plus
        # independent rounding can leave k_plus_reach[1] > k_plus_reach[0],
        # so forward-clamp each bucket to its predecessor. Then, for the
        # impression identity, absorb any positive residue starting at the
        # highest-frequency bucket (which has the loosest physical
        # interpretation: "saw it at least N times for large N"). If a
        # single bucket can't absorb the full residue (e.g. it would go
        # below zero), cascade into the next-highest bucket. Stop before
        # index 0 so the reach == k_plus_reach[0] identity is preserved
        # even in the (unrealistic) case where the residue exceeds the
        # total capacity of all higher-frequency buckets. The forward-
        # clamp must run before the cascade because clamping reduces
        # sum(k_plus_reach) and thus changes the excess. See Issue #4049.
        if k_plus_reach_values:
            if reach is not None:
                k_plus_reach_values[0] = reach
                for i in range(1, len(k_plus_reach_values)):
                    k_plus_reach_values[i] = min(k_plus_reach_values[i],
                                                 k_plus_reach_values[i - 1])
            if impressions is not None:
                excess = sum(k_plus_reach_values) - impressions
                i = len(k_plus_reach_values) - 1
                while excess > 0 and i >= 1:
                    absorbed = min(excess, k_plus_reach_values[i])
                    k_plus_reach_values[i] -= absorbed
                    excess -= absorbed
                    i -= 1
                if excess > 0:
                    # Reach > impressions in the input -- upstream data is
                    # inconsistent. Preserve k_plus_reach[0] == reach (rule 1)
                    # and leave the impressions identity (rule 2) violated by
                    # the leftover residue, so downstream consumers can see
                    # the input was corrupt.
                    logging.warning(
                        "Could not fully reconcile sum(k_plus_reach) <= "
                        "impressions: leftover residue %d (reach=%d, "
                        "impressions=%d). Preserving reach == "
                        "k_plus_reach[0]; sum(k_plus_reach) will exceed "
                        "impressions by this residue.",
                        excess, reach, impressions)
        basic_metric_set.k_plus_reach.extend(k_plus_reach_values)

    _recompute_derived_fields(basic_metric_set, population)
    return basic_metric_set


def _recompute_derived_fields(bms: BasicMetricSet, population: int) -> None:
    """Single source of truth for BasicMetricSet derived fields.

    percent_X = X / population * 100; average_frequency = impressions / reach;
    percent_k_plus_reach is rebuilt from k_plus_reach. Call after any
    mutation of reach, impressions, or k_plus_reach so the two derivation
    paths (construction in compute_basic_metric_set and post-construction
    mutation in _snap_cumulative_reach) stay in lock-step (Issue #4049).

    Always assigns (rather than only-on-truthy), so a drop of reach or
    impressions to zero clears the now-stale percent / average_frequency
    rather than leaving the old value behind.
    """
    if population <= 0:
        raise ValueError("Population must be a positive number.")
    bms.percent_reach = bms.reach / population * 100 if bms.reach else 0
    bms.grps = (bms.impressions / population * 100) if bms.impressions else 0
    if bms.reach and bms.impressions:
        bms.average_frequency = bms.impressions / bms.reach
    else:
        bms.average_frequency = 0
    del bms.percent_k_plus_reach[:]
    bms.percent_k_plus_reach.extend(
        v / population * 100 for v in bms.k_plus_reach)
