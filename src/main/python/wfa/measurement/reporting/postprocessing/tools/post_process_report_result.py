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
        # TODO(world-federation-of-advertisers/cross-media-measurement#4059):
        # Add a Rule 5 snap pass here (or in a sibling helper) for
        # `ami >= [custom, mrc]` across IQF-varying RSRs that share
        # (reporting_set, venn_region, grouping, event_filters,
        # metric_frequency_spec).
        if request is not None:
            self._reconcile_cross_window_identities(request,
                                                    reporting_set_results)

        logging.info("Successfully added all processed results.")

        return request

    @staticmethod
    def _dimension_key_excluding_metric_frequency_spec(
            rsr: ReportingSetResult) -> tuple:
        """Returns a hashable identity key for an RSR's Dimension, ignoring
        the metric_frequency_spec selector.

        Two RSRs that differ only in metric_frequency_spec describe the same
        underlying slice (one whole-campaign, the other weekly cumulative) and
        must agree on the metric values their solver-declared identities
        require.

        Keys on the server-computed `grouping_dimension_fingerprint` and
        `filter_fingerprint` (populated by SpannerReportResultsService and
        used by the `ReportingSetResultsByDimensions` UNIQUE INDEX as the
        authoritative dim identity). This avoids relying on
        `SerializeToString()` of nested messages, which Python protobuf does
        not guarantee deterministic across map iteration orders.
        """
        iqf_field = rsr.dimension.WhichOneof(
            'impression_qualification_filter')
        if iqf_field == 'external_impression_qualification_filter_id':
            iqf_key = (
                'external',
                rsr.dimension.external_impression_qualification_filter_id)
        elif iqf_field == 'custom':
            iqf_key = ('custom', rsr.dimension.custom)
        else:
            # Unreachable for valid persisted data: SpannerReportResults
            # Service.validate() rejects RSRs whose IQF oneof is unset.
            # Reaching here means either that validator was bypassed or
            # a new IQF variant was added to the proto without updating
            # this dim-key code -- in the latter case a silent fall
            # through to a generic 'none' key would collide a new-variant
            # RSR with an unset-IQF RSR (or each other), causing
            # mis-reconciliation. Fail loud so the proto-evolution
            # contributor is forced to update this function.
            raise ValueError(
                f"Unknown impression_qualification_filter oneof variant "
                f"{iqf_field!r} on ReportingSetResult "
                f"{rsr.external_reporting_set_result_id}; update "
                "_dimension_key_excluding_metric_frequency_spec to "
                "handle the new variant.")

        return (
            rsr.dimension.external_reporting_set_id,
            rsr.dimension.venn_diagram_region_type,
            iqf_key,
            rsr.grouping_dimension_fingerprint,
            rsr.filter_fingerprint,
        )

    def _reconcile_cross_window_identities(
        self,
        request: AddProcessedResultValuesRequest,
        reporting_set_results: list[ReportingSetResult],
    ) -> None:
        """Reconciles cumulative-reach identities across the
        whole-campaign and weekly RSRs of each underlying dimension.

        Runs three passes per dim, each enforcing an Issue #4049
        invariant that independent per-RSR rounding can otherwise
        break:

        1. Cross-window snap. whole_campaign.reach and the last
           weekly cumulative reach are constrained to be equal by
           the QP solver (report.py:_add_cumulative_whole_campaign
           _relations_to_spec) but rounded independently in
           post_process_report_summary_v2.process(); the solver
           TOLERANCE (0.1) can amplify into a 1-unit integer drift.
           Snap both sides to min(whole_campaign.reach,
           last_weekly.reach) so neither is pulled upward, which
           would risk re-breaking sum(k_plus_reach) <= impressions
           (Rule 4). Skipped (as a no-op) when the values already
           agree.

        2. Derived-field recompute. _snap_cumulative_reach routes
           every mutation through _recompute_derived_fields -- the
           same helper compute_basic_metric_set uses -- so
           percent_reach, percent_k_plus_reach, average_frequency,
           and grps stay consistent with the snapped reach. The
           two derivation paths (construction and mutation) never
           drift.

        3. Rule 1 sweep. Walks the weekly series newest-to-oldest
           and clamps any earlier window whose reach exceeds its
           successor's. Runs unconditionally (not just after a
           snap-down) so a pre-existing cumulative non-decreasing
           violation is fixed even when the cross-window snap was
           a no-op. _snap_cumulative_reach only lowers values, so
           Rule 4 stays intact and derived fields stay consistent.

        Skip semantics:
        - Empty total RSR, empty weekly RSR, either side's reach <= 0,
          or missing population_size: this dim has no cross-window
          identity to reconcile; warn (where the cause is data-model
          incomplete) and continue with the next dim.

        Raise semantics (data-model violations that imply upstream
        corruption -- see Issue #4056):
        - Two RSRs share dim key + selector kind: the post-processor
          collapsed two cadences' solver inputs into one and shipped
          values that do not correspond to either RSR's own
          measurements. PR #4057 rejects the input shape that
          produces this; reaching here means the validator was
          bypassed.
        - total RSR has > 1 reporting windows: _group_results_by_
          window misbehaved. A "total" selector covers the full
          reporting interval, so exactly one window is the data-
          model contract.

        TODO(world-federation-of-advertisers/cross-media-measurement
        #4059): Add a Rule 5 snap pass for ami >= [custom, mrc]
        across IQF-varying RSRs sharing (reporting_set, venn_region,
        grouping, event_filters, metric_frequency_spec).
        """
        # Group reporting_set_result IDs by dimension (excluding the
        # weekly/total selector) so we can match a whole_campaign RSR
        # to its corresponding weekly RSR. The dim-key collision case
        # (same dim + same selector kind, distinct full
        # metric_frequency values) raises in the loop body below --
        # see the docstring's "Raise semantics" section.
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
            key = self._dimension_key_excluding_metric_frequency_spec(rsr)
            bucket = dim_to_rsrs.setdefault(key, {})
            if selector in bucket:
                # Two RSRs that share both the dim key (excluding
                # metric_frequency_spec) AND the selector kind imply the
                # upstream data was already corrupted: the post-processor
                # groups solver input by (impression_filter,
                # edp_combination) only, so two cadences for the same slice
                # (e.g. weekly=MONDAY and weekly=TUESDAY) silently
                # overwrite each other and both RSRs receive processed
                # values derived from one cadence's solver run -- not just
                # a 1-unit drift but values that do not correspond to that
                # RSR's own measurements (Issue #4056). PR #4057 rejects
                # the only known input shape that produces this at the API
                # layer, so reaching this branch means the validator was
                # bypassed or a new input path was added without going
                # through it. Either way it is a bug and the report should
                # fail rather than ship inconsistent data.
                raise ValueError(
                    "Multiple ReportingSetResults share dimension key "
                    f"with selector={selector} (ids {bucket[selector]} "
                    f"and {rsr.external_reporting_set_result_id}); the "
                    "post-processor cannot disambiguate cadences for the "
                    "same slice. See Issue #4056.")
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
            if not total.reporting_window_results:
                # A 'total' RSR with no reporting windows means the spec
                # was set but the request-builder had nothing to write
                # (e.g. all of cumulative_results / non_cumulative_results
                # / whole_campaign_result were empty for this RSR). There
                # is no cross-window identity to reconcile in that case --
                # same category as the whole_reach <= 0 skip below.
                continue
            if len(total.reporting_window_results) > 1:
                # A 'total' selector reports over the full reporting
                # interval, so _group_results_by_window must produce
                # exactly one window key. More than one means that helper
                # itself misbehaved -- a data-model violation, not a
                # legitimate config. Fail rather than ship un-reconciled
                # data that hides the upstream bug.
                raise ValueError(
                    f"Total-selector ReportingSetResult {total_id} has "
                    f"{len(total.reporting_window_results)} reporting "
                    "windows (expected 1); _group_results_by_window must "
                    "produce one window for a 'total' selector.")
            if not weekly.reporting_window_results:
                logging.warning(
                    "Weekly-selector ReportingSetResult %d has no reporting "
                    "windows; skipping cross-window reconciliation for this "
                    "dimension.", weekly_id)
                continue
            # Sort once and reuse: last_weekly is the highest-end-date
            # entry, and the Rule 1 sweep below walks the same list
            # newest-to-oldest. Duplicate end dates would themselves
            # be a data-model violation; sorted()[-1] resolves ties
            # to the last duplicate, max() would resolve to the first
            # -- academic, since the sweep treats them as equal anyway.
            sorted_weekly = sorted(
                weekly.reporting_window_results,
                key=lambda w: (w.key.end.year, w.key.end.month, w.key.end.day),
            )
            last_weekly = sorted_weekly[-1]
            whole = total.reporting_window_results[0]
            whole_reach = whole.value.cumulative_results.reach
            last_weekly_reach = last_weekly.value.cumulative_results.reach
            # reach is a scalar int64 with proto default 0; we cannot
            # distinguish 'absent' from 'real zero'. If either side has
            # reach <= 0 (e.g. the total-selector spec did not request
            # whole-campaign cumulative reach but did request impressions /
            # GRPs / etc.), snapping to min(...) would zero out the other
            # sides legitimate reach and cascade through k_plus_reach /
            # percent_reach / average_frequency. Skip: there is no
            # cross-window reach identity to reconcile when one side has
            # no cumulative reach measurement.
            if whole_reach <= 0 or last_weekly_reach <= 0:
                logging.info(
                    'Skipping cross-window reach reconciliation for dim '
                    '(total=%d weekly=%d): one side has no positive '
                    'cumulative reach (whole=%d, last_weekly=%d).',
                    total_id, weekly_id, whole_reach, last_weekly_reach)
                continue
            # Population gates everything below: both the cross-window
            # snap and the Rule 1 sweep mutate via _snap_cumulative_reach,
            # which needs population to recompute derived fields.
            total_population = population_by_rsr_id.get(total_id, 0)
            weekly_population = population_by_rsr_id.get(weekly_id, 0)
            if total_population <= 0 or weekly_population <= 0:
                logging.warning(
                    "Missing population_size for RSR (total=%d weekly=%d); "
                    "skipping cross-window reconciliation for this dimension.",
                    total_id, weekly_id)
                continue
            # Cross-window snap. Skipping when the values already agree
            # avoids spurious mutation traffic; the Rule 1 sweep below
            # runs either way so pre-existing weekly monotonicity
            # violations (not introduced by the snap) are still caught.
            if whole_reach != last_weekly_reach:
                snapped_reach = min(whole_reach, last_weekly_reach)
                self._snap_cumulative_reach(
                    whole.value.cumulative_results, snapped_reach,
                    total_population)
                self._snap_cumulative_reach(
                    last_weekly.value.cumulative_results, snapped_reach,
                    weekly_population)
            # Rule 1 sweep: walk weekly windows newest-to-oldest and
            # clamp any earlier window whose reach exceeds its
            # successor's. Runs unconditionally (not just after a
            # snap-down) so a pre-existing cumulative non-decreasing
            # violation in the weekly series is fixed even when the
            # cross-window reaches happened to agree.
            # _snap_cumulative_reach only lowers values, so Rule 4
            # (sum(k_plus_reach) <= impressions) stays intact and
            # derived fields stay consistent.
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
            # Unconditional forward-clamp: take min() at every position
            # rather than breaking when the local pair is already
            # non-increasing. The early-break form is only safe when the
            # input is itself non-increasing -- a precondition the helper
            # currently has (every BasicMetricSet here was built by
            # compute_basic_metric_set, which enforces it) but the
            # docstring promises general-purpose forward-clamping. A
            # future caller passing an unprocessed BasicMetricSet would
            # silently leak a Rule 3 / monotonicity violation past the
            # break. Unconditional min() removes the precondition for
            # the same O(n) cost.
            for i in range(1, len(k_plus_reach)):
                k_plus_reach[i] = min(k_plus_reach[i], k_plus_reach[i - 1])
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

    if reach is not None:
        basic_metric_set.reach = reach

    if impressions is not None:
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
                # Walk high index -> low. This direction is load-bearing:
                # only lowering k[i] for i descending keeps the non-
                # increasing invariant (k[i-1] >= k[i]) intact, because
                # k[i-1] is either untouched or about to be lowered next.
                # A low->high or proportional rewrite would silently
                # break Rule 3 / monotonicity.
                i = len(k_plus_reach_values) - 1
                while excess > 0 and i >= 1:
                    absorbed = min(excess, k_plus_reach_values[i])
                    k_plus_reach_values[i] -= absorbed
                    excess -= absorbed
                    i -= 1
                if excess > 0:
                    # The histogram's weighted sum exceeds impressions and
                    # the cascade ran out of buckets to absorb the residue
                    # (cascade stops before index 0 to preserve rule 1 when
                    # reach is set). Upstream data is inconsistent; surface
                    # it as a warning rather than silently producing a
                    # BasicMetricSet that violates rule 2. Use %s for reach
                    # because reach may be None when only the histogram and
                    # impressions were supplied.
                    logging.warning(
                        "Could not fully reconcile sum(k_plus_reach) <= "
                        "impressions: leftover residue %d (reach=%s, "
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
