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
"""A tool for correcting a ReportSummaryV2 and producing a ReportPostProcessorResult."""

import sys
from typing import FrozenSet, TypeAlias

from absl import app
from absl import flags
from absl import logging

from noiseninja.noised_measurements import KReachMeasurements
from noiseninja.noised_measurements import Measurement
from noiseninja.noised_measurements import MeasurementSet
from report.report import EdpCombination
from report.report import MetricReport
from report.report import Report
from wfa.measurement.internal.reporting.postprocessing import (
    report_post_processor_result_pb2,
)
from wfa.measurement.internal.reporting.postprocessing import (
    report_summary_v2_pb2,
)

ReportPostProcessorResult = (
    report_post_processor_result_pb2.ReportPostProcessorResult)

ReportSummarySetResult = (
    report_summary_v2_pb2.ReportSummaryV2.ReportSummarySetResult)
ReportSummaryWindowResult = ReportSummarySetResult.ReportSummaryWindowResult

ImpressionFilter: TypeAlias = str

FLAGS = flags.FLAGS

flags.DEFINE_boolean("debug", False, "Enable debug mode.")


class ReportSummaryV2Processor:
    """Processes a ReportSummaryV2 and corrects its results.

  This class takes a ReportSummaryV2 as input and performs the following steps:
  1. Extracts weekly cumulative, weekly non-cumulative, and whole-campaign
     results.
  2. Constructs a Report object.
  """

    def __init__(
        self,
        report_summary: report_summary_v2_pb2.ReportSummaryV2,
        ami_mrc_exempted_reporting_set_ids: list[str],
    ):
        """Initializes the processor with a ReportSummary v2 proto."""
        self._report_summary = report_summary
        self._ami_mrc_exempted_reporting_set_ids = (
            ami_mrc_exempted_reporting_set_ids or []
        )
        self._weekly_cumulative_reaches: dict[ImpressionFilter,
                                              dict[EdpCombination,
                                                   list[Measurement]]] = {}
        self._weekly_non_cumulative_measurements: dict[ImpressionFilter, dict[
            EdpCombination, list[MeasurementSet]]] = {}
        self._whole_campaign_measurements: dict[ImpressionFilter,
                                                dict[EdpCombination,
                                                     MeasurementSet]] = {}
        # Map of aliased-metric-name -> canonical-metric-name.
        #
        # Two ReportSummarySetResults that share (impression_filter,
        # frozenset(data_providers)) measure the same true quantity -- e.g.
        # two composite ReportingSets whose set-expressions permute the same
        # DataProvider list have identical EventGroup membership. We feed the
        # solver a single Measurement per (filter, edp_combination), so only
        # one metric-name per bucket survives solver-input construction. The
        # response builder (post_process_report_result.py:
        # _process_window_results) then looks up each ReportSummarySetResult
        # by its OWN metric-name and would crash with KeyError on the ones
        # that were folded into a shared bucket.
        #
        # This map remembers which metric-names got folded away (aliased)
        # into which surviving metric-name (canonical). After the solver
        # produces corrected values, we propagate each canonical's value
        # into its aliases so every response-builder lookup succeeds.
        self._metric_name_aliases: dict[str, str] = {}

    def _build_report(self) -> Report:
        """Builds a Report object from the report summary data."""
        logging.info("Building a report from the report summary v2.")

        # Processes all union results from the input proto.
        self._process_union_results()

        # Gets all impression filters.
        all_impression_filters = (
            set(self._weekly_cumulative_reaches.keys()) |
            set(self._weekly_non_cumulative_measurements.keys()) |
            set(self._whole_campaign_measurements.keys()))

        # Builds metric reports.
        metric_reports = {}
        for impression_filter in all_impression_filters:
            metric_reports[impression_filter] = MetricReport(
                weekly_cumulative_reaches=self._weekly_cumulative_reaches.get(
                    impression_filter, {}),
                whole_campaign_measurements=self._whole_campaign_measurements.
                get(impression_filter, {}),
                weekly_non_cumulative_measurements=self.
                _weekly_non_cumulative_measurements.get(impression_filter, {}),
            )

        children_metrics = []
        if "mrc" in all_impression_filters:
            children_metrics.append("mrc")
        if "custom" in all_impression_filters:
            children_metrics.append("custom")


        # In ReportSummaryV2, we use the `external_reporting_set_id` as the key
        # for the metrics instead of `edp_names`. As a result, we use
        # `ami_mrc_exempted_reporting_set_ids` to indicate which
        # `external_reporting_set_ids` are exempted from AMI vs MRC consistency check.
        return Report(
            metric_reports,
            metric_subsets_by_parent={"ami": children_metrics}
            if "ami" in all_impression_filters and children_metrics
            else {},
            cumulative_inconsistency_allowed_edp_combinations={},
            ami_mrc_exempted_edps=self._ami_mrc_exempted_reporting_set_ids,
        )

    def _process_union_results(self):
        """Extracts all union results from the report summary v2."""
        logging.info("Processing union results from report summary v2.")

        for report_summary_set_result in (
                self._report_summary.report_summary_set_results):
            if report_summary_set_result.set_operation != "union":
                continue

            impression_filter = report_summary_set_result.impression_filter
            edp_combination = frozenset(
                report_summary_set_result.data_providers)

            # Initialize dictionaries for the impression_filter if not seen before.
            self._weekly_cumulative_reaches.setdefault(impression_filter, {})
            self._weekly_non_cumulative_measurements.setdefault(
                impression_filter, {})
            self._whole_campaign_measurements.setdefault(impression_filter, {})

            if report_summary_set_result.cumulative_results:
                # Process weekly cumulative reach time series.
                logging.debug(
                    f"Processing {impression_filter} cumulative results for EDPs"
                    f" {report_summary_set_result.data_providers}.")
                measurements = []
                for result in report_summary_set_result.cumulative_results:
                    if not result.HasField("reach"):
                        raise ValueError(
                            "Cumulative results must be reach results.")
                    measurements.append(
                        Measurement(
                            result.reach.value,
                            result.reach.standard_deviation,
                            result.reach.metric,
                        ))
                bucket = self._weekly_cumulative_reaches[impression_filter]
                if edp_combination in bucket:
                    self._record_measurement_list_aliases(
                        bucket[edp_combination], measurements)
                bucket[edp_combination] = measurements

            if report_summary_set_result.non_cumulative_results:
                logging.debug(
                    f"Processing {impression_filter} non-cumulative results for"
                    f" EDPs {report_summary_set_result.data_providers}.")
                weekly_results = []
                for result in report_summary_set_result.non_cumulative_results:
                    weekly_results.append(result)

                if weekly_results:
                    new_measurement_sets = [
                        self._extract_measurement_set(result)
                        for result in weekly_results
                    ]
                    bucket = self._weekly_non_cumulative_measurements[
                        impression_filter]
                    if edp_combination in bucket:
                        old_sets = bucket[edp_combination]
                        # strict=True mirrors the cumulative path's
                        # length guard: two ReportSummarySetResults sharing
                        # (impression_filter, edp_combination) must agree on
                        # cadence length. A mismatch is an upstream contract
                        # violation and must raise; silently leaving the
                        # extra aliased names unpropagated would reintroduce
                        # the KeyError this fix prevents.
                        for old_set, new_set in zip(old_sets,
                                                    new_measurement_sets,
                                                    strict=True):
                            self._record_measurement_set_aliases(
                                old_set, new_set)
                    bucket[edp_combination] = new_measurement_sets

            if report_summary_set_result.HasField('whole_campaign_result'):
                logging.debug(
                    f"Processing {impression_filter} whole campaign result for"
                    f" EDPs {report_summary_set_result.data_providers}.")
                new_set = self._extract_measurement_set(
                    report_summary_set_result.whole_campaign_result)
                bucket = self._whole_campaign_measurements[impression_filter]
                if edp_combination in bucket:
                    self._record_measurement_set_aliases(
                        bucket[edp_combination], new_set)
                bucket[edp_combination] = new_set


        logging.info("Finished processing results.")

    def _extract_measurement_set(
            self, result: ReportSummaryWindowResult) -> MeasurementSet:
        """Extracts a MeasurementSet from a ReportSummaryWindowResult."""
        reach = None
        k_reach = {}
        impression = None
        if result.HasField("reach"):
            reach = Measurement(
                result.reach.value,
                result.reach.standard_deviation,
                result.reach.metric,
            )
        if result.HasField("frequency"):
            for key, bin_result in result.frequency.bins.items():
                k_reach_id = f"{result.frequency.metric}-bin-{key}"
                k_reach[int(key)] = Measurement(
                    bin_result.value, bin_result.standard_deviation, k_reach_id
                )
        if result.HasField("impression_count"):
            impression = Measurement(
                result.impression_count.value,
                result.impression_count.standard_deviation,
                result.impression_count.metric,
            )
        return MeasurementSet(reach=reach,
                              k_reach=k_reach,
                              impression=impression)

    def _record_measurement_list_aliases(
            self, aliased: list[Measurement],
            canonical: list[Measurement]) -> None:
        """Records aliases for parallel weekly-cadence Measurement lists.

        The two lists correspond to the same weekly cadence for the same
        (impression_filter, edp_combination) via two different
        ReportSummarySetResults. Entry i of each list measures the same true
        quantity under a different metric-name; entry i of `aliased` gets
        recorded as an alias of entry i of `canonical`.
        """
        if len(aliased) != len(canonical):
            raise ValueError(
                f"Aliased measurement list has length {len(aliased)} but "
                f"canonical list has length {len(canonical)}; two "
                f"ReportSummarySetResults sharing (impression_filter, "
                f"edp_combination) must agree on cadence length.")
        for old, new in zip(aliased, canonical):
            if old.name != new.name:
                self._add_alias(old.name, new.name)

    def _record_measurement_set_aliases(self, aliased: MeasurementSet,
                                        canonical: MeasurementSet) -> None:
        """Records aliases for every Measurement inside a MeasurementSet."""
        if aliased.reach is not None and canonical.reach is not None:
            if aliased.reach.name != canonical.reach.name:
                self._add_alias(aliased.reach.name, canonical.reach.name)
        if aliased.impression is not None and canonical.impression is not None:
            if aliased.impression.name != canonical.impression.name:
                self._add_alias(aliased.impression.name,
                                canonical.impression.name)
        # k_reach: keys are frequency bins. Alias only where both sides have
        # the same bin. If `aliased` has a bin absent from `canonical`, that
        # bin's metric-name is never propagated; the response builder looks
        # up frequency bins with updated_measurements.get(name,
        # bin_result.value) (post_process_report_result.py:_process_window_results),
        # so it silently returns the *uncorrected raw* value rather than
        # KeyError. This only happens on a shape divergence between two
        # ReportSummarySetResults sharing (impression_filter, edp_combination),
        # which we treat as an upstream contract violation.
        for bin_key, new_meas in canonical.k_reach.items():
            old_meas = aliased.k_reach.get(bin_key)
            if old_meas is not None and old_meas.name != new_meas.name:
                self._add_alias(old_meas.name, new_meas.name)

    def _add_alias(self, aliased_name: str, canonical_name: str) -> None:
        """Records `aliased_name -> canonical_name` and keeps
        _metric_name_aliases flat so the propagation loop resolves every
        entry in one hop regardless of dict iteration order.

        Two forms of flattening handle chained collisions when 3 or more
        ReportSummarySetResults share the same (impression_filter,
        edp_combination):
          1. If `canonical_name` is itself an aliased key (a previously
             canonical name has since been aliased to something newer),
             follow the chain and record the ultimate target.
          2. If any existing alias pointed at `aliased_name` (that name
             was previously canonical for some entries), retarget those
             entries to the new canonical too. Without this, when the
             third collision arrives, earlier entries would still point
             at an intermediate name that is no longer in
             updated_measurements after the solver runs, and the
             propagation loop would skip them.
        """
        canonical = canonical_name
        while canonical in self._metric_name_aliases:
            canonical = self._metric_name_aliases[canonical]
        if aliased_name == canonical:
            return
        for existing_aliased, existing_canonical in list(
                self._metric_name_aliases.items()):
            if existing_canonical == aliased_name:
                self._metric_name_aliases[existing_aliased] = canonical
        self._metric_name_aliases[aliased_name] = canonical

    def process(self) -> ReportPostProcessorResult:
        """Corrects the report and returns the result."""
        report = self._build_report()
        corrected_report, report_post_processor_result = (
            report.get_corrected_report()
        )

        report_post_processor_result.pre_correction_report_summary_v2.CopyFrom(
            self._report_summary)

        # If the QP solver does not converge, return the report post processor
        # result that contains an empty map of updated measurements.
        if not corrected_report:
            return report_post_processor_result

        # If the QP solver finds a solution, update the report post processor
        # result with the updated measurements map.
        metric_name_to_value: dict[str, int] = {}
        for measurement_name in report.get_all_measurement_names():
            measurement = corrected_report.get_measurement_from_name(
                measurement_name)
            if measurement is None:
                raise ValueError(
                    f"Measurement with name {measurement_name} does not exist "
                    f"in the corrected report."
                )
            metric_name_to_value.update(
                {measurement_name: round(measurement.value)})

        report_post_processor_result.updated_measurements.update(
            metric_name_to_value)

        # Propagate solved values to metric-names that were folded away
        # during solver-input construction (see _metric_name_aliases
        # docstring on the constructor). Without this the response builder
        # in post_process_report_result.py:_process_window_results throws
        # KeyError when it iterates ReportSummarySetResults whose
        # metric-name was not the canonical one for its
        # (impression_filter, edp_combination) bucket.
        for aliased_name, canonical_name in self._metric_name_aliases.items():
            if canonical_name in report_post_processor_result.updated_measurements:
                report_post_processor_result.updated_measurements[aliased_name] = (
                    report_post_processor_result.updated_measurements[
                        canonical_name])

        logging.info("Finished correcting the report.")

        return report_post_processor_result
