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
        # Map of aliased metric-name -> canonical metric-name for
        # ReportSummarySetResults that share (impression_filter, frozenset(
        # data_providers)) with an earlier ReportSummarySetResult. Two composite
        # ReportingSets that differ only by DP-ordering in their set-expression
        # have identical EG membership and thus identical underlying VIDs, so
        # they measure the same true quantity. We feed the solver a single
        # Measurement per (filter, edp_combination) (correct) but the response-
        # builder in post_process_report_result.py:_process_window_results looks
        # up each ReportingSetResult by its own metric_name -- so any RSR whose
        # metric_name was displaced during solver-input construction has no
        # entry in updated_measurements and crashes with KeyError. After solver
        # completion we back-fill each displaced name with the winning name's
        # solved value.
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
                        for old_set, new_set in zip(old_sets,
                                                    new_measurement_sets):
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
            self, displaced: list[Measurement],
            winning: list[Measurement]) -> None:
        """Records each displaced Measurement's name as an alias of the
        corresponding winning Measurement's name.

        The two lists correspond to the same weekly cadence for the same
        (impression_filter, edp_combination) -- so entry i of `displaced` and
        entry i of `winning` measure the same true quantity through different
        ReportingSetResults' metric-name conventions.
        """
        if len(displaced) != len(winning):
            raise ValueError(
                f"Displaced measurement list has length {len(displaced)} but "
                f"winning list has length {len(winning)}; two "
                f"ReportSummarySetResults sharing (impression_filter, "
                f"edp_combination) must agree on cadence length.")
        for old, new in zip(displaced, winning):
            if old.name != new.name:
                self._metric_name_aliases[old.name] = new.name

    def _record_measurement_set_aliases(self, displaced: MeasurementSet,
                                        winning: MeasurementSet) -> None:
        """Records aliases for every Measurement inside a MeasurementSet."""
        if displaced.reach is not None and winning.reach is not None:
            if displaced.reach.name != winning.reach.name:
                self._metric_name_aliases[
                    displaced.reach.name] = winning.reach.name
        if displaced.impression is not None and winning.impression is not None:
            if displaced.impression.name != winning.impression.name:
                self._metric_name_aliases[
                    displaced.impression.name] = winning.impression.name
        # k_reach: keys are frequency bins; alias only where both sides have
        # the same bin (mismatched bin sets on ReportSummarySetResults sharing
        # (filter, edps) would already have failed upstream, but the guard
        # keeps this helper total).
        for bin_key, new_meas in winning.k_reach.items():
            old_meas = displaced.k_reach.get(bin_key)
            if old_meas is not None and old_meas.name != new_meas.name:
                self._metric_name_aliases[old_meas.name] = new_meas.name

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

        # Back-fill solved values for metric names that were displaced during
        # solver-input construction (see _metric_name_aliases docstring on the
        # constructor). Without this the response builder in
        # post_process_report_result.py:_process_window_results throws
        # KeyError when it iterates ReportingSetResults whose metric_name was
        # not the winning one for its (impression_filter, edp_combination)
        # bucket.
        for aliased_name, canonical_name in self._metric_name_aliases.items():
            if canonical_name in report_post_processor_result.updated_measurements:
                report_post_processor_result.updated_measurements[aliased_name] = (
                    report_post_processor_result.updated_measurements[
                        canonical_name])

        logging.info("Finished correcting the report.")

        return report_post_processor_result
