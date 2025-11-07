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

from absl import logging
from collections import defaultdict
from datetime import date
from typing import Any

from google.type import date_pb2

from src.main.proto.wfa.measurement.internal.reporting.postprocessing import (
    report_summary_v2_pb2, )
from wfa.measurement.internal.reporting.v2 import report_result_pb2
from wfa.measurement.internal.reporting.v2 import metric_frequency_spec_pb2
from wfa.measurement.internal.reporting.v2 import event_template_field_pb2

ReportResult = report_result_pb2.ReportResult
ReportSummaryV2 = report_summary_v2_pb2.ReportSummaryV2
ProtoDate = date_pb2.Date
EventTemplateField = event_template_field_pb2.EventTemplateField
NoisyMetricSet = ReportResult.ReportingSetResult.ReportingWindowResult.NoisyReportResultValues.NoisyMetricSet
ReportSummaryWindowResult = ReportSummaryV2.ReportSummarySetResult.ReportSummaryWindowResult


def report_result_to_report_summary_v2(
    report_result: ReportResult,
    primitive_reporting_sets_by_reporting_set_id: dict[str, list[str]],
) -> list[ReportSummaryV2]:
    """Converts a ReportResult to a list of ReportSummaryV2.

      The conversion steps are as follows:
      1.  Validating the input `ReportResult` to ensure it has all the required
          fields.
      2.  Grouping the individual `reporting_set_results` by their demographic
          and event filters.
      3.  Creating a single `ReportSummaryV2` proto for each of the above groups.

      Args:
        report_result: The ReportResult to be converted.
        primitive_reporting_sets_by_reporting_set_id: A reporting set id to EDP
          combination map.

      Returns:
        A list of ReportSummaryV2 messages, one for each set of grouping
        predicates found in the report_result.
    """
    _validate_report_result(report_result,
                            primitive_reporting_sets_by_reporting_set_id)

    grouped_results = _group_reporting_set_results(report_result)

    # Extracts report summaries from the report result.
    report_summaries = []
    for group_key, results_for_group in grouped_results.items():
        report_summary = _create_report_summary_for_group(
            report_result.cmms_measurement_consumer_id,
            report_result.external_report_result_id,
            group_key,
            results_for_group,
            primitive_reporting_sets_by_reporting_set_id,
        )
        report_summaries.append(report_summary)

    return report_summaries


def _validate_report_result(
    report_result: ReportResult,
    primitive_reporting_sets_by_reporting_set_id: dict[str,
                                                       list[str]]) -> None:
    """Performs validation checks on the input ReportResult.

    The function ensures that the `ReportResult` proto is well-formed and
    contains all the required fields. It also checks that the map
    primitive_reporting_sets_by_reporting_set_id has the required information for the
    conversion.

    Args:
      report_result: The `ReportResult` proto to be validated.
      primitive_reporting_sets_by_reporting_set_id: A reporting set IDs to EDP
        combination map.

    Raises:
      ValueError: If any validation check fails.
    """
    # Validates that the report result has the required top-level fields.
    if not report_result.cmms_measurement_consumer_id:
        raise ValueError(
            "The report result must have a cmms_measurement_consumer_id.")
    if not report_result.external_report_result_id:
        raise ValueError(
            "The report result must have an external_report_result_id.")
    if not report_result.HasField("report_start"):
        raise ValueError("The report result must have a report_start date.")

    if not report_result.reporting_set_results:
        raise ValueError(
            "The report result must have at least one reporting set result.")

    for entry in report_result.reporting_set_results:
        # Each reporting set result must have a key and a value.
        if not entry.HasField("key"):
            raise ValueError("ReportingSetResultEntry must have a key.")
        if not entry.HasField("value"):
            raise ValueError("ReportingSetResultEntry must have a value.")

        key = entry.key

        # Validates that the reporting set result key has the required fields.
        if not key.external_reporting_set_id:
            raise ValueError(
                "ReportingSetResultKey must have an external_reporting_set_id."
            )
        if key.venn_diagram_region_type == ReportResult.VennDiagramRegionType.VENN_DIAGRAM_REGION_TYPE_UNSPECIFIED:
            raise ValueError(
                "ReportingSetResultKey must have a venn_diagram_region_type.")
        if not key.WhichOneof("impression_qualification_filter"):
            raise ValueError(
                "ReportingSetResultKey must have an impression_qualification_filter."
            )
        if not key.HasField("metric_frequency_spec"):
            raise ValueError(
                "ReportingSetResultKey must have a metric_frequency_spec.")

        # Validates that the external_reporting_set_id is mapped to a set of
        # primitive reporting sets.
        if str(key.external_reporting_set_id
               ) not in primitive_reporting_sets_by_reporting_set_id:
            raise ValueError(
                "Cannot find the data providers for reporting set "
                f"{key.external_reporting_set_id}.")

        # Validates the population.
        if entry.value.population_size <= 0:
            raise ValueError("Population size must have a positive value.")

        # Validates that each reporting window result has the required fields.
        for window_entry in entry.value.reporting_window_results:
            if not window_entry.value.HasField("noisy_report_result_values"):
                raise ValueError("Missing noisy_report_result_values field.")
            if (window_entry.value.noisy_report_result_values.HasField(
                    "non_cumulative_results")
                    and not window_entry.key.HasField("non_cumulative_start")):
                raise ValueError(
                    "ReportingWindow with non_cumulative_results must have a "
                    "non-cumulative start date.")
            if not window_entry.key.HasField("end"):
                raise ValueError("ReportingWindow must have an end date.")


def _group_reporting_set_results(
    report_result: ReportResult
) -> defaultdict[tuple[int, int], list[ReportResult.ReportingSetResultEntry]]:
    """Groups reporting set results by their grouping predicates (demographic
    groupings and event filters).

    The function iterates through all `reporting_set_results` in a
    `ReportResult` and groups them based on their dimension filters (i.e.,
    demographic groupings and event filters).

    Args:
        report_result: The input `ReportResult`.

    Returns:
        A dictionary where keys are the filter group keys and values are lists
        of `ReportingSetResultEntry` that belong to the group.
    """
    grouped_results = defaultdict(list)
    for reporting_set_result_entry in report_result.reporting_set_results:
        value = reporting_set_result_entry.value
        group_key = (value.grouping_dimension_fingerprint,
                     value.filter_fingerprint)
        grouped_results[group_key].append(reporting_set_result_entry)
    return grouped_results


def _create_report_summary_for_group(
    cmms_measurement_consumer_id: str,
    external_report_result_id: int,
    group_key: tuple[int, int],
    results_for_group: list[ReportResult.ReportingSetResultEntry],
    primitive_reporting_sets_by_reporting_set_id: dict[str, list[str]],
) -> ReportSummaryV2:
    """Creates a `ReportSummaryV2` for a set of grouping predicates.

    This function processes all reporting set results that belong to the same
    group and generate a ReportSummaryV2.

    Args:
        cmms_measurement_consumer_id: The cmms measurement consumer id.
        external_report_result_id: The external report result id.
        group_key: The hashable key representing a group defined by demographic
          groupings and event filters.
        results_for_group: A list of all `ReportingSetResultEntry` protos that
          belong to this group.
        primitive_reporting_sets_by_reporting_set_id: A dictionary mapping reporting set
          IDs to their data provider lists.

    Returns:
        A fully populated `ReportSummaryV2` for the given group.
    """
    report_summary = ReportSummaryV2()
    report_summary.cmms_measurement_consumer_id = cmms_measurement_consumer_id
    report_summary.external_report_result_id = str(external_report_result_id)

    # Verifies that there is at least one result for this group.
    if len(results_for_group) == 0:
        raise ValueError(f"No results found for group: {group_key}.")

    # Since all results in results_for_group share the same grouping key, we get
    # the groupings and event filters from the first result.
    report_summary.groupings.extend(results_for_group[0].key.groupings)
    report_summary.event_filters.extend(results_for_group[0].key.event_filters)

    report_summary.population = _get_population(results_for_group)

    for entry in results_for_group:
        _process_reporting_set_result(
            report_summary, entry,
            primitive_reporting_sets_by_reporting_set_id)

    return report_summary


def _process_reporting_set_result(
    report_summary: ReportSummaryV2,
    reporting_set_result_entry: ReportResult.ReportingSetResultEntry,
    primitive_reporting_sets_by_reporting_set_id: dict[str, list[str]],
) -> None:
    """Extracts data from a `ReportingSetResultEntry` and adds it to a `ReportSummaryV2`.

    This function takes a single result entry from the input `ReportResult` and
    mapping its data to a new `ReportSummarySetResult` of the target
    `ReportSummaryV2`. It handles the translation of impression
    filters, set operations, data providers, and sorts the time-series data
    before copying the metric values.

    Args:
        report_summary: The `ReportSummaryV2` message to be populated.
        reporting_set_result_entry: The source `ReportingSetResultEntry` to
          process.
        primitive_reporting_sets_by_reporting_set_id: A dictionary mapping reporting set
          IDs to their data provider lists.
    """
    report_summary_set_result = _add_report_summary_set_result_metadata(
        reporting_set_result_entry,
        primitive_reporting_sets_by_reporting_set_id,
        report_summary,
    )
    _process_reporting_windows(reporting_set_result_entry,
                               report_summary_set_result)


def _add_report_summary_set_result_metadata(
    reporting_set_result_entry: ReportResult.ReportingSetResultEntry,
    primitive_reporting_sets_by_reporting_set_id: dict[str, list[str]],
    report_summary: ReportSummaryV2,
) -> ReportSummaryV2.ReportSummarySetResult:
    """Adds and populates metadata for a new ReportSummarySetResult.

    This function creates a new `ReportSummarySetResult` within the given
    `ReportSummaryV2` and populates its metadata fields, such as impression
    filter, set operation, and data providers, based on the input
    `ReportingSetResultEntry`.

    Args:
        reporting_set_result_entry: The source entry containing the metadata.
        primitive_reporting_sets_by_reporting_set_id: A mapping from reporting set ID to
          data provider lists.
        report_summary: The `ReportSummaryV2` to which the new set result will
          be added.

    Returns:
        The newly created and populated `ReportSummarySetResult`.
    """
    key = reporting_set_result_entry.key
    report_summary_set_result = report_summary.report_summary_set_results.add()

    # Get impression filter
    impression_filter_oneof = key.WhichOneof("impression_qualification_filter")
    if impression_filter_oneof == "external_impression_qualification_filter_id":
        report_summary_set_result.impression_filter = (
            key.external_impression_qualification_filter_id)
    elif impression_filter_oneof == "custom":
        report_summary_set_result.impression_filter = "custom"
    else:
        raise ValueError(
            f"Unknown impression filter type: {impression_filter_oneof}.")

    # Get set operation
    if key.venn_diagram_region_type == ReportResult.VennDiagramRegionType.UNION:
        report_summary_set_result.set_operation = "union"
    elif key.venn_diagram_region_type == ReportResult.VennDiagramRegionType.PRIMITIVE:
        report_summary_set_result.set_operation = "primitive"
    else:
        raise ValueError(
            f"Unknown venn diagram region type: {key.venn_diagram_region_type}."
        )

    # Get data providers
    reporting_set_id = str(key.external_reporting_set_id)
    report_summary_set_result.data_providers.extend(
        sorted(primitive_reporting_sets_by_reporting_set_id[reporting_set_id]))

    return report_summary_set_result


def _process_reporting_windows(
    reporting_set_result_entry: ReportResult.ReportingSetResultEntry,
    report_summary_set_result: ReportSummaryV2.ReportSummarySetResult,
) -> None:
    """Processes and copies reporting window data to report summary set result.

    This function sorts the reporting windows by date and iterates through them,
    copying cumulative, weekly non-cumulative, and total campaign
    non-cumulative results to the target `ReportSummarySetResult`.

    Args:
        reporting_set_result_entry: The source entry containing the window
          results.
        report_summary_set_result: The destination `ReportSummarySetResult` to
          populate.
    """
    key = reporting_set_result_entry.key
    value = reporting_set_result_entry.value
    sorted_windows = sorted(value.reporting_window_results,
                            key=lambda x: _proto_date_to_datetime(x.key.end))

    whole_campaign_non_cumulative = []
    for window_entry in sorted_windows:
        noisy_values = window_entry.value.noisy_report_result_values

        # Processes cumulative result.
        if noisy_values.HasField("cumulative_results"):
            metric_frequency_spec_type = key.metric_frequency_spec.WhichOneof(
                "selector")
            if metric_frequency_spec_type == "total":
                window_result = report_summary_set_result.whole_campaign_result
                _copy_window_results(
                    ReportSummaryWindowResult.MetricFrequencyType.TOTAL,
                    noisy_values.cumulative_results, report_summary_set_result,
                    False, window_entry.key.end, window_result)
            elif metric_frequency_spec_type == "weekly":
                window_result = report_summary_set_result.cumulative_results.add(
                )
                _copy_window_results(
                    ReportSummaryWindowResult.MetricFrequencyType.WEEKLY,
                    noisy_values.cumulative_results, report_summary_set_result,
                    True, window_entry.key.end, window_result)
            else:
                raise ValueError(f"Unknown metric frequency type: "
                                 f"{metric_frequency_spec_type}")

        # Processes non-cumulative result. If the result is weekly non
        # cumulative metrics, append it the non_cumulative_results. If it is a
        # total campaign result, store the pair (result, end date), and append
        # it to the non_cumulative_results once all the weekly non cumulative
        # results have been processed.
        if noisy_values.HasField("non_cumulative_results"):
            window_result = report_summary_set_result.non_cumulative_results.add(
            )
            _copy_window_results(
                ReportSummaryWindowResult.MetricFrequencyType.WEEKLY,
                noisy_values.non_cumulative_results, report_summary_set_result,
                False, window_entry.key.end, window_result)

    for result, end_date in whole_campaign_non_cumulative:
        window_result = report_summary_set_result.non_cumulative_results.add()
        _copy_window_results(
            ReportSummaryWindowResult.MetricFrequencyType.TOTAL, result,
            report_summary_set_result, False, end_date, window_result)


def _copy_window_results(
    metric_frequency_type: ReportSummaryWindowResult.MetricFrequencyType,
    noisy_metric_set: NoisyMetricSet,
    report_summary_set_result: ReportSummaryV2.ReportSummarySetResult,
    is_cumulative: bool,
    window_end_date: ProtoDate,
    report_summary_window_result: ReportSummaryWindowResult,
) -> None:
    """Copies metric values from a NoisyMetricSet to a ReportSummaryWindowResult.

    This helper function copies the reach, impression count, and frequency
    histogram from the input `NoisyMetricSet` to the target
    `ReportSummaryWindowResult`. It also constructs and assigns a unique
    metric name for each copied metric.

    Args:
      metric_frequency_type: The frequency of the metric (e.g., TOTAL, WEEKLY).
      noisy_metric_set: The noisy metric values.
      report_summary_window_result: The destination proto to be populated.
      report_summary_set_result: The parent set result which is used to get the
        data providers and impression filter.
      is_cumulative: A boolean indicating if the metric is cumulative.
      window_end_date: The end date of the reporting window, used for the metric
        name.
      """

    report_summary_window_result.metric_frequency_type = metric_frequency_type

    # Generates the unique name for the metric ID.
    metric_name_parts = []
    if metric_frequency_type == ReportSummaryWindowResult.MetricFrequencyType.TOTAL:
        metric_name_parts.append("whole_campaign")
    elif metric_frequency_type == ReportSummaryWindowResult.MetricFrequencyType.WEEKLY:
        if is_cumulative:
            metric_name_parts.append("cumulative")
        else:
            metric_name_parts.append("non_cumulative")
    else:
        raise ValueError(
            f"Unknown metric frequency type: {metric_frequency_type}")

    metric_name_parts.extend(report_summary_set_result.data_providers)
    metric_name_parts.append(report_summary_set_result.impression_filter)
    metric_name_parts.append(
        f"{window_end_date.year}_{window_end_date.month:02d}_{window_end_date.day:02d}"
    )
    base_metric_name = "_".join(metric_name_parts)

    # Copies reach.
    if noisy_metric_set.HasField("reach"):
        report_summary_window_result.reach.value = noisy_metric_set.reach.value
        report_summary_window_result.reach.standard_deviation = (
            noisy_metric_set.reach.univariate_statistics.standard_deviation)
        report_summary_window_result.reach.metric = f"reach_{base_metric_name}"

    # Copies impression count.
    if noisy_metric_set.HasField("impression_count"):
        report_summary_window_result.impression_count.value = (
            noisy_metric_set.impression_count.value)
        report_summary_window_result.impression_count.standard_deviation = (
            noisy_metric_set.impression_count.univariate_statistics.
            standard_deviation)
        report_summary_window_result.impression_count.metric = f"impression_{base_metric_name}"

    # Copies frequency histogram.
    if noisy_metric_set.HasField("frequency_histogram"):
        for key, bin_result in noisy_metric_set.frequency_histogram.bin_results.items(
        ):
            bin = report_summary_window_result.frequency.bins[key]
            bin.value = int(bin_result.value)
            if bin_result.HasField("univariate_statistics"):
                bin.standard_deviation = (
                    bin_result.univariate_statistics.standard_deviation)
        report_summary_window_result.frequency.metric = f"frequency_{base_metric_name}"


def _get_population(
    results_for_group: list[ReportResult.ReportingSetResultEntry]) -> int:
    """Gets the population for a group of results.

    This function iterates through a list of `ReportingSetResultEntry` that
    belong to the same group, extracts the population from the first result and
    verifies that all other results in the group have the same population.

    Args:
      results_for_group: A list of `ReportingSetResultEntry` for a single result
        group.

    Returns:
      The population of the group.

    Raises:
      ValueError: If different populations are found within the same group.
    """
    population = None
    for entry in results_for_group:
        current_population = entry.value.population_size
        if population == None:
            population = current_population
        elif population != current_population:
            raise ValueError(
                "Inconsistent population sizes found within the same result group."
            )
    if population != None:
        return population
    else:
        raise ValueError("Population not found.")


def _proto_date_to_datetime(proto_date: ProtoDate) -> date:
    """Converts a google.type.Date to a Python datetime.date."""
    return date(proto_date.year, proto_date.month, proto_date.day)
