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


def get_report_summary_v2_from_report_result(
    report_result: ReportResult,
    edp_combinations_by_reporting_set_id: dict[str, list[str]],
) -> list[ReportSummaryV2]:
    """Converts a ReportResult to a list of ReportSummaryV2.

  Args:
    report_result: The ReportResult to convert.
    edp_combinations_by_reporting_set_id: A dict mapping reporting set id to EDPs.

  Returns:
    A list of converted ReportSummaryV2 messages, one for each set of grouping
    predicates found in the report_result.
  """
    _validate_report_result(report_result,
                            edp_combinations_by_reporting_set_id)

    if not report_result.reporting_set_results:
        logging.warning(
            "The report result does not have any reporting set results.")
        return []

    grouped_results = _group_reporting_set_results(report_result)

    # Extracts report summaries from the report result.
    report_summaries = []
    for group_key, results_for_group in grouped_results.items():
        report_summary = _create_report_summary_for_group(
            report_result,
            group_key,
            results_for_group,
            edp_combinations_by_reporting_set_id,
        )
        report_summaries.append(report_summary)

    return report_summaries


def _validate_report_result(report_result: ReportResult,
                            edp_combinations: dict[str, list[str]]) -> None:
    """Performs validation checks on the input ReportResult."""
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
        return

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
        if str(key.external_reporting_set_id) not in edp_combinations:
            raise ValueError(
                "Cannot find the data providers for reporting set "
                f"{key.external_reporting_set_id}.")
        if not key.HasField("metric_frequency_spec"):
            raise ValueError(
                "ReportingSetResultKey must have a metric_frequency_spec.")
        if not key.WhichOneof("impression_qualification_filter"):
            raise ValueError(
                "ReportingSetResultKey must have an impression_qualification_filter."
            )
        if key.venn_diagram_region_type == ReportResult.VennDiagramRegionType.VENN_DIAGRAM_REGION_TYPE_UNSPECIFIED:
            raise ValueError(
                "ReportingSetResultKey must have a venn_diagram_region_type.")

        # Validates that each reporting window result has the required fields.
        for window_entry in entry.value.reporting_window_results:
            if (window_entry.value.noisy_report_result_values.HasField(
                    "non_cumulative_results")
                    and not window_entry.key.HasField("non_cumulative_start")):
                raise ValueError(
                    "ReportingWindow with non_cumulative_results must have a "
                    "non-cumulative start date.")
            if not window_entry.key.HasField("end"):
                raise ValueError("ReportingWindow must have an end date.")
            if not window_entry.value.HasField("noisy_report_result_values"):
                raise ValueError("Missing noisy_report_result_values field.")


def _group_reporting_set_results(
    report_result: ReportResult, ) -> defaultdict[frozenset, list]:
    """Groups reporting set results by their demographic and event filter keys."""
    grouped_results = defaultdict(list)
    for reporting_set_result_entry in report_result.reporting_set_results:
        group_key = _get_group_key(reporting_set_result_entry.key)
        grouped_results[group_key].append(reporting_set_result_entry)
    return grouped_results


def _create_report_summary_for_group(
    report_result: ReportResult,
    group_key: frozenset[tuple[str, any]],
    results_for_group: list,
    edp_combinations: dict[str, list[str]],
) -> ReportSummaryV2:
    """Creates a ReportSummaryV2 for a specific group of results."""
    report_summary = ReportSummaryV2()
    report_summary.cmms_measurement_consumer_id = (
        report_result.cmms_measurement_consumer_id)
    report_summary.external_report_result_id = str(
        report_result.external_report_result_id)

    grouping_predicate_strings = [
        f"{path}={value}" for path, value in group_key if value
    ]
    report_summary.grouping_predicates.extend(
        sorted(grouping_predicate_strings) or ["-"])

    population = _get_population(results_for_group)
    if population > 0:
        report_summary.population = population

    for entry in results_for_group:
        _process_reporting_set_result(report_summary, entry, edp_combinations)

    return report_summary


def _process_reporting_set_result(
    report_summary: ReportSummaryV2,
    reporting_set_result_entry: ReportResult.ReportingSetResultEntry,
    edp_combinations: dict[str, list[str]],
) -> None:
    """Processes a single ReportingSetResultEntry and adds it to the ReportSummaryV2."""
    key = reporting_set_result_entry.key
    value = reporting_set_result_entry.value

    summary_set_result = report_summary.report_summary_set_results.add()

    # Get impression filter
    impression_filter_oneof = key.WhichOneof("impression_qualification_filter")
    if impression_filter_oneof == "external_impression_qualification_filter_id":
        summary_set_result.impression_filter = (
            key.external_impression_qualification_filter_id)
    elif impression_filter_oneof == "custom":
        summary_set_result.impression_filter = "custom"

    # Get set operation
    if key.venn_diagram_region_type == ReportResult.VennDiagramRegionType.UNION:
        summary_set_result.set_operation = "union"
    elif key.venn_diagram_region_type == ReportResult.VennDiagramRegionType.PRIMITIVE:
        summary_set_result.set_operation = "primitive"

    # Get data providers
    reporting_set_id = str(key.external_reporting_set_id)
    summary_set_result.data_providers.extend(
        sorted(edp_combinations[reporting_set_id]))

    sorted_windows = sorted(value.reporting_window_results,
                            key=lambda x: _proto_date_to_datetime(x.key.end))

    whole_campaign_non_cumulative = []
    for window_entry in sorted_windows:
        noisy_values = window_entry.value.noisy_report_result_values

        if noisy_values.HasField("cumulative_results"):
            window_result = summary_set_result.cumulative_results.add()
            _copy_window_results(
                ReportSummaryWindowResult.MetricFrequencyType.WEEKLY,
                noisy_values.cumulative_results, window_result,
                summary_set_result, True, window_entry.key.end)

        if noisy_values.HasField("non_cumulative_results"):
            freq_spec = key.metric_frequency_spec.WhichOneof("selector")
            if freq_spec == "total":
                whole_campaign_non_cumulative.append(
                    (noisy_values.non_cumulative_results,
                     window_entry.key.end))
            elif freq_spec == "weekly":
                window_result = summary_set_result.non_cumulative_results.add()
                _copy_window_results(
                    ReportSummaryWindowResult.MetricFrequencyType.WEEKLY,
                    noisy_values.non_cumulative_results, window_result,
                    summary_set_result, False, window_entry.key.end)

    for result, end_date in whole_campaign_non_cumulative:
        window_result = summary_set_result.non_cumulative_results.add()
        _copy_window_results(
            ReportSummaryWindowResult.MetricFrequencyType.TOTAL, result,
            window_result, summary_set_result, False, end_date)


def _get_population(results_for_group: list) -> int:
    """Gets the population for a group of results."""
    population = 0
    for entry in results_for_group:
        current_population = entry.value.population_size
        if current_population > 0:
            if population == 0:
                population = current_population
            elif population != current_population:
                raise ValueError(
                    "Inconsistent population sizes found within the same result group."
                )
    return population


def _get_group_key(
    reporting_set_result_key: ReportResult.ReportingSetResultKey,
) -> frozenset[tuple[str, any]]:
    """Creates a hashable key for grouping reporting set results.

  Each EventTemplateField is represented by a tuple (path, value).
  Results without any groupings or event filters will be assigned the key
  ("-", "").
  """
    all_terms = list(reporting_set_result_key.groupings)
    for event_filter in reporting_set_result_key.event_filters:
        all_terms.extend(event_filter.terms)

    if not all_terms:
        # Uses a special key for results with no demographic breakdown or filters.
        return frozenset({("-", "")})
    else:
        return frozenset(_get_hashable_term(term) for term in all_terms)


def _get_hashable_term(term: EventTemplateField) -> tuple[str, any]:
    """Creates a stable, hashable representation of an EventTemplateField."""
    value_message = term.value
    value_kind = value_message.WhichOneof("selector")
    return (term.path, getattr(value_message, value_kind))


def _proto_date_to_datetime(proto_date: ProtoDate) -> date:
    """Converts a google.type.Date to a Python datetime.date."""
    return date(proto_date.year, proto_date.month, proto_date.day)


def _copy_window_results(
    metric_frequency_type: ReportSummaryWindowResult.MetricFrequencyType,
    noisy_metric_set: NoisyMetricSet,
    report_summary_window_result: ReportSummaryWindowResult,
    report_summary_set_result: ReportSummaryV2.ReportSummarySetResult,
    is_cumulative: bool,
    window_end_date: ProtoDate,
) -> None:
    """Helper to copy metric values from a NoisyMetricSet to a ReportSummaryWindowResult."""

    report_summary_window_result.metric_frequency_type = metric_frequency_type

    # Generates the unique name for the metric ID.
    metric_name_parts = ["cumulative" if is_cumulative else "non_cumulative"]
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
            bin = report_summary_window_result.frequency.bins[str(key)]
            bin.value = int(bin_result.value)
            if bin_result.HasField("univariate_statistics"):
                bin.standard_deviation = (
                    bin_result.univariate_statistics.standard_deviation)
        report_summary_window_result.frequency.metric = f"frequency_{base_metric_name}"
