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
    report_summary_v2_pb2,)
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
  """Converts a ReportResult to a list of ReportSummaryV2, grouped by demographics.

  Args:
    report_result: The ReportResult to convert.
    edp_combinations_by_reporting_set_id: A dict mapping reporting set id to EDPs.

  Returns:
    A list of converted ReportSummaryV2 messages, one for each demographic
    group found in the report_result.
  """
  if not report_result.cmms_measurement_consumer_id:
    raise ValueError("The report result must have a cmms_measurement_consumer_id.")
  if not report_result.external_report_result_id:
    raise ValueError("The report result must have an external_report_result_id.")
  if not report_result.HasField("report_start"):
    raise ValueError("The report result must have a report_start date.")

  # If the report result does not have any reporting set results, return an
  # empty list.
  if len(report_result.reporting_set_results) == 0:
    logging.warning("The report result does not have any reporting set results.")
    return []

  # Group reporting set results by demographic groupings and event filters.
  # The key is a frozenset of hashable representations of all EventTemplateFields
  # from both the `groupings` and `event_filters` fields.
  grouped_results = defaultdict(list)
  for reporting_set_result_entry in report_result.reporting_set_results:
    if not reporting_set_result_entry.HasField("key"):
      raise ValueError("ReportingSetResultEntry must have a key.")
    if not reporting_set_result_entry.HasField("value"):
      raise ValueError("ReportingSetResultEntry must have a value.")
    group_key = _get_group_key(reporting_set_result_entry.key)
    grouped_results[group_key].append(reporting_set_result_entry)

  # Validate that all reporting windows have start and end dates before processing.
  for reporting_set_result_entry in report_result.reporting_set_results:
    for window_entry in reporting_set_result_entry.value.reporting_window_results:
      if not window_entry.key.HasField("start"):
        raise ValueError("ReportingWindow must have a start date.")
      if not window_entry.key.HasField("end"):
        raise ValueError("ReportingWindow must have an end date.")
  # Determine the overall report date range to identify whole-campaign results.
  all_window_dates = [
      (
          _proto_date_to_datetime(window_entry.key.start),
          _proto_date_to_datetime(window_entry.key.end),
      )
      for reporting_set_result_entry in report_result.reporting_set_results
      for window_entry in reporting_set_result_entry.value.reporting_window_results
  ]

  if not all_window_dates:
    raise ValueError("The report does not have any reporting windows.")

  report_start_date = date(
      report_result.report_start.year,
      report_result.report_start.month,
      report_result.report_start.day,
  )
  report_end_date = max(end for start, end in all_window_dates)

  report_summaries = []
  for group_key, results_for_group in sorted(grouped_results.items()):
    report_summary = ReportSummaryV2()
    report_summary.cmms_measurement_consumer_id = (
        report_result.cmms_measurement_consumer_id
    )
    report_summary.external_report_result_id = str(
        report_result.external_report_result_id
    )
    # Create a descriptive string for each term in the group key (e.g., "path=value")
    grouping_predicate_strings = [
        f"{path}={value}" for path, value in group_key if value
    ]
    report_summary.grouping_predicates.extend(
        sorted(grouping_predicate_strings) or ["-"]
    )

    for reporting_set_result_entry in results_for_group:
      reporting_set_result_key = reporting_set_result_entry.key
      reporting_set_result_value = reporting_set_result_entry.value

      if not reporting_set_result_key.external_reporting_set_id:
        raise ValueError("ReportingSetResultKey must have an external_reporting_set_id.")
      if (reporting_set_result_key.venn_diagram_region_type
          == ReportResult.VennDiagramRegionType.VENN_DIAGRAM_REGION_TYPE_UNSPECIFIED):
        raise ValueError("ReportingSetResultKey must have a venn_diagram_region_type.")
      if not reporting_set_result_key.WhichOneof("impression_qualification_filter"):
        raise ValueError(
            "ReportingSetResultKey must have an impression_qualification_filter."
        )
      if not reporting_set_result_key.HasField("metric_frequency_spec"):
        raise ValueError("ReportingSetResultKey must have a metric_frequency_spec.")

      summary_set_result = report_summary.report_summary_set_results.add()

      # Map Impression Filter from the oneof using a match/case statement.
      match reporting_set_result_key.WhichOneof("impression_qualification_filter"):
        case "external_impression_qualification_filter_id":
          summary_set_result.impression_filter = (
              reporting_set_result_key.external_impression_qualification_filter_id
          )
        case "custom" if reporting_set_result_key.custom:
          summary_set_result.impression_filter = "custom"

      # Map Set Operation
      if (
          reporting_set_result_key.venn_diagram_region_type
          == ReportResult.VennDiagramRegionType.UNION
      ):
        summary_set_result.set_operation = "union"
      elif (
          reporting_set_result_key.venn_diagram_region_type
          == ReportResult.VennDiagramRegionType.PRIMITIVE
      ):
        summary_set_result.set_operation = "primitive"

      # Map Data Providers using the external_reporting_set_id.
      reporting_set_id = str(reporting_set_result_key.external_reporting_set_id)
      if reporting_set_id in edp_combinations_by_reporting_set_id:
        data_providers = edp_combinations_by_reporting_set_id[reporting_set_id]
        summary_set_result.data_providers.extend(sorted(data_providers))
      else:
        raise ValueError(f"Cannot find the data providers for reporting set {reporting_set_id}.")

      # Map Population
      if reporting_set_result_value.population_size > 0:
        report_summary.population = reporting_set_result_value.population_size

      # Map Reporting Window Results
      sorted_window_results = sorted(
          reporting_set_result_value.reporting_window_results,
          key=lambda wre: _proto_date_to_datetime(wre.key.end),
      )

      whole_campaign_non_cumulative_results = []
      for window_entry in sorted_window_results:
        window_key = window_entry.key
        window_value = window_entry.value
        # We only process noisy results for post-processing.
        if not window_value.HasField("noisy_report_result_values"):
          continue

        noisy_values = window_value.noisy_report_result_values

        # Map Cumulative Results
        if noisy_values.HasField("cumulative_results"):
          summary_window = summary_set_result.cumulative_results.add()
          _copy_window_results(
              noisy_values.cumulative_results,
              summary_window,
              summary_set_result,
              is_cumulative=True,
              window_end_date=window_key.end,
          )

        # Map Non-Cumulative Results
        if noisy_values.HasField("non_cumulative_results"):
          is_whole_campaign = (
              report_start_date is not None
              and _proto_date_to_datetime(window_key.start)
              == report_start_date
              and _proto_date_to_datetime(window_key.end)
              == report_end_date
          )

          if is_whole_campaign:
            # Defer adding whole-campaign results to the end.
            whole_campaign_non_cumulative_results.append((
                noisy_values.non_cumulative_results,
                window_key.end,
            ))
          else:
            # Add weekly non-cumulative results immediately.
            summary_window = summary_set_result.non_cumulative_results.add()
            _copy_window_results(
                noisy_values.non_cumulative_results,
                summary_window,
                summary_set_result,
                is_cumulative=False,
                window_end_date=window_key.end,
            )

      # Add the deferred whole-campaign results last.
      for result, end_date in whole_campaign_non_cumulative_results:
        summary_window = summary_set_result.non_cumulative_results.add()
        _copy_window_results(
            result,
            summary_window,
            summary_set_result,
            is_cumulative=False,
            window_end_date=end_date,
        )
    report_summaries.append(report_summary)

  return report_summaries

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
    # Use a special key for results with no demographic breakdown or filters.
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
    noisy_metric_set: NoisyMetricSet,
    summary_window: ReportSummaryWindowResult,
    summary_set_result: ReportSummaryV2.ReportSummarySetResult,
    is_cumulative: bool,
    window_end_date: ProtoDate | None,
) -> None:
  """Helper to copy metric values from a NoisyMetricSet to a ReportSummaryWindowResult."""

  # Build the base name for the metric ID.
  metric_name_parts = ["cumulative" if is_cumulative else "non_cumulative"]
  metric_name_parts.extend(summary_set_result.data_providers)
  metric_name_parts.append(summary_set_result.impression_filter)

  if window_end_date is not None:
    metric_name_parts.append(
        f"{window_end_date.year}_{window_end_date.month:02d}_{window_end_date.day:02d}"
    )
  base_metric_name = "_".join(metric_name_parts)

  # Copies Reach.
  if noisy_metric_set.HasField("reach"):
    summary_window.reach.value = noisy_metric_set.reach.value
    summary_window.reach.standard_deviation = (
        noisy_metric_set.reach.univariate_statistics.standard_deviation
    )
    summary_window.reach.metric = f"reach_{base_metric_name}"

  # Copies Impression Count.
  if noisy_metric_set.HasField("impression_count"):
    summary_window.impression_count.value = noisy_metric_set.impression_count.value
    summary_window.impression_count.standard_deviation = (
        noisy_metric_set.impression_count.univariate_statistics.standard_deviation
    )
    summary_window.impression_count.metric = f"impression_{base_metric_name}"

  # Copies Frequency Histogram
  if noisy_metric_set.HasField("frequency_histogram"):
    # The bins are a map from frequency (e.g., 1, 2, 3) to the bin result.
    for key, bin_result in noisy_metric_set.frequency_histogram.bin_results.items():
      summary_bin = summary_window.frequency.bins[str(key)]
      summary_bin.value = int(bin_result.value)
      if bin_result.HasField("univariate_statistics"):
        summary_bin.standard_deviation = (
            bin_result.univariate_statistics.standard_deviation
        )
    summary_window.frequency.metric = f"frequency_{base_metric_name}"