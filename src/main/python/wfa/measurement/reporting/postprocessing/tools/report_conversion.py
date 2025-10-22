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
  # If the report result does not have any reporting set results, return an
  # empty list.
  if len(report_result.reporting_set_results) == 0:
    logging.warning("The report result does not have any reporting set results.")
    return []

  # Group reporting set results by demographic groupings and event filters.
  # The key is a frozenset of hashable representations of all EventTemplateFields
  # from both the `groupings` and `event_filters` fields.
  grouped_results = defaultdict(list)
  for reporting_set_result in report_result.reporting_set_results:
    group_key = _get_group_key(reporting_set_result)
    grouped_results[group_key].append(reporting_set_result)

  # Determine the overall report date range to identify whole-campaign results.
  all_window_dates = [
      (
          _proto_date_to_datetime(window_result.window_start_date),
          _proto_date_to_datetime(window_result.window_end_date),
      )
      for reporting_set_result in report_result.reporting_set_results
      for window_result in reporting_set_result.reporting_window_results
  ]

  if not all_window_dates:
    raise ValueError("The report does not have any reporting windows.")

  if not report_result.HasField("report_start"):
    raise ValueError("The report does not have a report_start date.")

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
    grouping_predicate_strings = [f"{path}={value}" for path, value in group_key]
    report_summary.grouping_predicates.extend(
        sorted(grouping_predicate_strings) or ["-"]
    )

    for reporting_set_result in results_for_group:
      summary_set_result = report_summary.report_summary_set_results.add()

      # Map Impression Filter from the oneof using a match/case statement.
      match reporting_set_result.WhichOneof("impression_qualification_filter"):
        case "external_impression_qualification_filter_id":
          summary_set_result.impression_filter = (
              reporting_set_result.external_impression_qualification_filter_id
          )
        case "custom" if reporting_set_result.custom:
          summary_set_result.impression_filter = "custom"

      # Map Set Operation
      if (
          reporting_set_result.venn_diagram_region_type
          == ReportResult.VennDiagramRegionType.UNION
      ):
        summary_set_result.set_operation = "union"
      elif (
          reporting_set_result.venn_diagram_region_type
          == ReportResult.VennDiagramRegionType.PRIMITIVE
      ):
        summary_set_result.set_operation = "primitive"

      # Map Data Providers using the external_reporting_set_id.
      reporting_set_id = str(reporting_set_result.external_reporting_set_id)
      if reporting_set_id in edp_combinations_by_reporting_set_id:
        data_providers = edp_combinations_by_reporting_set_id[reporting_set_id]
        summary_set_result.data_providers.extend(sorted(data_providers))
      else:
        raise ValueError(f"Cannot find the data providers for reporting set {reporting_set_id}.")

      # Map Population
      if reporting_set_result.population_size > 0:
        report_summary.population = reporting_set_result.population_size

      # Map Reporting Window Results
      sorted_window_results = sorted(
          reporting_set_result.reporting_window_results, key=lambda wr: _proto_date_to_datetime(wr.window_end_date)
      )

      whole_campaign_non_cumulative_results = []
      for window_result in sorted_window_results:
        # We only process noisy results for post-processing.
        if not window_result.HasField("noisy_report_result_values"):
          continue

        noisy_values = window_result.noisy_report_result_values

        # Map Cumulative Results
        if noisy_values.HasField("cumulative_results"):
          summary_window = summary_set_result.cumulative_results.add()
          _copy_window_results(
              noisy_values.cumulative_results,
              summary_window,
              reporting_set_result.metric_frequency_type,
              summary_set_result,
              is_cumulative=True,
              window_end_date=window_result.window_end_date,
          )

        # Map Non-Cumulative Results
        if noisy_values.HasField("non_cumulative_results"):
          is_whole_campaign = (
              report_start_date is not None
              and _proto_date_to_datetime(window_result.window_start_date)
              == report_start_date
              and _proto_date_to_datetime(window_result.window_end_date)
              == report_end_date
          )

          if is_whole_campaign:
            # Defer adding whole-campaign results to the end.
            whole_campaign_non_cumulative_results.append((
                noisy_values.non_cumulative_results,
                window_result.window_end_date,
            ))
          else:
            # Add weekly non-cumulative results immediately.
            summary_window = summary_set_result.non_cumulative_results.add()
            _copy_window_results(
                noisy_values.non_cumulative_results,
                summary_window,
                reporting_set_result.metric_frequency_type,
                summary_set_result,
                is_cumulative=False,
                window_end_date=window_result.window_end_date,
            )

      # Add the deferred whole-campaign results last.
      for result, end_date in whole_campaign_non_cumulative_results:
        summary_window = summary_set_result.non_cumulative_results.add()
        _copy_window_results(
            result,
            summary_window,
            reporting_set_result.metric_frequency_type,
            summary_set_result,
            is_cumulative=False,
            window_end_date=end_date,
        )
    report_summaries.append(report_summary)

  return report_summaries

def _get_group_key(
    reporting_set_result: ReportResult.ReportingSetResult,
) -> frozenset[tuple[str, any]]:
  """Creates a hashable key for grouping reporting set results.

  Each EventTemplateField is represented by a tuple (path, value).
  Results without any groupings or event filters will be assigned the key
  ("-", "").
  """
  all_terms = list(reporting_set_result.groupings)
  for event_filter in reporting_set_result.event_filters:
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
    metric_frequency_type: ReportResult.MetricFrequencyType,
    summary_set_result: ReportSummaryV2.ReportSummarySetResult,
    is_cumulative: bool,
    window_end_date: ProtoDate | None,
) -> None:
  """Helper to copy metric values from a NoisyMetricSet to a ReportSummaryWindowResult."""
  summary_window.metric_frequency_type = metric_frequency_type

  # Build the base name for the metric ID.
  metric_name_parts = [
      ReportResult.MetricFrequencyType.Name(metric_frequency_type).lower(),
      "cumulative" if is_cumulative else "non_cumulative",
  ]
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
    for key, bin_result in noisy_metric_set.frequency_histogram.bins.items():
      summary_bin = summary_window.frequency.bins[key]
      summary_bin.value = int(bin_result.value)
      if bin_result.HasField("univariate_statistics"):
        summary_bin.standard_deviation = (
            bin_result.univariate_statistics.standard_deviation
        )
    summary_window.frequency.metric = f"frequency_{base_metric_name}"