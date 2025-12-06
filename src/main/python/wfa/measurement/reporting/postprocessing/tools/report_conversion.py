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

from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import date

from google.type import date_pb2

from wfa.measurement.internal.reporting.postprocessing import report_summary_v2_pb2
from wfa.measurement.internal.reporting.v2 import report_result_pb2
from wfa.measurement.internal.reporting.v2 import metric_frequency_spec_pb2
from wfa.measurement.internal.reporting.v2 import event_template_field_pb2

ReportingSetResult = report_result_pb2.ReportingSetResult
ReportSummaryV2 = report_summary_v2_pb2.ReportSummaryV2
ProtoDate = date_pb2.Date
EventTemplateField = event_template_field_pb2.EventTemplateField
NoisyMetricSet = (
    ReportingSetResult.ReportingWindowResult.NoisyReportResultValues.NoisyMetricSet
)
ReportSummaryWindowResult = ReportSummaryV2.ReportSummarySetResult.ReportSummaryWindowResult


def report_summaries_from_reporting_set_results(
    reporting_set_results: Sequence[ReportingSetResult],
    primitive_reporting_sets_by_reporting_set_id: dict[str, list[str]],
) -> list[ReportSummaryV2]:
    """Converts a sequence of ReportingSetResult to a list of ReportSummaryV2.

    The conversion steps are as follows:
    1.  Validate the input `ReportingSetResult`s to ensure they have all the required
        fields.
    2.  Group the individual `ReportingSetResult`s by their demographic
        and event filters.
    3.  Creating a single `ReportSummaryV2` proto for each of the above groups.

    Args:
      reporting_set_results: All ReportingSetResults from a single ReportResult.
      primitive_reporting_sets_by_reporting_set_id: A reporting set id to EDP
        combination map.

    Returns:
      A list of ReportSummaryV2 messages, one for each set of grouping
      predicates found in the report_result.
    """
    _validate_report_result(
        reporting_set_results, primitive_reporting_sets_by_reporting_set_id
    )

    grouped_results = _group_reporting_set_results(reporting_set_results)

    # Extracts report summaries from the report result.
    report_summaries = []
    for group_key, results_for_group in grouped_results.items():
        report_summary = _create_report_summary_for_group(
            results_for_group[0].cmms_measurement_consumer_id,
            results_for_group[0].external_report_result_id,
            group_key,
            results_for_group,
            primitive_reporting_sets_by_reporting_set_id,
        )
        report_summaries.append(report_summary)

    return report_summaries


def _validate_report_result(
    reporting_set_results: Sequence[ReportingSetResult],
    primitive_reporting_sets_by_reporting_set_id: dict[str, list[str]],
) -> None:
    """Performs validation checks on the input ReportResult.

    The function ensures that the `ReportResult` proto is well-formed and
    contains all the required fields. It also checks that the map
    primitive_reporting_sets_by_reporting_set_id has the required information for the
    conversion.

    Args:
      reporting_set_results: List of `ReportingSetResult`s to be validated.
      primitive_reporting_sets_by_reporting_set_id: A reporting set IDs to EDP
        combination map.

    Raises:
      ValueError: If any validation check fails.
    """
    if not reporting_set_results:
        raise ValueError("There must be at least one ReportingSetResult")

    # Validates that the report result has the required top-level fields.
    for reporting_set_result in reporting_set_results:
        if not reporting_set_result.cmms_measurement_consumer_id:
            raise ValueError(
                "The ReportingSetResult must have a"
                " cmms_measurement_consumer_id."
            )
        if not reporting_set_result.external_report_result_id:
            raise ValueError(
                "The ReportingSetResult must have an external_report_result_id."
            )

        # Each reporting set result must have a dimension and a value.
        if not reporting_set_result.HasField("dimension"):
            raise ValueError("ReportingSetResult must have a dimension.")

        dimension = reporting_set_result.dimension

        # Validates that the reporting set result dimension has the required fields.
        if not dimension.external_reporting_set_id:
            raise ValueError(
                "ReportingSetResultKey must have an external_reporting_set_id."
            )
        if (
            dimension.venn_diagram_region_type
            == ReportingSetResult.Dimension.VennDiagramRegionType.VENN_DIAGRAM_REGION_TYPE_UNSPECIFIED
        ):
            raise ValueError(
                "ReportingSetResultKey must have a venn_diagram_region_type."
            )
        if not dimension.WhichOneof("impression_qualification_filter"):
            raise ValueError(
                "ReportingSetResultKey must have an"
                " impression_qualification_filter."
            )
        if not dimension.HasField("metric_frequency_spec"):
            raise ValueError(
                "ReportingSetResultKey must have a metric_frequency_spec."
            )

        # Validates that the external_reporting_set_id is mapped to a set of
        # primitive reporting sets.
        if (
            str(dimension.external_reporting_set_id)
            not in primitive_reporting_sets_by_reporting_set_id
        ):
            raise ValueError(
                "Cannot find the data providers for reporting set "
                f"{dimension.external_reporting_set_id}."
            )

        # Validates the population.
        if reporting_set_result.population_size <= 0:
            raise ValueError("Population size must have a positive value.")

        # Validates that each reporting window result has the required fields.
        for window_entry in reporting_set_result.reporting_window_results:
            if not window_entry.value.HasField(
                "unprocessed_report_result_values"
            ):
                raise ValueError(
                    "Missing unprocessed_report_result_values field."
                )
            if window_entry.value.unprocessed_report_result_values.HasField(
                "non_cumulative_results"
            ) and not window_entry.key.HasField("non_cumulative_start"):
                raise ValueError(
                    "ReportingWindow with non_cumulative_results must have a "
                    "non-cumulative start date."
                )
            if not window_entry.key.HasField("end"):
                raise ValueError("ReportingWindow must have an end date.")
            if dimension.metric_frequency_spec.WhichOneof(
                "selector"
            ) == "total" and window_entry.value.unprocessed_report_result_values.HasField(
                "non_cumulative_results"
            ):
                raise ValueError(
                    "Non cumulative results cannot have metric frequency spec"
                    " of TOTAL."
                )


def _group_reporting_set_results(
    reporting_set_results: Sequence[ReportingSetResult],
) -> defaultdict[tuple[int, int], list[ReportingSetResult]]:
    """Groups reporting set results by their grouping predicates (demographic
    groupings and event filters).

    The function iterates through all `reporting_set_results` and groups them based on their dimension filters (i.e.,
    demographic groupings and event filters).

    Args:
        reporting_set_results: The input `ReportingSetResult`s.

    Returns:
        A dictionary where keys are the filter group keys and values are lists
        of `ReportingSetResult`s that belong to the group.
    """
    grouped_results = defaultdict(list)
    for reporting_set_result in reporting_set_results:
        group_key = (
            reporting_set_result.grouping_dimension_fingerprint,
            reporting_set_result.filter_fingerprint,
        )
        grouped_results[group_key].append(reporting_set_result)
    return grouped_results


def _create_report_summary_for_group(
    cmms_measurement_consumer_id: str,
    external_report_result_id: int,
    group_key: tuple[int, int],
    results_for_group: Sequence[ReportingSetResult],
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
    report_summary.external_report_result_id = external_report_result_id

    # Verifies that there is at least one result for this group.
    if not results_for_group:
        raise ValueError(f"No results found for group: {group_key}.")

    # Since all results in results_for_group share the same grouping key, we get
    # the groupings and event filters from the first result.
    report_summary.groupings.extend(
        _extract_event_template_fields(results_for_group[0].dimension.grouping)
    )
    report_summary.event_filters.extend(
        results_for_group[0].dimension.event_filters
    )

    report_summary.population = _get_population(results_for_group)

    for reporting_set_result in results_for_group:
        _process_reporting_set_result(
            report_summary,
            reporting_set_result,
            primitive_reporting_sets_by_reporting_set_id,
        )

    return report_summary


def _extract_event_template_fields(
    grouping: ReportingSetResult.Dimension.Grouping,
) -> Iterable[EventTemplateField]:
    """Extracts `EventTemplateField` values from the given grouping.

    Args:
        grouping: `Grouping` for a `ReportingSetResult`
    Returns:
        `EventTemplateField` values
    """
    for path in sorted(grouping.value_by_path):
        field = EventTemplateField()
        field.path = path
        field.value.CopyFrom(grouping.value_by_path[path])
        yield field


def _process_reporting_set_result(
    report_summary: ReportSummaryV2,
    reporting_set_result: ReportingSetResult,
    primitive_reporting_sets_by_reporting_set_id: dict[str, list[str]],
) -> None:
    """Extracts data from a `ReportingSetResult` and adds it to a `ReportSummaryV2`.

    This function takes a single result entry from the input `ReportResult` and
    mapping its data to a new `ReportSummarySetResult` of the target
    `ReportSummaryV2`. It handles the translation of impression
    filters, set operations, data providers, and sorts the time-series data
    before copying the metric values.

    Args:
        report_summary: The `ReportSummaryV2` message to be populated.
        reporting_set_result: The source `ReportingSetResult` to
          process.
        primitive_reporting_sets_by_reporting_set_id: A dictionary mapping reporting set
          IDs to their data provider lists.
    """
    report_summary_set_result = _add_report_summary_set_result_metadata(
        reporting_set_result,
        primitive_reporting_sets_by_reporting_set_id,
        report_summary,
    )
    _process_reporting_windows(reporting_set_result, report_summary_set_result)


def _add_report_summary_set_result_metadata(
    reporting_set_result: ReportingSetResult,
    primitive_reporting_sets_by_reporting_set_id: dict[str, list[str]],
    report_summary: ReportSummaryV2,
) -> ReportSummaryV2.ReportSummarySetResult:
    """Adds and populates metadata for a new ReportSummarySetResult.

    This function creates a new `ReportSummarySetResult` within the given
    `ReportSummaryV2` and populates its metadata fields, such as impression
    filter, set operation, and data providers, based on the input
    `ReportingSetResult`.

    Args:
        reporting_set_result: The source entry containing the metadata.
        primitive_reporting_sets_by_reporting_set_id: A mapping from reporting set ID to
          data provider lists.
        report_summary: The `ReportSummaryV2` to which the new set result will
          be added.

    Returns:
        The newly created and populated `ReportSummarySetResult`.
    """
    dimension = reporting_set_result.dimension
    report_summary_set_result = report_summary.report_summary_set_results.add()

    report_summary_set_result.external_reporting_set_result_id = (
        reporting_set_result.external_reporting_set_result_id)

    # Gets the impression filter.
    if dimension.custom:
        report_summary_set_result.impression_filter = "custom"
    else:
        report_summary_set_result.impression_filter = (
            dimension.external_impression_qualification_filter_id
        )

    # Gets the set operation.
    if (
        dimension.venn_diagram_region_type
        == ReportingSetResult.Dimension.VennDiagramRegionType.UNION
    ):
        report_summary_set_result.set_operation = "union"
    elif (
        dimension.venn_diagram_region_type
        == ReportingSetResult.Dimension.VennDiagramRegionType.PRIMITIVE
    ):
        report_summary_set_result.set_operation = "primitive"
    else:
        raise ValueError(
            "Unsupported venn diagram region type:"
            f" {dimension.venn_diagram_region_type}."
        )

    # Get data providers
    reporting_set_id = str(dimension.external_reporting_set_id)
    report_summary_set_result.data_providers.extend(
        sorted(primitive_reporting_sets_by_reporting_set_id[reporting_set_id]))

    report_summary_set_result.metric_frequency_spec.CopyFrom(
        dimension.metric_frequency_spec)

    return report_summary_set_result


def _process_reporting_windows(
    reporting_set_result: ReportingSetResult,
    report_summary_set_result: ReportSummaryV2.ReportSummarySetResult,
) -> None:
    """Processes and copies reporting window data to report summary set result.

    This function sorts the reporting windows by date and iterates through them,
    copying cumulative, weekly non-cumulative, and total campaign
    non-cumulative results to the target `ReportSummarySetResult`.

    Args:
        reporting_set_result: The source entry containing the window
          results.
        report_summary_set_result: The destination `ReportSummarySetResult` to
          populate.
    """
    dimension = reporting_set_result.dimension
    external_reporting_set_result_id = (
        reporting_set_result.external_reporting_set_result_id)
    sorted_windows = sorted(
        reporting_set_result.reporting_window_results,
        key=lambda x: _proto_date_to_datetime(x.key.end),
    )

    for window_entry in sorted_windows:
        noisy_values = window_entry.value.unprocessed_report_result_values

        # Processes cumulative results. The whole campaign result has the
        # metric_frequency_spec_type of TOTAL.
        if noisy_values.HasField("cumulative_results"):
            is_cumulative = True
            metric_frequency_spec_type = (
                dimension.metric_frequency_spec.WhichOneof("selector")
            )
            if metric_frequency_spec_type == "total":
                window_result = report_summary_set_result.whole_campaign_result
                _copy_window_results(
                    noisy_values.cumulative_results,
                    external_reporting_set_result_id,
                    is_cumulative,
                    window_entry.key,
                    window_result
                )
            elif metric_frequency_spec_type == "weekly":
                window_result = report_summary_set_result.cumulative_results.add(
                )
                _copy_window_results(
                    noisy_values.cumulative_results,
                    external_reporting_set_result_id,
                    is_cumulative,
                    window_entry.key,
                    window_result
                )
            else:
                raise ValueError(f"Unsupported metric frequency type: "
                                 f"{metric_frequency_spec_type}")

        # Processes non-cumulative result.
        if noisy_values.HasField("non_cumulative_results"):
            is_cumulative = False
            window_result = report_summary_set_result.non_cumulative_results.add(
            )
            _copy_window_results(
                noisy_values.non_cumulative_results,
                external_reporting_set_result_id,
                is_cumulative,
                window_entry.key,
                window_result
            )


def _copy_window_results(
    noisy_metric_set: NoisyMetricSet,
    external_reporting_set_result_id,
    is_cumulative: bool,
    reporting_window: ReportingSetResult.ReportingWindow,
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
      is_cumulative: A boolean indicating if the metric is cumulative.
      reporting_window: The source reporting window, used for the metric name and
        to populate the key.
      """

    # Copies the reporting window key.
    report_summary_window_result.key.CopyFrom(reporting_window)

    base_metric_name = get_metric_name(
        external_reporting_set_result_id,
        is_cumulative,
        reporting_window
    )

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
            frequency_bin = report_summary_window_result.frequency.bins[key]
            frequency_bin.value = bin_result.value
            if bin_result.HasField("univariate_statistics"):
                frequency_bin.standard_deviation = (
                    bin_result.univariate_statistics.standard_deviation
                )
        report_summary_window_result.frequency.metric = f"frequency_{base_metric_name}"


def get_metric_name(
    external_reporting_set_result_id: int,
    is_cumulative: bool,
    reporting_window: ReportingSetResult.ReportingWindow,
) -> str:
    """Generates the unique name for the metric ID.

    Args:
        metric_frequency_type: The frequency of the metric (e.g., TOTAL, WEEKLY).
        report_summary_set_result: The parent set result which is used to get the
          data providers and impression filter.
        is_cumulative: A boolean indicating if the metric is cumulative.
        reporting_window: The source reporting window, used for the metric name.

    Returns:
        The base name for the metric.
    """
    metric_name_parts = [str(external_reporting_set_result_id)]

    if is_cumulative:
        metric_name_parts.append("cumulative")
    else:
        metric_name_parts.append("non_cumulative")

    metric_name_parts.append(
        f"{reporting_window.end.year}_{reporting_window.end.month:02d}_{reporting_window.end.day:02d}"
    )
    return "_".join(metric_name_parts)


def _get_population(results_for_group: Sequence[ReportingSetResult]) -> int:
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
    for reporting_set_result in results_for_group:
        current_population = reporting_set_result.population_size
        if population is None:
            population = current_population
        elif population != current_population:
            raise ValueError(
                "Inconsistent population sizes found within the same result group."
            )
    if population is not None:
        return population
    else:
        raise ValueError("Population not found.")


def _proto_date_to_datetime(proto_date: ProtoDate) -> date:
    """Converts a google.type.Date to a Python datetime.date."""
    return date(proto_date.year, proto_date.month, proto_date.day)
