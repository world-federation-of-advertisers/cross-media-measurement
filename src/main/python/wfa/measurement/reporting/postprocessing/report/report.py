import random

# Copyright 2024 The Cross-Media Measurement Authors
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

import numpy as np

from noiseninja.noised_measurements import SetMeasurementsSpec, Measurement
from noiseninja.solver import Solver
from typing import FrozenSet
from itertools import combinations
from functools import reduce

MIN_STANDARD_VARIATION_RATIO = 0.001


def get_subset_relationships(edp_combinations: list[FrozenSet[str]]):
    """Returns a list of tuples where first element in the tuple is the parent
    and second element is the subset."""
    subset_relationships = []

    for comb1, comb2 in combinations(edp_combinations, 2):
        if comb1.issubset(comb2):
            subset_relationships.append((comb2, comb1))
        elif comb2.issubset(comb1):
            subset_relationships.append((comb1, comb2))
    return subset_relationships


def is_cover(target_set, possible_cover):
    """Checks if a collection of sets covers a target set.

    Args:
      target_set: The set that should be covered.
      possible_cover: A collection of sets that may cover the target set.

    Returns:
      True if the union of the sets in `possible_cover` equals `target_set`,
      False otherwise.
    """
    union_of_possible_cover = reduce(
        lambda x, y: x.union(y), possible_cover
    )
    return union_of_possible_cover == target_set


def get_covers(target_set, other_sets):
    """Finds all combinations of sets from `other_sets` that cover `target_set`.

    This function identifies all possible combinations of sets within `other_sets`
    whose union equals the `target_set`. It only considers sets that are subsets of
    the `target_set`.

    Args:
      target_set: The set that needs to be covered.
      other_sets: A collection of sets that may be used to cover the `target_set`.

    Returns:
      A list of tuples, where each tuple represents a covering relationship.
      The first element of the tuple is the `target_set`, and the second element
      is a tuple containing the sets from `other_sets` that cover it.
    """

    def generate_all_length_combinations(data):
        """Generates all possible combinations of elements from a list.

        Args:
          data: The list of elements.

        Returns:
          A list of tuples, where each tuple represents a combination of elements.
        """
        return [
            comb for r in range(1, len(data) + 1) for comb in
            combinations(data, r)
        ]

    cover_relationship = []
    all_subsets_of_possible_covered = [other_set for other_set in other_sets
                                       if
                                       other_set.issubset(target_set)]
    possible_covers = generate_all_length_combinations(
        all_subsets_of_possible_covered)
    for possible_cover in possible_covers:
        if is_cover(target_set, possible_cover):
            cover_relationship.append((target_set, possible_cover))
    return cover_relationship


def get_cover_relationships(edp_combinations: list[FrozenSet[str]]):
    """Returns covers as defined here: # https://en.wikipedia.org/wiki/Cover_(topology).
    For each set (s_i) in the list, enumerate combinations of all sets excluding this one.
    For each of these considered combinations, take their union and check if it is equal to
    s_i. If so, this combination is a cover of s_i.
    """
    cover_relationships = []
    for i in range(len(edp_combinations)):
        possible_covered = edp_combinations[i]
        other_sets = edp_combinations[:i] + edp_combinations[i + 1:]
        cover_relationship = get_covers(possible_covered, other_sets)
        cover_relationships.extend(cover_relationship)
    return cover_relationships


class MetricReport:
    """Represents a metric sub-report view (e.g., MRC, AMI) within a report.

      This class stores and provides access to reach measurements for different
      EDP (Event, Data Provider, and Platform) combinations. It holds two types
      of reach data:

          * Cumulative reach over time, represented as a time series.
          * Reach for the whole campaign.

      Attributes:
          _reach_time_series: A dictionary mapping EDP combinations (represented
                              as frozensets of strings) to lists of Measurement
                              objects, where each list represents a time series of
                              reach values.
          _reach_whole_campaign: A dictionary mapping EDP combinations to
                                 Measurement objects representing the reach for
                                 the whole campaign.
      """

    def __init__(
            self,
            reach_time_series: dict[FrozenSet[str], list[Measurement]],
            reach_whole_campaign: dict[FrozenSet[str], Measurement],
    ):
        num_periods = len(next(iter(reach_time_series.values())))
        for series in reach_time_series.values():
            if len(series) != num_periods:
                raise ValueError(
                    "All time series must have the same length {1: d} vs {2: d}".format(
                        len(series), len(num_periods)
                    )
                )

        self._reach_time_series = reach_time_series
        self._reach_whole_campaign = reach_whole_campaign

    def sample_with_noise(self) -> "MetricReport":
        """
        :return: a new MetricReport where measurements have been resampled
        according to their mean and variance.
        """
        return MetricReport(
            reach_time_series={
                edp_combination: [
                    MetricReport._sample_with_noise(measurement)
                    for measurement in self._reach_time_series[
                        edp_combination
                    ]
                ]
                for edp_combination in
                self._reach_time_series.keys()
            }
        )

    def get_cumulative_measurement(self, edp_combination: str, period: int):
        return self._reach_time_series[edp_combination][
            period]

    def get_whole_campaign_measurement(self, edp_combination: str):
        return self._reach_whole_campaign[edp_combination]

    def get_cumulative_edp_combinations(self):
        return set(self._reach_time_series.keys())

    def get_whole_campaign_edp_combinations(self):
        return set(self._reach_whole_campaign.keys())

    def get_cumulative_edp_combinations_count(self):
        return len(self._reach_time_series.keys())

    def get_whole_campaign_edp_combinations_count(self):
        return len(self._reach_whole_campaign.keys())

    def get_number_of_periods(self):
        return len(next(iter(self._reach_time_series.values())))

    def get_cumulative_subset_relationships(self):
        return get_subset_relationships(list(self._reach_time_series))

    def get_whole_campaign_subset_relationships(self):
        return get_subset_relationships(list(self._reach_whole_campaign))

    def get_cumulative_cover_relationships(self):
        return get_cover_relationships(list(self._reach_time_series))

    def get_whole_campaign_cover_relationships(self):
        return get_cover_relationships(list(self._reach_whole_campaign))

    @staticmethod
    def _sample_with_noise(measurement: Measurement):
        return Measurement(
            measurement.value + random.gauss(0, measurement.sigma),
            measurement.sigma
        )


class Report:
    """Represents a full report with multiple MetricReports and set relationships.

      This class aggregates multiple MetricReport objects, and the subset relation
      between the the metrics.

      Attributes:
          _metric_reports: A dictionary mapping metric names (e.g., "MRC", "AMI")
                           to their corresponding MetricReport objects.
          _metric_subsets_by_parent: A dictionary defining subset relationships
                                     between metrics. Each key is a parent metric,
                                     and the value is a list of its child metrics.
          _cumulative_inconsistency_allowed_edp_combinations: A set of EDP
                                                              combinations for
                                                              which inconsistencies
                                                              in cumulative
                                                              measurements are
                                                              allowed.
      """

    def __init__(
            self,
            metric_reports: dict[str, MetricReport],
            metric_subsets_by_parent: dict[str, list[str]],
            cumulative_inconsistency_allowed_edp_combinations: set[str],
    ):
        """
        Args:
            metric_reports: a dictionary mapping metric types to a MetricReport
            metric_subsets_by_parent: a dictionary containing subset
                relationship between the metrics. .e.g. ami >= [custom, mrc]
            cumulative_inconsistency_allowed_edps : a set containing edp keys that won't
                be forced to have self cumulative reaches be increasing
        """
        self._metric_reports = metric_reports
        self._metric_subsets_by_parent = metric_subsets_by_parent
        self._cumulative_inconsistency_allowed_edp_combinations = (
            cumulative_inconsistency_allowed_edp_combinations
        )

        # All metrics in the set relationships must have a corresponding report.
        for parent in metric_subsets_by_parent.keys():
            if not (parent in metric_reports):
                raise ValueError(
                    "key {1} does not have a corresponding report".format(parent)
                )
            for child in metric_subsets_by_parent[parent]:
                if not (child in metric_reports):
                    raise ValueError(
                        "key {1} does not have a corresponding report".format(child)
                    )

        self._metric_index = {}
        for index, metric in enumerate(metric_reports.keys()):
            self._metric_index[metric] = index

        self._num_periods = next(
            iter(metric_reports.values())).get_number_of_periods()

        # Assign an index to each measurement.
        measurement_index = 0
        self._measurement_name_to_index = {}
        self._max_standard_deviation = 0
        for metric in metric_reports.keys():
            for edp_combination in metric_reports[
                metric].get_whole_campaign_edp_combinations():
                measurement = metric_reports[metric].get_whole_campaign_measurement(
                    edp_combination)
                self._measurement_name_to_index[measurement.name] = measurement_index
                self._max_standard_deviation = max(self._max_standard_deviation, measurement.sigma)
                measurement_index += 1
            for edp_combination in metric_reports[
                metric].get_cumulative_edp_combinations():
                for period in range(0, self._num_periods):
                    measurement = metric_reports[metric].get_cumulative_measurement(
                        edp_combination, period)
                    self._measurement_name_to_index[measurement.name] = measurement_index
                    self._max_standard_deviation = max(self._max_standard_deviation, measurement.sigma)
                    measurement_index += 1

        self._num_vars = measurement_index

    def get_metric_report(self, metric: str) -> MetricReport:
        return self._metric_reports[metric]

    def get_metrics(self) -> set[str]:
        return set(self._metric_reports.keys())

    def get_corrected_report(self) -> "Report":
        """Returns a corrected, consistent report.
        Note all measurements in the corrected report are set to have 0 variance
        """
        spec = self.to_set_measurement_spec()
        solution = Solver(spec).solve_and_translate()
        return self.report_from_solution(solution, spec)

    def report_from_solution(self, solution, spec):
        return Report(
            metric_reports={
                metric: self._metric_report_from_solution(metric, solution)
                for metric in self._metric_reports
            },
            metric_subsets_by_parent=self._metric_subsets_by_parent,
            cumulative_inconsistency_allowed_edp_combinations=self._cumulative_inconsistency_allowed_edp_combinations,
        )

    def sample_with_noise(self) -> "Report":
        """Returns a new report sampled according to the mean and variance of
        all metrics in this report. Useful to bootstrap sample reports.
        """
        return Report(
            metric_reports={
                i: self._metric_reports[i].sample_with_noise()
                for i in self._metric_reports
            },
            metric_subsets_by_parent=self._metric_subsets_by_parent,
            cumulative_inconsistency_allowed_edp_combinations=self._cumulative_inconsistency_allowed_edp_combinations,
        )

    def to_array(self) -> np.array:
        """Returns an array representation of all the mean measurement values
        in this report
        """
        array = np.zeros(self._num_vars)
        for metric in self._metric_reports:
            for edp_combination in self._metric_reports[
                metric].get_cumulative_edp_combinations():
                for period in range(0, self._num_periods):
                    array.put(
                        self._get_measurement_index(
                            self._metric_reports[metric]
                            .get_cumulative_measurement(edp_combination, period)
                        ),
                        self._metric_reports[metric]
                        .get_cumulative_measurement(edp_combination, period)
                        .value,
                    )
            for edp_combination in self._metric_reports[
                metric].get_whole_campaign_edp_combinations():
                array.put(
                    self._get_measurement_index(
                        self._metric_reports[metric]
                        .get_whole_campaign_measurement(edp_combination)
                    ),
                    self._metric_reports[metric]
                    .get_whole_campaign_measurement(edp_combination)
                    .value,
                )
        return array

    def to_set_measurement_spec(self):
        spec = SetMeasurementsSpec()
        self._add_measurements_to_spec(spec)
        self._add_set_relations_to_spec(spec)
        return spec

    def _add_cover_relations_to_spec(self, spec):
        # sum of subsets >= union for each period
        for metric in self._metric_reports:
            for cover_relationship in self._metric_reports[
                metric].get_cumulative_cover_relationships():
                covered_parent = cover_relationship[0]
                covering_children = cover_relationship[1]
                for period in range(0, self._num_periods):
                    spec.add_cover(
                        children=list(self._get_cumulative_measurement_index(
                            metric, covering_child, period)
                                      for covering_child in covering_children),
                        parent=self._get_cumulative_measurement_index(
                            metric, covered_parent, period),
                    )
            for cover_relationship in self._metric_reports[
                metric].get_whole_campaign_cover_relationships():
                covered_parent = cover_relationship[0]
                covering_children = cover_relationship[1]
                spec.add_cover(
                    children=list(self._get_whole_campaign_measurement_index(
                        metric, covering_child)
                                  for covering_child in covering_children),
                    parent=self._get_whole_campaign_measurement_index(
                        metric, covered_parent),
                )

    def _add_subset_relations_to_spec(self, spec):
        # Adds relations for cumulative measurements.
        for metric in self._metric_reports:
            for subset_relationship in self._metric_reports[
                metric
            ].get_cumulative_subset_relationships():
                parent_edp_combination = subset_relationship[0]
                child_edp_combination = subset_relationship[1]
                for period in range(0, self._num_periods):
                    spec.add_subset_relation(
                        child_set_id=self._get_measurement_index(
                            self._metric_reports[
                                metric].get_cumulative_measurement(
                                child_edp_combination, period)),
                        parent_set_id=self._get_measurement_index(
                            self._metric_reports[
                                metric].get_cumulative_measurement(
                                parent_edp_combination, period)),
                    )

            # Adds relations for whole campaign measurements.
            for subset_relationship in self._metric_reports[
                metric
            ].get_whole_campaign_subset_relationships():
                parent_edp_combination = subset_relationship[0]
                child_edp_combination = subset_relationship[1]
                spec.add_subset_relation(
                    child_set_id=self._get_measurement_index(
                        self._metric_reports[
                            metric].get_whole_campaign_measurement(
                            child_edp_combination)),
                    parent_set_id=self._get_measurement_index(
                        self._metric_reports[
                            metric].get_whole_campaign_measurement(
                            parent_edp_combination)),
                )

    # TODO(@ple13):Use timestamp to check if the last cumulative measurement covers
    # the whole campaign. If yes, make sure that the two measurements are equal
    # instead of less than or equal.
    def _add_cumulative_whole_campaign_relations_to_spec(self, spec):
        # Adds relations between cumulative and whole campaign measurements.
        # For an edp combination, the last cumulative measurement is less than or
        # equal to the whole campaign measurement.
        for metric in self._metric_reports:
            for edp_combination in self._metric_reports[
                metric].get_cumulative_edp_combinations().intersection(
                self._metric_reports[
                    metric].get_whole_campaign_edp_combinations()):
                spec.add_subset_relation(
                    child_set_id=self._get_measurement_index(
                        self._metric_reports[
                            metric].get_cumulative_measurement(
                            edp_combination, (self._num_periods - 1))),
                    parent_set_id=self._get_measurement_index(
                        self._metric_reports[
                            metric].get_whole_campaign_measurement(
                            edp_combination)),
                )

    def _add_metric_relations_to_spec(self, spec):
        # metric1>=metric#2
        for parent_metric in self._metric_subsets_by_parent:
            for child_metric in self._metric_subsets_by_parent[parent_metric]:
                # Handles cumulative measurements of common edp combinations.
                for edp_combination in self._metric_reports[
                    parent_metric].get_cumulative_edp_combinations().intersection(
                    self._metric_reports[
                        child_metric].get_cumulative_edp_combinations()):
                    for period in range(0, self._num_periods):
                        spec.add_subset_relation(
                            child_set_id=self._get_measurement_index(
                                self._metric_reports[
                                    child_metric].get_cumulative_measurement(
                                    edp_combination, period)),
                            parent_set_id=self._get_measurement_index(
                                self._metric_reports[
                                    parent_metric].get_cumulative_measurement(
                                    edp_combination, period)),
                        )
                # Handles whole campaign measurements of common edp combinations.
                for edp_combination in self._metric_reports[
                    parent_metric].get_whole_campaign_edp_combinations().intersection(
                    self._metric_reports[
                        child_metric].get_whole_campaign_edp_combinations()):
                    spec.add_subset_relation(
                        child_set_id=self._get_measurement_index(
                            self._metric_reports[
                                child_metric].get_whole_campaign_measurement(
                                edp_combination)),
                        parent_set_id=self._get_measurement_index(
                            self._metric_reports[
                                parent_metric].get_whole_campaign_measurement(
                                edp_combination)),
                    )

    def _add_cumulative_relations_to_spec(self, spec):
        for metric in self._metric_reports.keys():
            for edp_combination in self._metric_reports[
                metric].get_cumulative_edp_combinations():
                if (
                        len(edp_combination) == 1
                        and next(iter(edp_combination))
                        in self._cumulative_inconsistency_allowed_edp_combinations
                ):
                    continue
                for period in range(0, self._num_periods):
                    if period >= self._num_periods - 1:
                        continue
                    spec.add_subset_relation(
                        child_set_id=self._get_measurement_index(
                            self._metric_reports[
                                metric].get_cumulative_measurement(
                                edp_combination, period)),
                        parent_set_id=self._get_measurement_index(
                            self._metric_reports[
                                metric].get_cumulative_measurement(
                                edp_combination, period + 1)),
                    )

    def _add_set_relations_to_spec(self, spec):
        # sum of subsets >= union for each period.
        self._add_cover_relations_to_spec(spec)

        # subset <= union.
        self._add_subset_relations_to_spec(spec)

        # metric1>=metric#2.
        self._add_metric_relations_to_spec(spec)

        # period1 <= period2.
        self._add_cumulative_relations_to_spec(spec)

        # Last cumulative measurement <= whole campaign measurement.
        self._add_cumulative_whole_campaign_relations_to_spec(spec)

    def _add_measurements_to_spec(self, spec):
        for metric in self._metric_reports.keys():
            for edp_combination in self._metric_reports[
                metric].get_cumulative_edp_combinations():
                for period in range(0, self._num_periods):
                    measurement = self._metric_reports[
                        metric].get_cumulative_measurement(edp_combination, period)
                    spec.add_measurement(
                        self._get_measurement_index(measurement),
                        Measurement(measurement.value,
                                    max(measurement.sigma / self._max_standard_deviation, MIN_STANDARD_VARIATION_RATIO),
                                    measurement.name),
                    )
            for edp_combination in self._metric_reports[
                metric].get_whole_campaign_edp_combinations():
                measurement = self._metric_reports[
                    metric].get_whole_campaign_measurement(edp_combination)
                spec.add_measurement(
                    self._get_measurement_index(measurement),
                    Measurement(measurement.value,
                                max(measurement.sigma / self._max_standard_deviation, MIN_STANDARD_VARIATION_RATIO),
                                measurement.name),
                )

    def _get_measurement_index(self, measurement: Measurement):
        return self._measurement_name_to_index[measurement.name]

    def _get_cumulative_measurement_index(self, metric: str,
                                          edp_combination: str, period: int):
        return self._get_measurement_index(
            self._metric_reports[metric].get_cumulative_measurement(
                edp_combination, period)
        )

    def _get_whole_campaign_measurement_index(self, metric: str,
                                              edp_combination: str):
        return self._get_measurement_index(
            self._metric_reports[metric].get_whole_campaign_measurement(
                edp_combination)
        )

    def _metric_report_from_solution(self, metric, solution):
        solution_time_series = {}
        solution_whole_campaign = {}
        for edp_combination in self._metric_reports[
            metric].get_cumulative_edp_combinations():
            solution_time_series[edp_combination] = [
                Measurement(
                    solution[
                        self._get_measurement_index(self._metric_reports[
                            metric].get_cumulative_measurement(
                            edp_combination, period))
                    ],
                    self._metric_reports[metric].get_cumulative_measurement(
                        edp_combination, period).sigma,
                    self._metric_reports[metric].get_cumulative_measurement(
                        edp_combination, period).name,
                )
                for period in range(0, self._num_periods)
            ]
        for edp_combination in self._metric_reports[
            metric].get_whole_campaign_edp_combinations():
            solution_whole_campaign[edp_combination] = Measurement(
                solution[
                    self._get_measurement_index(self._metric_reports[
                        metric].get_whole_campaign_measurement(
                        edp_combination))
                ],
                self._metric_reports[metric].get_whole_campaign_measurement(
                    edp_combination).sigma,
                self._metric_reports[metric].get_whole_campaign_measurement(
                    edp_combination).name,
            )
        return MetricReport(
            reach_time_series=solution_time_series,
            reach_whole_campaign=solution_whole_campaign,
        )
