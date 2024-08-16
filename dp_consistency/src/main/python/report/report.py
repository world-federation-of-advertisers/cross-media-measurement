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

from src.main.python.noiseninja.noised_measurements import SetMeasurementsSpec, Measurement
from src.main.python.noiseninja.solver import Solver
from typing import FrozenSet
from itertools import combinations
from functools import reduce


class MetricReport:
    """Represents a metric sub-report view (e.g. MRC, AMI, etc)
    within a report.
    """

    __reach_time_series_by_edp_combination: dict[FrozenSet[str], list[Measurement]]

    def __init__(
        self,
        reach_time_series_by_edp_combination: dict[FrozenSet[str], list[Measurement]],
    ):
        num_periods = len(next(iter(reach_time_series_by_edp_combination.values())))
        for series in reach_time_series_by_edp_combination.values():
            if len(series) != num_periods:
                raise ValueError(
                    "all time series must have the same length {1: d} vs {2: d}".format(
                        len(series), len(num_periods)
                    )
                )

        self.__reach_time_series_by_edp_combination = (
            reach_time_series_by_edp_combination
        )

    def sample_with_noise(self) -> "MetricReport":
        """
        :return: a new MetricReport where measurements have been resampled
        according to their mean and variance.
        """
        return MetricReport(
            reach_time_series_by_edp_combination={
                edp_comb: [
                    MetricReport.__sample_with_noise(measurement)
                    for measurement in self.__reach_time_series_by_edp_combination[
                        edp_comb
                    ]
                ]
                for edp_comb in self.__reach_time_series_by_edp_combination.keys()
            }
        )

    def get_edp_comb_measurement(self, edp_comb: str, period: int):
        return self.__reach_time_series_by_edp_combination[edp_comb][period]

    def get_edp_combs(self):
        return list(self.__reach_time_series_by_edp_combination.keys())

    def get_num_edp_combs(self):
        return len(self.__reach_time_series_by_edp_combination.keys())

    def get_number_of_periods(self):
        return len(next(iter(self.__reach_time_series_by_edp_combination.values())))

    def get_subset_relationships(self):
        """Returns a list of tuples where first element in the tuple is the parent
        and second element is the subset."""
        subset_relationships = []
        edp_combinations = list(self.__reach_time_series_by_edp_combination)

        for comb1, comb2 in combinations(edp_combinations, 2):
            if comb1.issubset(comb2):
                subset_relationships.append((comb2, comb1))
            elif comb2.issubset(comb1):
                subset_relationships.append((comb1, comb2))
        return subset_relationships

    def get_cover_relationships(self):
        """Returns covers as defined here: # https://en.wikipedia.org/wiki/Cover_(topology).
        For each set (s_i) in the list, enumerate combinations of all sets excluding this one.
        For each of these considered combinations, take their union and check if it is equal to
        s_i. If so, this combination is a cover of s_i.
        """

        def generate_all_length_combinations(data):
            return [
                comb for r in range(1, len(data) + 1) for comb in combinations(data, r)
            ]

        cover_relationships = []
        edp_combinations = list(self.__reach_time_series_by_edp_combination)
        for i in range(len(edp_combinations)):
            possible_covered = edp_combinations[i]
            other_sets = edp_combinations[:i] + edp_combinations[i + 1 :]
            all_subsets_of_possible_covered = [other_set for other_set in other_sets if other_set.issubset(possible_covered)]
            possible_covers = generate_all_length_combinations(all_subsets_of_possible_covered)
            for possible_cover in possible_covers:
                union_of_possible_cover = reduce(
                    lambda x, y: x.union(y), possible_cover
                )
                if union_of_possible_cover == possible_covered:
                    cover_relationships.append((possible_covered, possible_cover))
        return cover_relationships

    @staticmethod
    def __sample_with_noise(measurement: Measurement):
        return Measurement(
            measurement.value + random.gauss(0, measurement.sigma), measurement.sigma
        )


class Report:
    """
    Represents a full report, consisting of multiple MetricReports,
    which may have set relationships between each other.
    """

    __metric_reports: dict[str, MetricReport]
    __metric_subsets_by_parent: dict[str, list[str]]
    __metric_index: dict[str, int]
    __edp_comb_index: dict[str, int]

    def __init__(
        self,
        metric_reports: dict[str, MetricReport],
        metric_subsets_by_parent: dict[str, list[str]],
        cumulative_inconsistency_allowed_edp_combs: set[str],
    ):
        """
        Args:
            metric_reports: a dictionary mapping metric types to a MetricReport
            metric_subsets_by_parent: a dictionary containing subset
                relationship between the metrics. .e.g. ami >= [custom, mrc]
            cumulative_inconsistency_allowed_edps : a set containing edp keys that won't
                be forced to have self cumulative reaches be increasing
        """
        self.__metric_reports = metric_reports
        self.__metric_subsets_by_parent = metric_subsets_by_parent
        self.__cumulative_inconsistency_allowed_edp_combs = (
            cumulative_inconsistency_allowed_edp_combs
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

        self.__metric_index = {}
        for index, metric in enumerate(metric_reports.keys()):
            self.__metric_index[metric] = index

        self.__edp_comb_index = {}
        for index, edp_comb in enumerate(
            next(iter(metric_reports.values())).get_edp_combs()
        ):
            self.__edp_comb_index[edp_comb] = index

        self.__num_edp_combs = len(self.__edp_comb_index.keys())
        self.__num_periods = next(iter(metric_reports.values())).get_number_of_periods()

        num_vars_per_period = (self.__num_edp_combs + 1) * len(metric_reports.keys())
        self.__num_vars = self.__num_periods * num_vars_per_period

    def get_metric_report(self, metric: str) -> MetricReport:
        return self.__metric_reports[metric]

    def get_metrics(self) -> set[str]:
        return set(self.__metric_reports.keys())

    def get_corrected_report(self) -> "Report":
        """Returns a corrected, consistent report.
        Note all measurements in the corrected report are set to have 0 variance
        """
        spec = self.to_set_measurement_spec()
        solution = Solver(spec).solve_and_translate()
        return self.report_from_solution(solution)

    def report_from_solution(self, solution):
        return Report(
            metric_reports={
                metric: self.__metric_report_from_solution(metric, solution)
                for metric in self.__metric_reports
            },
            metric_subsets_by_parent=self.__metric_subsets_by_parent,
            cumulative_inconsistency_allowed_edp_combs=self.__cumulative_inconsistency_allowed_edp_combs,
        )

    def sample_with_noise(self) -> "Report":
        """Returns a new report sampled according to the mean and variance of
        all metrics in this report. Useful to bootstrap sample reports.
        """
        return Report(
            metric_reports={
                i: self.__metric_reports[i].sample_with_noise()
                for i in self.__metric_reports
            },
            metric_subsets_by_parent=self.__metric_subsets_by_parent,
            cumulative_inconsistency_allowed_edp_combs=self.__cumulative_inconsistency_allowed_edp_combs,
        )

    def to_array(self) -> np.array:
        """Returns an array representation of all the mean measurement values
        in this report
        """
        array = np.zeros(self.__num_vars)
        for metric in self.__metric_reports:
            for period in range(0, self.__num_periods):
                for edp_comb in self.__edp_comb_index:
                    edp_comb_ind = self.__edp_comb_index[edp_comb]
                    array.put(
                        self.__get_var_index(
                            period, self.__metric_index[metric], edp_comb_ind
                        ),
                        self.__metric_reports[metric]
                        .get_edp_comb_measurement(edp_comb, period)
                        .value,
                    )
        return array

    def to_set_measurement_spec(self):
        spec = SetMeasurementsSpec()
        self.__add_measurements_to_spec(spec)
        self.__add_set_relations_to_spec(spec)
        return spec

    def __add_set_relations_to_spec(self, spec):
        for period in range(0, self.__num_periods):

            # sum of subsets >= union for each period
            for metric in self.__metric_reports:
                metric_ind = self.__metric_index[metric]
                for cover_relationship in self.__metric_reports[
                    metric
                ].get_cover_relationships():
                    covered_parent = cover_relationship[0]
                    covering_children = cover_relationship[1]
                    spec.add_cover(
                        children=list(
                            self.__get_var_index(
                                period,
                                metric_ind,
                                self.__edp_comb_index[covering_child],
                            )
                            for covering_child in covering_children
                        ),
                        parent=self.__get_var_index(
                            period, metric_ind, self.__edp_comb_index[covered_parent]
                        ),
                    )

            # subset <= union
            for metric in self.__metric_reports:
                metric_ind = self.__metric_index[metric]
                for subset_relationship in self.__metric_reports[
                    metric
                ].get_subset_relationships():
                    parent_edp_comb = subset_relationship[0]
                    child_edp_comb = subset_relationship[1]
                    spec.add_subset_relation(
                        child_set_id=self.__get_var_index(
                            period, metric_ind, self.__edp_comb_index[child_edp_comb]
                        ),
                        parent_set_id=self.__get_var_index(
                            period, metric_ind, self.__edp_comb_index[parent_edp_comb]
                        ),
                    )

            # metric1>=metric#2
            for parent_metric in self.__metric_subsets_by_parent:
                for child_metric in self.__metric_subsets_by_parent[parent_metric]:
                    for edp_comb in self.__edp_comb_index:
                        edp_comb_ind = self.__edp_comb_index[edp_comb]
                        spec.add_subset_relation(
                            child_set_id=self.__get_var_index(
                                period, self.__metric_index[child_metric], edp_comb_ind
                            ),
                            parent_set_id=self.__get_var_index(
                                period, self.__metric_index[parent_metric], edp_comb_ind
                            ),
                        )

            # period1 <= period2
            for edp_comb in self.__edp_comb_index:
                if (
                    len(edp_comb) == 1
                    and next(iter(edp_comb))
                    in self.__cumulative_inconsistency_allowed_edp_combs
                ):
                    continue
                if period >= self.__num_periods - 1:
                    continue
                for metric in range(0, len(self.__metric_index.keys())):
                    edp_comb_ind = self.__edp_comb_index[edp_comb]
                    spec.add_subset_relation(
                        child_set_id=self.__get_var_index(period, metric, edp_comb_ind),
                        parent_set_id=self.__get_var_index(
                            period + 1, metric, edp_comb_ind
                        ),
                    )

    def __add_measurements_to_spec(self, spec):
        for metric in self.__metric_reports:
            for period in range(0, self.__num_periods):
                for edp_comb in self.__edp_comb_index:
                    edp_comb_ind = self.__edp_comb_index[edp_comb]
                    spec.add_measurement(
                        self.__get_var_index(
                            period, self.__metric_index[metric], edp_comb_ind
                        ),
                        self.__metric_reports[metric].get_edp_comb_measurement(
                            edp_comb, period
                        ),
                    )

    def __get_var_index(self, period: int, metric: int, edp: int):
        return (
            metric * self.__num_edp_combs * self.__num_periods
            + edp * self.__num_periods
            + period
        )

    def __metric_report_from_solution(self, metric, solution):
        solution_time_series = {}
        for edp_comb in self.__edp_comb_index:
            edp_comb_ind = self.__edp_comb_index[edp_comb]
            solution_time_series[edp_comb] = [
                Measurement(
                    solution[
                        self.__get_var_index(
                            period, self.__metric_index[metric], edp_comb_ind
                        )
                    ],
                    0,
                )
                for period in range(0, self.__num_periods)
            ]

        return MetricReport(reach_time_series_by_edp_combination=solution_time_series)
