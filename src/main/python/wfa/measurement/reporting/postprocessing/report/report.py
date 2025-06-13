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

import math
import numpy as np
import random
from functools import reduce
from itertools import combinations
from typing import Any
from typing import FrozenSet
from typing import Optional
from typing import Tuple

from absl import logging
from qpsolvers import Solution

from noiseninja.noised_measurements import Measurement
from noiseninja.noised_measurements import OrderedSets
from noiseninja.noised_measurements import SetMeasurementsSpec
from noiseninja.solver import Solver
from src.main.proto.wfa.measurement.reporting.postprocessing.v2alpha import \
  report_post_processor_result_pb2

ReportPostProcessorResult = report_post_processor_result_pb2.ReportPostProcessorResult
ReportPostProcessorStatus = report_post_processor_result_pb2.ReportPostProcessorStatus
ReportQuality = report_post_processor_result_pb2.ReportQuality

MIN_STANDARD_VARIATION_RATIO = 0.001
UNIT_SCALING_FACTOR = 1.0
TOLERANCE = 1e-6

# The probability of a value falling outside the [-7*STDDEV; 7*STDDEV] range is
# approximately 2^{-38.5}.
STANDARD_DEVIATION_TEST_THRESHOLD = 7.0

CONSISTENCY_TEST_TOLERANCE = 1.0


def fuzzy_equal(val: float, target: float, tolerance: float) -> bool:
  """Checks if two float values are approximately equal within an absolute tolerance."""
  return math.isclose(val, target, rel_tol=0.0, abs_tol=tolerance)


def fuzzy_less_equal(smaller: float, larger: float, tolerance: float) -> bool:
  """Checks if one float value is less than or equal to another within a tolerance."""
  return larger - smaller + tolerance >= 0


def get_subset_relationships(edp_combinations: list[FrozenSet[str]]) -> list[
  Tuple[FrozenSet[str], FrozenSet[str]]]:
  """Returns a list of tuples where first element in the tuple is the parent
  and second element is the subset."""
  logging.debug(
      "Getting subset relations for the list of EDP combinations "
      f"{edp_combinations}."
  )
  subset_relationships = []
  for comb1, comb2 in combinations(edp_combinations, 2):
    if comb1.issubset(comb2):
      subset_relationships.append((comb2, comb1))
    elif comb2.issubset(comb1):
      subset_relationships.append((comb1, comb2))
  logging.debug(
      f"The subset relationships for {edp_combinations} are "
      f"{subset_relationships}."
  )
  return subset_relationships


def is_cover(target_set: FrozenSet[str],
    possible_cover: list[FrozenSet[str]]) -> bool:
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


def get_covers(target_set: FrozenSet[str], other_sets: list[FrozenSet[str]]) -> \
    list[Tuple[FrozenSet[str], list[FrozenSet[str]]]]:
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
  logging.debug(f"Getting cover relations for {target_set} from {other_sets}.")

  def generate_all_length_combinations(data: list[Any]) -> list[
    tuple[Any, ...]]:
    """Generates all possible combinations of elements from a list.

    Args:
      data: A list of elements.

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
  logging.debug(
      f"The cover relationship is {cover_relationship}."
  )
  return cover_relationship


def get_cover_relationships(edp_combinations: list[FrozenSet[str]]) -> list[
  Tuple[FrozenSet[str], list[FrozenSet[str]]]]:
  """Returns covers as defined here: # https://en.wikipedia.org/wiki/Cover_(topology).
  For each set (s_i) in the list, enumerate combinations of all sets excluding this one.
  For each of these considered combinations, take their union and check if it is equal to
  s_i. If so, this combination is a cover of s_i.
  """
  logging.debug(
      "Getting all cover relationships from a list of EDP combinations "
      f"{edp_combinations}"
  )
  cover_relationships = []
  for i in range(len(edp_combinations)):
    possible_covered = edp_combinations[i]
    other_sets = edp_combinations[:i] + edp_combinations[i + 1:]
    cover_relationship = get_covers(possible_covered, other_sets)
    cover_relationships.extend(cover_relationship)
  return cover_relationships


def is_union_reach_consistent(
    union_measurement: Measurement,
    component_measurements: list[Measurement], population_size: float) -> bool:
  """Verifies that the expected union reach is statistically consistent with
  individual EDP measurements assuming conditional independence between the sets
  of VIDs reached by the different EDPs.

  The check is done by comparing the absolute difference between the observed
  union reach and the expected union reach against a confidence range.

  Let U be the population size, X_1, ..., X_n be the single EDPs. If the reach
  of the EDPs are independent of one another, the expected union reach is:
      |X_1 union … union Xn_| = U - (U - |X_1|)...(U - |X_n|)/U^{n-1}

  Let D = expected union - measuremed union.

  The standard deviation of the difference between the expected union reach
  and the measured union reach is bounded by
  std(D) <= sqrt(var(|X_1|) + ... + var(|X_n|) + var(measured union)).

  Returns:
    True if D is in [-7*std(D); 7*std(D)].
    False otherwise.
  """

  if population_size <= 0:
    raise ValueError(
        f"The population size must be greater than 0, but got"
        f" {population_size}."
    )

  if len(component_measurements) <= 1:
    raise ValueError(
        f"The length of individual reaches must be at least 2, but got"
        f" {len(component_measurements)}."
    )

  variance = union_measurement.sigma ** 2

  probability = 1.0

  for measurement in component_measurements:
    probability *= max(0.0, 1.0 - measurement.value / population_size)
    variance += measurement.sigma ** 2

  probability = min(1.0, probability)

  expected_union_measurement = population_size * (1.0 - probability)

  # An upperbound of STDDEV(expected union - measured union).
  standard_deviation = np.sqrt(variance)

  return abs(expected_union_measurement - union_measurement.value) <= \
    STANDARD_DEVIATION_TEST_THRESHOLD * standard_deviation


def get_edps_from_edp_combination(edp_combination: FrozenSet[str],
    all_edp_combinations: FrozenSet[str]) -> list[FrozenSet[str]]:
  edps: list[FrozenSet[str]] = all_edp_combinations.intersection(
      [frozenset({edp}) for edp in edp_combination]
  )
  return edps


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
      k_reach: dict[FrozenSet[str], dict[int, Measurement]],
      impression: dict[FrozenSet[str], Measurement],
  ):
    num_periods = len(
        next(iter(reach_time_series.values()))) if reach_time_series else 0
    num_frequencies = len(next(iter(k_reach.values()))) if k_reach else 0

    for series in reach_time_series.values():
      if len(series) != num_periods:
        raise ValueError(
            "All time series must have the same length {1: d} vs {2: d}".format(
                len(series), len(num_periods)
            )
        )

    for item in k_reach.values():
      if len(item) != num_frequencies:
        raise ValueError(
            "All k_reach must have the same length {a: d} vs {2: d}".format(
                len(item), len(num_frequencies)
            )
        )

    self._reach_time_series = reach_time_series
    self._reach_whole_campaign = reach_whole_campaign
    self._impression = impression
    self._k_reach = k_reach

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

  def get_cumulative_measurements(self, edp_combination: FrozenSet[str]) -> \
      list[Measurement]:
    return self._reach_time_series[edp_combination]

  def get_cumulative_measurement(self, edp_combination: FrozenSet[str],
      period: int) -> Measurement:
    return self._reach_time_series[edp_combination][period]

  def get_whole_campaign_measurement(self,
      edp_combination: FrozenSet[str]) -> Measurement:
    return self._reach_whole_campaign[edp_combination]

  def get_impression_measurement(self,
      edp_combination: FrozenSet[str]) -> Measurement:
    return self._impression[edp_combination]

  def get_k_reach_measurements(self, edp_combination: FrozenSet[str]) -> list[
    Measurement]:
    return [measurement for measurement in
            self._k_reach[edp_combination].values()]

  def get_k_reach_measurement(self, edp_combination: FrozenSet[str],
      frequency: int) -> Measurement:
    return self._k_reach[edp_combination][frequency]

  def get_cumulative_edp_combinations(self) -> set[FrozenSet[str]]:
    return set(self._reach_time_series.keys())

  def get_whole_campaign_edp_combinations(self) -> set[FrozenSet[str]]:
    return set(self._reach_whole_campaign.keys())

  def get_impression_edp_combinations(self) -> set[FrozenSet[str]]:
    return set(self._impression.keys())

  def get_k_reach_edp_combinations(self) -> set[FrozenSet[str]]:
    return set(self._k_reach.keys())

  def get_cumulative_edp_combinations_count(self) -> int:
    return len(self._reach_time_series.keys())

  def get_whole_campaign_edp_combinations_count(self) -> int:
    return len(self._reach_whole_campaign.keys())

  def get_number_of_periods(self) -> int:
    return len(next(iter(self._reach_time_series.values()))) \
      if self._reach_time_series else 0

  def get_number_of_frequencies(self) -> int:
    return len(next(iter(self._k_reach.values()))) if self._k_reach else 0

  def get_cumulative_subset_relationships(self) -> list[
    Tuple[FrozenSet[str], FrozenSet[str]]]:
    return get_subset_relationships(list(self._reach_time_series))

  def get_whole_campaign_subset_relationships(self) -> list[
    Tuple[FrozenSet[str], FrozenSet[str]]]:
    return get_subset_relationships(list(self._reach_whole_campaign))

  def get_cumulative_cover_relationships(self) -> list[
    Tuple[FrozenSet[str], list[FrozenSet[str]]]]:
    return get_cover_relationships(list(self._reach_time_series))

  def get_whole_campaign_cover_relationships(self) -> list[
    Tuple[FrozenSet[str], list[FrozenSet[str]]]]:
    return get_cover_relationships(list(self._reach_whole_campaign))

  @staticmethod
  def _sample_with_noise(measurement: Measurement) -> Measurement:
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
                                                            allowed. This is for
                                                            TV measurements.
        _population_size: The size of the population.
    """

  def __init__(
      self,
      metric_reports: dict[str, MetricReport],
      metric_subsets_by_parent: dict[str, list[str]],
      cumulative_inconsistency_allowed_edp_combinations: set[str],
      population_size: float = 0.0,
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
    self._population_size = population_size

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

    self._num_frequencies = next(
        iter(metric_reports.values())).get_number_of_frequencies()

    # Assigns an index to each measurement and keeps track of the max standard
    # deviation. This max standard deviation will be used to normalized the
    # standard deviation of the measurements when the report is corrected.
    measurement_index = 0
    self._measurement_name_to_index = {}
    self._max_standard_deviation = UNIT_SCALING_FACTOR
    for metric in metric_reports.keys():
      # Assigns an index for whole campaign reaches.
      for edp_combination in metric_reports[
        metric].get_whole_campaign_edp_combinations():
        measurement = metric_reports[metric].get_whole_campaign_measurement(
            edp_combination)
        self._measurement_name_to_index[measurement.name] = measurement_index
        self._max_standard_deviation = max(self._max_standard_deviation,
                                           measurement.sigma)
        measurement_index += 1

      # Assigns an index for cumulative reaches.
      for edp_combination in metric_reports[
        metric].get_cumulative_edp_combinations():
        for period in range(0, self._num_periods):
          measurement = metric_reports[metric].get_cumulative_measurement(
              edp_combination, period)
          self._measurement_name_to_index[measurement.name] = measurement_index
          self._max_standard_deviation = max(self._max_standard_deviation,
                                             measurement.sigma)
          measurement_index += 1

      # Assign an index for k_reach.
      for edp_combination in metric_reports[
        metric].get_k_reach_edp_combinations():
        for frequency in range(1, self._num_frequencies + 1):
          measurement = metric_reports[metric].get_k_reach_measurement(
              edp_combination, frequency)
          self._measurement_name_to_index[measurement.name] = measurement_index
          self._max_standard_deviation = max(self._max_standard_deviation,
                                             measurement.sigma)
          measurement_index += 1

      # Assigns an index for impressions.
      for edp_combination in metric_reports[
        metric].get_impression_edp_combinations():
        measurement = metric_reports[metric].get_impression_measurement(
            edp_combination)
        self._measurement_name_to_index[measurement.name] = measurement_index
        self._max_standard_deviation = max(self._max_standard_deviation,
                                           measurement.sigma)
        measurement_index += 1

    self._num_vars = measurement_index

  def get_metric_report(self, metric: str) -> "MetricReport":
    return self._metric_reports[metric]

  def get_metrics(self) -> set[str]:
    return set(self._metric_reports.keys())

  def get_corrected_report(self) -> tuple["Report", ReportPostProcessorResult]:
    """Returns a corrected, consistent report."""
    pre_correction_quality = self.get_report_quality()

    spec = self.to_set_measurement_spec()
    solution, report_post_processor_status = Solver(spec).solve_and_translate()

    corrected_report = self.report_from_solution(solution)

    # If a solution is found, get the report quality. Otherwise, return default
    # ReportQuality.
    post_correction_quality = (
        corrected_report.get_report_quality()
        if corrected_report
        else ReportQuality()
    )

    return corrected_report, \
      ReportPostProcessorResult(
          updated_measurements={},
          status=report_post_processor_status,
          pre_correction_quality=pre_correction_quality,
          post_correction_quality=post_correction_quality,
      )

  def report_from_solution(self, solution: Solution) -> Optional["Report"]:
    logging.info("Generating the adjusted report from the solution.")

    if solution:
      return Report(
          metric_reports={
              metric: self._metric_report_from_solution(metric, solution)
              for metric in self._metric_reports
          },
          metric_subsets_by_parent=self._metric_subsets_by_parent,
          cumulative_inconsistency_allowed_edp_combinations=self._cumulative_inconsistency_allowed_edp_combinations,
          population_size=self._population_size
      )
    else:
      return None

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
      for edp_combination in self._metric_reports[
        metric].get_k_reach_edp_combinations():
        for frequency in range(1, self._num_frequencies + 1):
          array.put(
              self._get_measurement_index(
                  self._metric_reports[metric].get_k_reach_measurement(
                      edp_combination, frequency)
              ),
              self._metric_reports[metric].get_k_reach_measurement(
                  edp_combination, frequency).value
          )

      for edp_combination in self._metric_reports[
        metric].get_impression_edp_combinations():
        array.put(
            self._get_measurement_index(
                self._metric_reports[metric].get_impression_measurement(
                    edp_combination)
            ),
            self._metric_reports[metric].get_impression_measurement(
                edp_combination).value
        )
    return array

  def to_set_measurement_spec(self) -> SetMeasurementsSpec:
    spec = SetMeasurementsSpec()
    self._add_measurements_to_spec(spec)
    self._add_set_relations_to_spec(spec)
    return spec

  def _add_cover_relations_to_spec(self, spec: SetMeasurementsSpec):
    # sum of subsets >= union for each period
    for metric in self._metric_reports:
      for cover_relationship in self._metric_reports[
        metric].get_cumulative_cover_relationships():
        logging.debug(
            f"Adding {metric} cover relations for cumulative measurements."
        )
        covered_parent = cover_relationship[0]
        covering_children = cover_relationship[1]
        for period in range(0, self._num_periods):
          spec.add_cover(
              children=list(
                  self._get_cumulative_measurement_index(metric, covering_child,
                                                         period) for
                  covering_child in covering_children),
              parent=self._get_cumulative_measurement_index(metric,
                                                            covered_parent,
                                                            period),
          )
      for cover_relationship in self._metric_reports[
        metric].get_whole_campaign_cover_relationships():
        logging.debug(
            f"Adding {metric} cover relations for total campaign measurements."
        )
        covered_parent = cover_relationship[0]
        covering_children = cover_relationship[1]
        spec.add_cover(
            children=list(self._get_whole_campaign_measurement_index(
                metric, covering_child)
                          for covering_child in covering_children),
            parent=self._get_whole_campaign_measurement_index(
                metric, covered_parent),
        )
    logging.info("Finished adding cover relations to spec.")

  def _add_subset_relations_to_spec(self, spec: SetMeasurementsSpec):
    # Adds relations for cumulative measurements.
    for metric in self._metric_reports:
      metric_report = self._metric_reports[metric]
      for subset_relationship in \
          metric_report.get_cumulative_subset_relationships():
        parent_edp_combination = subset_relationship[0]
        child_edp_combination = subset_relationship[1]
        for period in range(0, self._num_periods):
          spec.add_subset_relation(
              child_set_id=self._get_measurement_index(
                  metric_report.get_cumulative_measurement(
                      child_edp_combination, period
                  )
              ),
              parent_set_id=self._get_measurement_index(
                  metric_report.get_cumulative_measurement(
                      parent_edp_combination, period
                  )
              ),
          )

      # Adds relations for whole campaign measurements.
      for subset_relationship in \
          metric_report.get_whole_campaign_subset_relationships():
        parent_edp_combination = subset_relationship[0]
        child_edp_combination = subset_relationship[1]
        spec.add_subset_relation(
            child_set_id=self._get_measurement_index(
                metric_report.get_whole_campaign_measurement(
                    child_edp_combination
                )
            ),
            parent_set_id=self._get_measurement_index(
                metric_report.get_whole_campaign_measurement(
                    parent_edp_combination
                )
            ),
        )
    logging.info("Finished adding subset relations to spec.")


  def _get_ordered_sets_for_cumulative_measurements_within_metric(self,
      metric_report: MetricReport, edp_combination: FrozenSet[str],
      edps: list[FrozenSet[str]], period: int) -> OrderedSets:
    """Gets ordered sets for cumulative measurements within the same metric.

    Let X_{1, i}, X_{2, i}, ..., X_{k, i} be cumulative measurements at
    period i and X_{1, i + 1}, X_{2, i + 1}, ..., X_{k, i + 1} be
    cumulative measurements at period (i+1). As X_{j, i} is a subset of
    X_{j, i+1} for j in [1, k], we have:
    |X_{1, i}| + ... + |X_{k, i}| - |X_{1, i} U ... U X_{k, i}| <=
    |X_{1, i + 1}| + ... + |X_{k, i + 1}| -
    |X_{1, i + 1} U ... U X_{k, i + 1}|.
    This produces an ordered pair
    (X_{1, i}, ..., X_{k, i}, {X_{1, i + 1} U ... U X_{1, i + 1}}) and
    (X_{1, i + 1}, ..., X_{k, i + 1}, {X_{1, i} U ... U X_{k, i}}).

    Args:
        metric_report: Containing the measurements for a specific metric.
        edp_combination: The set of EDPs used in the measurements.
        edps: A list of all single EDP obtained from the above edp combination.
        period: The time period of the cumulative measurements.

    Returns:
        An OrderedSets instance containing a larger set and a smaller set.
    """
    smaller_set: set[int] = \
      [
          self._get_measurement_index(
              metric_report.get_cumulative_measurement(
                  edp_combination, period + 1
              )
          )
      ] + [
          self._get_measurement_index(
              metric_report.get_cumulative_measurement(
                  edp, period
              )
          ) for edp in edps
      ]

    greater_set: set[int] = \
      [
          self._get_measurement_index(
              metric_report.get_cumulative_measurement(
                  edp_combination, period
              )
          )
      ] + [
          self._get_measurement_index(
              metric_report.get_cumulative_measurement(
                  edp, period + 1
              )
          ) for edp in edps
      ]

    return OrderedSets(set(greater_set), set(smaller_set))

  def _add_overlap_relations_to_spec(self, spec: SetMeasurementsSpec):
    # Overlap constraints within a metric.
    for metric in self._metric_reports:
      metric_report = self._metric_reports[metric]
      cumulative_edp_combinations = \
        metric_report.get_cumulative_edp_combinations()
      for edp_combination in cumulative_edp_combinations:
        if len(edp_combination) <= 1:
          continue
        edps: list[FrozenSet[str]] = get_edps_from_edp_combination(
            edp_combination, cumulative_edp_combinations
        )
        if len(edps) != len(edp_combination):
          logging.info(
              f'Skipping the overlap check for the cumulative measurements of '
              f'{edp_combination} in {metric}. Expecting measurements for each '
              f'EDP in {edp_combination}, however, there are only measurements '
              f'for EDPs in {edps}.'
          )
          continue

        for period in range(0, self._num_periods - 1):
          ordered_sets = \
            self._get_ordered_sets_for_cumulative_measurements_within_metric(
                metric_report, edp_combination, edps, period
            )
          spec.add_ordered_sets_relation(ordered_sets)

  def _add_cumulative_whole_campaign_relations_to_spec(self,
      spec: SetMeasurementsSpec):
    # Adds relations between cumulative and whole campaign measurements.
    # For an edp combination, the last cumulative reach is equal to the whole
    # campaign reach.
    for metric in self._metric_reports:
      metric_report = self._metric_reports[metric]
      common_edp_combinations = \
        metric_report.get_cumulative_edp_combinations().intersection(
            metric_report.get_whole_campaign_edp_combinations())
      for edp_combination in common_edp_combinations:
        spec.add_equal_relation(
            set_id_one=self._get_measurement_index(
                metric_report.get_cumulative_measurement(edp_combination, (
                    self._num_periods - 1))),
            set_id_two=[
                self._get_measurement_index(
                    metric_report.get_whole_campaign_measurement(
                        edp_combination))
            ],
        )
    logging.info(
        "Finished adding the relationship between cumulative and total "
        "campaign measurements to spec."
    )

  def _add_k_reach_whole_campaign_relations_to_spec(self,
      spec: SetMeasurementsSpec):
    for metric in self._metric_reports:
      metric_report = self._metric_reports[metric]
      common_edp_combinations = \
        metric_report.get_whole_campaign_edp_combinations().intersection(
            metric_report.get_k_reach_edp_combinations())
      for edp_combination in common_edp_combinations:
        spec.add_equal_relation(
            set_id_one=self._get_measurement_index(
                metric_report.get_whole_campaign_measurement(edp_combination)),
            set_id_two=[
                self._get_measurement_index(
                    metric_report.get_k_reach_measurement(edp_combination,
                                                          frequency)
                )
                for frequency in range(1, self._num_frequencies + 1)
            ]
        )

  def _add_impression_relations_to_spec(self, spec: SetMeasurementsSpec):
    for metric in self._metric_reports:
      metric_report = self._metric_reports[metric]
      edp_combinations = metric_report.get_impression_edp_combinations()
      for edp_combination in edp_combinations:
        if len(edp_combination) > 1:
          single_edp_subset = [
              comb for comb in edp_combinations
              if len(comb) == 1 and comb.issubset(edp_combination)
          ]
          spec.add_equal_relation(
              set_id_one=self._get_measurement_index(
                  metric_report.get_impression_measurement(edp_combination)),
              set_id_two=[
                  self._get_measurement_index(
                      metric_report.get_impression_measurement(child_edp)
                  )
                  for child_edp in single_edp_subset
              ]
          )

  def _add_whole_campaign_reach_impression_relations_to_spec(self,
      spec: SetMeasurementsSpec):
    for metric in self._metric_reports:
      metric_report = self._metric_reports[metric]
      common_edp_combinations = \
        metric_report.get_whole_campaign_edp_combinations().intersection(
            metric_report.get_impression_edp_combinations())
      for edp_combination in common_edp_combinations:
        spec.add_subset_relation(
            child_set_id=self._get_measurement_index(
                metric_report.get_whole_campaign_measurement(edp_combination)),
            parent_set_id=self._get_measurement_index(
                metric_report.get_impression_measurement(edp_combination)),
        )

  def _add_k_reach_impression_relations_to_spec(self,
      spec: SetMeasurementsSpec):
    for metric in self._metric_reports:
      metric_report = self._metric_reports[metric]
      common_edp_combinations = \
        metric_report.get_k_reach_edp_combinations().intersection(
            metric_report.get_impression_edp_combinations())
      for edp_combination in common_edp_combinations:
        spec.add_weighted_sum_upperbound_relation(
            weighted_id_set=[
                [
                    self._get_measurement_index(
                        metric_report.get_k_reach_measurement(edp_combination,
                                                              frequency)),
                    frequency
                ]
                for frequency in range(1, self._num_frequencies + 1)
            ],
            upperbound_id=self._get_measurement_index(
                metric_report.get_impression_measurement(edp_combination))
        )

  def _get_ordered_sets_for_cumulative_measurements_across_metric(self,
      parent_metric_report: MetricReport, child_metric_report: MetricReport,
      edp_combination: FrozenSet[str], edps: list[FrozenSet[str]],
      period: int) -> OrderedSets:
    """Gets ordered sets for cumulative measurements across metric.

    Let X_{1, i}, ..., X_{k, i} be the child cumulative measurements and
    Y_{1, i}, ..., Y_{k, i} be the parent cumulative measurements at the i-th
    period. As X_{j, i} is a subset of Y_{j, i} for j in [1, k], we have:
    |X_{1, i}| + ... + |X_{k, i}| - |X_{1, i} U ... U X_{k, i}| <=
    |Y_{1, i}| + ... + |Y_{k, i}| - |Y_{1, i} U ... U Y_{k, i}|.
    This produces an ordered pair:
    (X_{1, i}, ..., X_{k, i}, {Y_{1, i} U ... U Y_{k, i}) and
    (Y_{1, i}, ..., Y_{k, i}, {X_{1, i} U ... U X_{k, i}).

    Args:
        parent_metric_report: Containing the measurements for parent metric.
        child_metric_report: Containing the measurements for child metric.
        edp_combination: The set of EDPs used in the measurements.
        edps: A list of all single EDP obtained from the above edp combination.
        period: The time period of the cumulative measurements.

    Returns:
        An OrderedSets instance containing a larger set and a smaller set.
    """

    smaller_set: set[int] = \
      [
          self._get_measurement_index(
              parent_metric_report.get_cumulative_measurement(
                  edp_combination, period
              )
          )
      ] + [
          self._get_measurement_index(
              child_metric_report.get_cumulative_measurement(
                  edp, period
              )
          ) for edp in edps
      ]

    greater_set: set[int] = \
      [
          self._get_measurement_index(
              child_metric_report.get_cumulative_measurement(
                  edp_combination, period
              )
          )
      ] + [
          self._get_measurement_index(
              parent_metric_report.get_cumulative_measurement(
                  edp, period
              )
          ) for edp in edps
      ]

    return OrderedSets(set(greater_set), set(smaller_set))

  def _get_ordered_sets_for_whole_campaign_measurements_across_metric(self,
      parent_metric_report: MetricReport, child_metric_report: MetricReport,
      edp_combination: FrozenSet[str],
      edps: list[FrozenSet[str]]) -> OrderedSets:
    """Gets ordered sets for whole campaign measurements across metric.

    Let X_1, ..., X_k be the child total campaign measurements and Y_1, ..., Y_k
    be the parent total campaign measurements at the i-th period. As X_j is a
    subset of Y_j for j in [1, k], we have:
    |X_1| + ... + |X_k| - |X_1 U ... U X_k| <=
    |Y_1| + ... + |Y_k| - |Y_1 U ... U Y_k|.
    This produces an ordered pair:
    (X_1, ..., X_k, {Y_1 U ... U Y_k}) and (Y_1, ..., Y_k, {X_1 U ... U X_k}).

    Args:
        parent_metric_report: Containing the measurements for parent metric.
        child_metric_report: Containing the measurements for child metric.
        edp_combination: The set of EDPs used in the measurements.
        edps: A list of all single EDP obtained from the above edp combination.

    Returns:
        An OrderedSets instance containing a larger set and a smaller set.
    """
    smaller_set: list[int] = \
      [
          self._get_measurement_index(
              parent_metric_report.get_whole_campaign_measurement(
                  edp_combination
              )
          )
      ] + [
          self._get_measurement_index(
              child_metric_report.get_whole_campaign_measurement(edp)
          ) for edp in edps
      ]

    greater_set: list[int] = \
      [
          self._get_measurement_index(
              child_metric_report.get_whole_campaign_measurement(
                  edp_combination
              )
          )
      ] + [
          self._get_measurement_index(
              parent_metric_report.get_whole_campaign_measurement(edp)
          )
          for edp in edps
      ]

    return OrderedSets(set(greater_set), set(smaller_set))

  def _add_metric_relations_to_spec(self, spec: SetMeasurementsSpec):
    # metric1>=metric#2
    for parent_metric in self._metric_subsets_by_parent:
      for child_metric in self._metric_subsets_by_parent[parent_metric]:
        logging.debug(
            f"Adding metric relationship for {child_metric} and "
            f"{parent_metric}."
        )

        parent_metric_report = self._metric_reports[parent_metric]
        child_metric_report = self._metric_reports[child_metric]

        common_cumulative_edp_combinations = \
          parent_metric_report.get_cumulative_edp_combinations().intersection(
              child_metric_report.get_cumulative_edp_combinations())
        # Handles subset relations for cumulative measurements.
        for edp_combination in common_cumulative_edp_combinations:
          for period in range(0, self._num_periods):
            spec.add_subset_relation(
                child_set_id=self._get_measurement_index(
                    child_metric_report.get_cumulative_measurement(
                        edp_combination, period)),
                parent_set_id=self._get_measurement_index(
                    parent_metric_report.get_cumulative_measurement(
                        edp_combination, period)),
            )
        # Handles overlap relations for cumulative measurements.
        for edp_combination in common_cumulative_edp_combinations:
          if len(edp_combination) <= 1:
            continue
          edps: list[FrozenSet[str]] = get_edps_from_edp_combination(
              edp_combination, common_cumulative_edp_combinations
          )
          if len(edps) != len(edp_combination):
            logging.info(
                f'Skipping the overlap check for the cumulative measurements of '
                f'{edp_combination} across {parent_metric}/{child_metric}. '
                f'Expecting measurements for each EDP in {edp_combination}, '
                f'however, there are only measurements for EDPs in {edps}.'
            )
            continue

          for period in range(0, self._num_periods):
            ordered_sets = \
              self._get_ordered_sets_for_cumulative_measurements_across_metric(
                  parent_metric_report, child_metric_report, edp_combination,
                  edps, period
              )
            spec.add_ordered_sets_relation(ordered_sets)

        common_whole_campaign_edp_combinations = \
          parent_metric_report.get_whole_campaign_edp_combinations().intersection(
              child_metric_report.get_whole_campaign_edp_combinations())
        # Handles subset relations for whole campaign  measurements.
        for edp_combination in common_whole_campaign_edp_combinations:
          spec.add_subset_relation(
              child_set_id=self._get_measurement_index(
                  child_metric_report.get_whole_campaign_measurement(
                      edp_combination)),
              parent_set_id=self._get_measurement_index(
                  parent_metric_report.get_whole_campaign_measurement(
                      edp_combination)),
          )

        # Handles overlap relations for whole campaign measurements.
        for edp_combination in common_whole_campaign_edp_combinations:
          if len(edp_combination) <= 1:
            continue

          edps: list[FrozenSet[str]] = get_edps_from_edp_combination(
              edp_combination, common_whole_campaign_edp_combinations
          )
          if len(edps) != len(edp_combination):
            logging.info(
                f'Skipping the overlap check for the whole campaign measurement'
                f' of {edp_combination} across {parent_metric}/{child_metric}. '
                f'Expecting measurements for each EDP in {edp_combination}, '
                f'however, there are only measurements for EDPs in {edps}.'
            )
            continue

          ordered_sets = \
            self._get_ordered_sets_for_whole_campaign_measurements_across_metric(
                parent_metric_report, child_metric_report, edp_combination, edps
            )
          spec.add_ordered_sets_relation(ordered_sets)

        # Handles impression measurements of common edp combinations.
        common_impression_edp_combinations = \
          parent_metric_report.get_impression_edp_combinations().intersection(
              child_metric_report.get_impression_edp_combinations())
        for edp_combination in common_impression_edp_combinations:
          spec.add_subset_relation(
              child_set_id=self._get_measurement_index(
                  child_metric_report.get_impression_measurement(
                      edp_combination)),
              parent_set_id=self._get_measurement_index(
                  parent_metric_report.get_impression_measurement(
                      edp_combination)),
          )

  logging.info(
      "Finished adding the relationship for measurements from different "
      "metrics."
  )

  def _add_cumulative_relations_to_spec(self, spec: SetMeasurementsSpec):
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
    logging.info("Finished adding cumulative relations to spec.")

  def _add_set_relations_to_spec(self, spec: SetMeasurementsSpec):
    # sum of subsets >= union for each period.
    self._add_cover_relations_to_spec(spec)

    # subset <= union.
    self._add_subset_relations_to_spec(spec)

    self._add_overlap_relations_to_spec(spec)

    # period1 <= period2.
    self._add_cumulative_relations_to_spec(spec)

    self._add_k_reach_whole_campaign_relations_to_spec(spec)

    self._add_impression_relations_to_spec(spec)

    self._add_whole_campaign_reach_impression_relations_to_spec(spec)

    self._add_k_reach_impression_relations_to_spec(spec)

    # Last cumulative measurement <= whole campaign measurement.
    self._add_cumulative_whole_campaign_relations_to_spec(spec)

    # metric#1>=metric#2.
    self._add_metric_relations_to_spec(spec)

    logging.info("Finished adding set relations to spec.")

  def _add_measurements_to_spec(self, spec: SetMeasurementsSpec):
    for metric in self._metric_reports.keys():
      for edp_combination in self._metric_reports[
        metric].get_cumulative_edp_combinations():
        for period in range(self._num_periods):
          measurement = self._metric_reports[
            metric].get_cumulative_measurement(edp_combination, period)
          spec.add_measurement(
              self._get_measurement_index(measurement),
              Measurement(measurement.value,
                          self._normalized_sigma(measurement.sigma),
                          measurement.name),
          )
      for edp_combination in self._metric_reports[
        metric].get_whole_campaign_edp_combinations():
        measurement = self._metric_reports[
          metric].get_whole_campaign_measurement(edp_combination)
        spec.add_measurement(
            self._get_measurement_index(measurement),
            Measurement(measurement.value,
                        self._normalized_sigma(measurement.sigma),
                        measurement.name),
        )
      for edp_combination in self._metric_reports[
        metric].get_k_reach_edp_combinations():
        for frequency in range(1, self._num_frequencies + 1):
          measurement = self._metric_reports[metric].get_k_reach_measurement(
              edp_combination, frequency)
          spec.add_measurement(
              self._get_measurement_index(measurement),
              Measurement(measurement.value,
                          self._normalized_sigma(measurement.sigma),
                          measurement.name),
          )
      for edp_combination in self._metric_reports[
        metric].get_impression_edp_combinations():
        measurement = self._metric_reports[
          metric].get_impression_measurement(edp_combination)
        spec.add_measurement(
            self._get_measurement_index(measurement),
            Measurement(measurement.value,
                        self._normalized_sigma(measurement.sigma),
                        measurement.name),
        )
      logging.info(
          "Finished adding the measurements to the set measurement spec.")

  def _normalized_sigma(self, sigma: float) -> float:
    """Normalizes the standard deviation.

    Args:
      sigma: The standard deviation to normalize.

    Returns:
      The normalized standard deviation, capped at
      MIN_STANDARD_VARIATION_RATIO.
    """

    # Zero value for sigma means that this measurement will not be corrected,
    # thus the normalized value of zero is not capped at
    # MIN_STANDARD_VARIATION_RATIO.
    if not sigma:
      return 0.0

    normalized_sigma = sigma / self._max_standard_deviation
    return max(normalized_sigma, MIN_STANDARD_VARIATION_RATIO)

  def _get_measurement_index(self, measurement: Measurement) -> int:
    return self._measurement_name_to_index[measurement.name]

  def _get_cumulative_measurement_index(self, metric: str,
      edp_combination: FrozenSet[str], period: int) -> int:
    return self._get_measurement_index(
        self._metric_reports[metric].get_cumulative_measurement(
            edp_combination, period)
    )

  def _get_whole_campaign_measurement_index(self, metric: str,
      edp_combination: FrozenSet[str]) -> int:
    return self._get_measurement_index(
        self._metric_reports[metric].get_whole_campaign_measurement(
            edp_combination)
    )

  def _metric_report_from_solution(self, metric: str,
      solution: Solution) -> "MetricReport":
    logging.debug(f"Generating the metric report for {metric}.")
    solution_time_series = {}
    solution_whole_campaign = {}
    solution_k_reach = {}
    solution_impression = {}

    metric_report = self._metric_reports[metric]
    for edp_combination in metric_report.get_cumulative_edp_combinations():
      solution_time_series[edp_combination] = [
          Measurement(
              solution[
                self._get_measurement_index(
                    metric_report.get_cumulative_measurement(
                        edp_combination, period))
              ],
              metric_report.get_cumulative_measurement(
                  edp_combination, period).sigma,
              metric_report.get_cumulative_measurement(
                  edp_combination, period).name,
          )
          for period in range(0, self._num_periods)
      ]
    for edp_combination in metric_report.get_whole_campaign_edp_combinations():
      solution_whole_campaign[edp_combination] = Measurement(
          solution[
            self._get_measurement_index(
                metric_report.get_whole_campaign_measurement(edp_combination)
            )
          ],
          metric_report.get_whole_campaign_measurement(edp_combination).sigma,
          metric_report.get_whole_campaign_measurement(edp_combination).name,
      )
    for edp_combination in metric_report.get_k_reach_edp_combinations():
      solution_k_reach[edp_combination] = {
          frequency: Measurement(
              solution[
                self._get_measurement_index(
                    metric_report.get_k_reach_measurement(edp_combination,
                                                          frequency))
              ],
              metric_report.get_k_reach_measurement(edp_combination,
                                                    frequency).sigma,
              metric_report.get_k_reach_measurement(edp_combination,
                                                    frequency).name
          )
          for frequency in range(1, self._num_frequencies + 1)
      }

    for edp_combination in metric_report.get_impression_edp_combinations():
      solution_impression[edp_combination] = Measurement(
          solution[
            self._get_measurement_index(
                metric_report.get_impression_measurement(
                    edp_combination))
          ],
          metric_report.get_impression_measurement(edp_combination).sigma,
          metric_report.get_impression_measurement(edp_combination).name,
      )
    return MetricReport(
        reach_time_series=solution_time_series,
        reach_whole_campaign=solution_whole_campaign,
        k_reach=solution_k_reach,
        impression=solution_impression,
    )

  def _is_zero_variance_edp(self, edp_combination: FrozenSet[str]) -> bool:
    """Checks if a given EDP combination represents a zero variance EDP.

    An EDP is a zero variance EDP if the standard deviation is zero for all
    measurements.

    Args:
        edp_combination: A frozenset of strings representing the EDP identifiers
                         to check.

    Returns:
        True or False.
    """
    standard_deviations: list[float] = []
    for metric in self._metric_reports.keys():
      metric_report = self._metric_reports[metric]
      if edp_combination in metric_report.get_cumulative_edp_combinations():
        standard_deviations.extend(
            [
                measurement.sigma
                for measurement in
                metric_report.get_cumulative_measurements(edp_combination)
            ]
        )
      if edp_combination in metric_report.get_whole_campaign_edp_combinations():
        standard_deviations.append(
            metric_report.get_whole_campaign_measurement(edp_combination).sigma
        )
      if edp_combination in metric_report.get_impression_edp_combinations():
        standard_deviations.append(
            metric_report.get_impression_measurement(edp_combination).sigma
        )
      if edp_combination in metric_report.get_k_reach_edp_combinations():
        standard_deviations.extend(
            [
                measurement.sigma
                for measurement in
                metric_report.get_k_reach_measurements(edp_combination)
            ]
        )

    return all(val == 0.0 for val in standard_deviations)

  def _are_edp_measurements_consistent(self,
      edp_combination: FrozenSet[str]) -> bool:
    """Checks if all measurements of a specific edp combination are consistent."""
    # Check for the consistency within a metric.
    for metric in self._metric_reports.keys():
      metric_report = self._metric_reports[metric]

      cumulative_measurements = [
          metric_report.get_cumulative_measurement(
              edp_combination,
              period
          ).value
          for period in range(0, self._num_periods)
      ]
      whole_campaign_measurement = metric_report.get_whole_campaign_measurement(
          edp_combination).value
      impression_measurement = metric_report.get_impression_measurement(
          edp_combination).value
      k_reach_measurements = {
          frequency: metric_report.get_k_reach_measurement(
              edp_combination,
              frequency
          ).value
          for frequency in range(1, self._num_frequencies + 1)
      }

      # Cumulative measurements are non-decreasing.
      for period in range(0, self._num_periods - 1):
        if not fuzzy_less_equal(cumulative_measurements[period],
                                cumulative_measurements[period + 1],
                                CONSISTENCY_TEST_TOLERANCE):
          return False

      # Whole campaign reach matches last cumulative reach.
      if not fuzzy_equal(cumulative_measurements[-1],
                         whole_campaign_measurement,
                         CONSISTENCY_TEST_TOLERANCE):
        return False

      sum_of_k_reach_keys = sum(k_reach_measurements.keys())
      sum_of_k_reach_values = sum(k_reach_measurements.values())
      weighted_sum_of_k_reach_values = sum(
          key * value for key, value in k_reach_measurements.items())

      # Whole campaign reach equals to the sum of k-reaches.
      if not fuzzy_equal(
          whole_campaign_measurement,
          sum_of_k_reach_values,
          sum_of_k_reach_keys * CONSISTENCY_TEST_TOLERANCE):
        return False

      # Impression is greater than or equal to weighted sum of k-reaches.
      if not fuzzy_less_equal(
          weighted_sum_of_k_reach_values,
          impression_measurement,
          sum_of_k_reach_keys * CONSISTENCY_TEST_TOLERANCE):
        return False

    # Check for the consistency between ordered metrics.
    for parent_metric in self._metric_subsets_by_parent:
      for child_metric in self._metric_subsets_by_parent[parent_metric]:
        parent_metric_report = self._metric_reports[parent_metric]
        child_metric_report = self._metric_reports[child_metric]

        # Handles cumulative measurements.
        cumulative_edp_combinations = \
          parent_metric_report.get_cumulative_edp_combinations()
        for edp_combination in cumulative_edp_combinations:
          for period in range(0, self._num_periods):
            if not fuzzy_less_equal(
                child_metric_report.get_cumulative_measurement(edp_combination,
                                                               period).value,
                parent_metric_report.get_cumulative_measurement(edp_combination,
                                                                period).value,
                CONSISTENCY_TEST_TOLERANCE):
              return False

        # Handles whole campaign measurements.
        whole_campaign_edp_combinations = \
          parent_metric_report.get_whole_campaign_edp_combinations()
        for edp_combination in whole_campaign_edp_combinations:
          if not fuzzy_less_equal(
              child_metric_report.get_whole_campaign_measurement(
                  edp_combination).value,
              parent_metric_report.get_whole_campaign_measurement(
                  edp_combination).value,
              CONSISTENCY_TEST_TOLERANCE):
            return False

        # Handles impression measurements.
        impression_edp_combinations = \
          parent_metric_report.get_impression_edp_combinations()
        for edp_combination in impression_edp_combinations:
          if not fuzzy_less_equal(
              child_metric_report.get_impression_measurement(
                  edp_combination).value,
              parent_metric_report.get_impression_measurement(
                  edp_combination).value,
              CONSISTENCY_TEST_TOLERANCE):
            return False

    return True

  def _validate_independence_checks(self,
      edp_combination: FrozenSet[str]) -> bool:
    """Verifies if the observed union measurements for the edp_combination meet
    the independence check.
    """
    edps = [frozenset({element}) for element in edp_combination]

    for metric in self._metric_reports.keys():
      metric_report = self._metric_reports[metric]

      # Check if the whole campaign measurements follow the CI model.
      union_measurement = metric_report.get_whole_campaign_measurement(
          edp_combination)
      component_measurements = [
          metric_report.get_whole_campaign_measurement(single_edp)
          for single_edp in edps
      ]

      if not is_union_reach_consistent(union_measurement,
                                       component_measurements,
                                       self._population_size):
        return False

      # Check if the cumulative measurements follow the CI model.
      for period in range(self._num_periods):
        union_measurement = metric_report.get_cumulative_measurement(
            edp_combination, period)
        component_measurements = [
            metric_report.get_cumulative_measurement(single_edp, period)
            for single_edp in edps
        ]

        if not is_union_reach_consistent(union_measurement,
                                         component_measurements,
                                         self._population_size):
          return False

    return True

  def _get_zero_variace_edps(self) -> list[str]:
    """Get the zero variance EDPs.

    Returns:
       A list of zero variance EDPs.
    """
    zero_variance_edp_combinations: list[str] = []
    if self._metric_reports.keys() is None:
      raise ValueError("The report does not contain any measurements.")

    edp_combinations = self._metric_reports[
      next(iter(self._metric_reports.keys()))].get_cumulative_edp_combinations()

    for edp_combination in edp_combinations:
      if self._is_zero_variance_edp(edp_combination):
        zero_variance_edp_combinations.append(edp_combination)

    return zero_variance_edp_combinations

  def get_report_quality(self) -> ReportQuality:
    """Calculates and returns the overall data quality status for the report.

    Returns:
        A ReportQuality protobuf message instance populated with the status
        outcomes (`zero_variance_measurements_status`, `union_status`) from the performed checks.
    """
    # Gets the zero variance measurement quality status.
    zero_variance_edp_combinations = self._get_zero_variace_edps()
    if not zero_variance_edp_combinations:
      zero_variance_measurements_status = \
        ReportQuality \
          .ZeroVarianceMeasurementsStatus \
          .ZERO_VARIANCE_MEASUREMENTS_STATUS_UNSPECIFIED
    else:
      zero_variance_measurements_status = \
        ReportQuality \
          .ZeroVarianceMeasurementsStatus \
          .CONSISTENT
      for zero_variance_edp_combination in zero_variance_edp_combinations:
        if not self._are_edp_measurements_consistent(
            zero_variance_edp_combination):
          zero_variance_measurements_status = \
            ReportQuality.ZeroVarianceMeasurementsStatus.INCONSISTENT
          break

    # Gets the union check status.
    independence_check_status = \
      ReportQuality \
        .IndependenceCheckStatus \
        .INDEPENDENCE_CHECK_STATUS_UNSPECIFIED
    if self._population_size > 0:
      independence_check_status = \
        ReportQuality.IndependenceCheckStatus.WITHIN_CONFIDENCE_RANGE
      edp_combinations = self._metric_reports[
        next(iter(self._metric_reports.keys()))
      ].get_whole_campaign_edp_combinations()
      for edp_combination in edp_combinations:
        if len(edp_combination) > 1:
          if not self._validate_independence_checks(edp_combination):
            independence_check_status = \
              ReportQuality.IndependenceCheckStatus.OUTSIDE_CONFIDENCE_RANGE
            break

    return ReportQuality(
        zero_variance_measurements_status=zero_variance_measurements_status,
        union_status=independence_check_status,
    )
