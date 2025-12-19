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
from typing import TypeAlias

from absl import logging
from qpsolvers import Solution

from noiseninja.noised_measurements import KReachMeasurements
from noiseninja.noised_measurements import Measurement
from noiseninja.noised_measurements import MeasurementSet
from noiseninja.noised_measurements import OrderedSets
from noiseninja.noised_measurements import SetMeasurementsSpec
from noiseninja.solver import Solver
from wfa.measurement.internal.reporting.postprocessing import report_post_processor_result_pb2

ReportPostProcessorResult = report_post_processor_result_pb2.ReportPostProcessorResult
ReportPostProcessorStatus = report_post_processor_result_pb2.ReportPostProcessorStatus
ReportQuality = report_post_processor_result_pb2.ReportQuality

EdpCombination: TypeAlias = FrozenSet[str]

MIN_STANDARD_VARIATION_RATIO = 0.001
UNIT_SCALING_FACTOR = 1.0
TOLERANCE = 1e-6

# The probability of a value falling outside the [-7*STDDEV; 7*STDDEV] range is
# approximately 2^{-38.5}.
STANDARD_DEVIATION_TEST_THRESHOLD = 7.0

CONSISTENCY_TEST_TOLERANCE = 1.0

LARGE_CORRECTION_THRESHOLD = 1.0


def fuzzy_equal(val: float, target: float, tolerance: float) -> bool:
  """Checks if two float values are approximately equal within an absolute tolerance."""
  return math.isclose(val, target, rel_tol=0.0, abs_tol=tolerance)


def fuzzy_less_equal(smaller: float, larger: float, tolerance: float) -> bool:
  """Checks if one float value is less than or equal to another within a tolerance."""
  return larger - smaller + tolerance >= 0


def get_subset_relationships(
    edp_combinations: list[EdpCombination],
) -> list[Tuple[EdpCombination, EdpCombination]]:
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


def is_cover(
    target_set: EdpCombination, possible_cover: list[EdpCombination]
) -> bool:
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


def get_covers(
    target_set: EdpCombination, other_sets: list[EdpCombination]
) -> list[Tuple[EdpCombination, list[EdpCombination]]]:
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


def get_cover_relationships(
    edp_combinations: list[EdpCombination],
) -> list[Tuple[EdpCombination, list[EdpCombination]]]:
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


def get_edps_from_edp_combination(
    edp_combination: EdpCombination,
    all_edp_combinations: set[EdpCombination]
) -> list[EdpCombination]:
  return list(
    all_edp_combinations.intersection([frozenset({edp}) for edp in edp_combination])
  )


def build_measurement_set(
    reach: dict[EdpCombination, Measurement],
    k_reach: dict[EdpCombination, KReachMeasurements],
    impression: dict[EdpCombination, Measurement]
) -> dict[EdpCombination, MeasurementSet]:
  """Builds a dictionary of MeasurementSet from separate measurement dicts."""
  all_edps = (
      set(reach.keys())
      | set(k_reach.keys())
      | set(impression.keys())
  )
  whole_campaign_measurements = {}
  for edp in all_edps:
    whole_campaign_measurements[edp] = MeasurementSet(
        reach=reach.get(edp),
        k_reach=k_reach.get(edp, {}),
        impression=impression.get(edp),
    )
  return whole_campaign_measurements


class MetricReport:
  """Represents a metric sub-report view (e.g., MRC, AMI) within a report.

    This class stores and provides access to various measurements for different
    EDP (Event, Data Provider, and Platform) combinations. It holds three main
    types of data:

        * Cumulative reach over time, represented as a time series.
        * A set of measurements (reach, k-reach, impression) for the whole
          campaign.
        * A time series of weekly non-cumulative measurements for each period.

    Attributes:
        _weekly_cumulative_reaches: A dictionary mapping EDP combinations (represented
                            as frozensets of strings) to lists of Measurement
                            objects, where each list represents a time series of
                            reach values.
        _whole_campaign_measurements: A dictionary mapping EDP combinations to
                                      MeasurementSet objects, each containing
                                      the reach, k-reach, and impression for
                                      the entire campaign.
        _weekly_non_cumulative_measurements: A dictionary mapping EDP combinations to
                                      lists of MeasurementSet objects, where
                                      each set represents the non-cumulative
                                      measurements for a specific time period.
  """

  def __init__(
      self,
      weekly_cumulative_reaches: dict[EdpCombination, list[Measurement]],
      whole_campaign_measurements: dict[EdpCombination, MeasurementSet],
      weekly_non_cumulative_measurements: dict[
          EdpCombination, list[MeasurementSet]
      ],
  ):
    # Get the number of periods and check that all time series have the same
    # length.
    periods = set()
    for edp_combination in weekly_cumulative_reaches.keys():
      periods.add(len(weekly_cumulative_reaches[edp_combination]))
    for edp_combination in weekly_non_cumulative_measurements.keys():
      periods.add(len(weekly_non_cumulative_measurements[edp_combination]))

    if len(periods) > 1:
      raise ValueError("All weekly measurements must have the same number of periods.")

    self._num_periods = periods.pop() if len(periods) == 1 else 0

    frequencies = set()
    for edp_combination in whole_campaign_measurements.keys():
      k_reach_measurements = whole_campaign_measurements[edp_combination].k_reach
      if k_reach_measurements:
        frequencies.add(len(k_reach_measurements))

    for edp_combination in weekly_non_cumulative_measurements.keys():
      for period in range(0, self._num_periods):
        k_reach_measurements = weekly_non_cumulative_measurements[edp_combination][period].k_reach
        if k_reach_measurements:
          frequencies.add(len(k_reach_measurements))

    if len(frequencies) > 1:
      raise ValueError("All k-reach measurements must have the same number of frequencies.")

    self._num_frequencies = frequencies.pop() if len(frequencies) == 1 else 0

    self._weekly_cumulative_reaches = weekly_cumulative_reaches
    self._whole_campaign_measurements = whole_campaign_measurements
    self._weekly_non_cumulative_measurements = weekly_non_cumulative_measurements

  def sample_with_noise(self) -> "MetricReport":
    """
    :return: a new MetricReport where measurements have been resampled
    according to their mean and variance.
    """
    return MetricReport(
        weekly_cumulative_reaches={
            edp_combination: [
                MetricReport._sample_with_noise(measurement)
                for measurement in self._weekly_cumulative_reaches[
                  edp_combination
                ]
            ]
            for edp_combination in
            self._weekly_cumulative_reaches.keys()
        }
    )

  def get_num_periods(self) -> int:
    return self._num_periods

  def get_num_frequencies(self) -> int:
    return self._num_frequencies

  def get_weekly_cumulative_reach_measurements(
      self, edp_combination: EdpCombination
  ) -> list[Measurement]:
    if edp_combination not in self._weekly_cumulative_reaches:
      return None

    return self._weekly_cumulative_reaches[edp_combination]

  def get_weekly_cumulative_reach_measurement(
      self, edp_combination: EdpCombination, period: int
  ) -> Measurement:
    if edp_combination not in self._weekly_cumulative_reaches:
      return None

    if period >= self._num_periods:
      return None

    return self._weekly_cumulative_reaches[edp_combination][period]

  def get_weekly_non_cumulative_reach_measurement(
      self, edp_combination: EdpCombination, period: int
  ) -> Measurement:
    if edp_combination not in self._weekly_non_cumulative_measurements:
      return None

    if period >= len(self._weekly_non_cumulative_measurements[edp_combination]):
      return None

    if self._weekly_non_cumulative_measurements[edp_combination][period].reach is None:
      return None

    return self._weekly_non_cumulative_measurements[edp_combination][period].reach

  def get_weekly_non_cumulative_impression_measurement(
      self, edp_combination: EdpCombination, period: int
  ) -> Measurement:
    if edp_combination not in self._weekly_non_cumulative_measurements:
      return None

    if period >= len(self._weekly_non_cumulative_measurements[edp_combination]):
      return None

    if self._weekly_non_cumulative_measurements[edp_combination][period].impression is None:
      return None

    return self._weekly_non_cumulative_measurements[edp_combination][period].impression

  def get_weekly_non_cumulative_k_reach_measurements(
      self, edp_combination: EdpCombination, period: int
  ) -> list[Measurement]:
    if edp_combination not in self._weekly_non_cumulative_measurements:
      return None

    if period >= len(self._weekly_non_cumulative_measurements[edp_combination]):
      return None

    return self._weekly_non_cumulative_measurements[edp_combination][period].k_reach.values()

  def get_weekly_non_cumulative_k_reach_measurement(
      self, edp_combination: EdpCombination, period: int, frequency: int
  ) -> Measurement:
    k_reach_measurements = self.get_weekly_non_cumulative_k_reach_measurements(
        edp_combination, period)

    if k_reach_measurements is None:
      return None

    return self._weekly_non_cumulative_measurements[edp_combination][period].k_reach[frequency]

  def get_whole_campaign_reach_measurement(
      self, edp_combination: EdpCombination
  ) -> Measurement:
    if edp_combination not in self._whole_campaign_measurements:
      return None

    return self._whole_campaign_measurements[edp_combination].reach

  def get_whole_campaign_impression_measurement(
      self, edp_combination: EdpCombination
  ) -> Measurement:
    if edp_combination not in self._whole_campaign_measurements:
      return None

    return self._whole_campaign_measurements[edp_combination].impression

  def get_whole_campaign_k_reach_measurements(
      self, edp_combination: EdpCombination
  ) -> list[Measurement]:
    if edp_combination not in self._whole_campaign_measurements:
      return None

    if self._whole_campaign_measurements[edp_combination].k_reach is None:
      return None

    return list(self._whole_campaign_measurements[edp_combination].k_reach.values())

  def get_whole_campaign_k_reach_measurement(
      self, edp_combination: EdpCombination, frequency: int
  ) -> Measurement:
    if edp_combination not in self._whole_campaign_measurements:
      return None

    if self._whole_campaign_measurements[edp_combination].k_reach is None:
      return None

    if frequency not in self._whole_campaign_measurements[edp_combination].k_reach:
      return None

    return self._whole_campaign_measurements[edp_combination].k_reach[frequency]

  def get_weekly_cumulative_reach_edp_combinations(self) -> set[EdpCombination]:
    return set(self._weekly_cumulative_reaches.keys())

  def get_weekly_non_cumulative_reach_edp_combinations(self) -> set[EdpCombination]:
    return {
        edp
        for edp in self._weekly_non_cumulative_measurements.keys()
        if len(self._weekly_non_cumulative_measurements[edp]) > 0 and
          self._weekly_non_cumulative_measurements[edp][0].reach is not None
    }

  def get_weekly_non_cumulative_k_reach_edp_combinations(self) -> set[EdpCombination]:
    return {
        edp
        for edp in self._weekly_non_cumulative_measurements.keys()
        if len(self._weekly_non_cumulative_measurements[edp]) > 0 and
          self._weekly_non_cumulative_measurements[edp][0].k_reach
    }

  def get_weekly_non_cumulative_impression_edp_combinations(self) -> set[EdpCombination]:
    return {
        edp
        for edp in self._weekly_non_cumulative_measurements.keys()
        if len(self._weekly_non_cumulative_measurements[edp]) > 0 and
          self._weekly_non_cumulative_measurements[edp][0].impression is not None
    }

  def get_whole_campaign_reach_edp_combinations(self) -> set[EdpCombination]:
    return {
        edp
        for edp, measurement_set in self._whole_campaign_measurements.items()
        if measurement_set.reach is not None
    }
    
  def get_whole_campaign_impression_edp_combinations(self) -> set[EdpCombination]:
    return {
        edp
        for edp, measurement_set in self._whole_campaign_measurements.items()
        if measurement_set.impression is not None
    }

  def get_whole_campaign_k_reach_edp_combinations(self) -> set[EdpCombination]:
    return {
        edp
        for edp, measurement_set in self._whole_campaign_measurements.items()
        if len(measurement_set.k_reach) > 0
    }

  def get_weekly_cumulative_reach_edp_combinations_count(self) -> int:
    return len(self._weekly_cumulative_reaches.keys())

  def get_whole_campaign_reach_edp_combinations_count(self) -> int:
    return len(self.get_whole_campaign_reach_edp_combinations())

  def get_number_of_periods(self) -> int:
    return len(next(iter(self._weekly_cumulative_reaches.values()))) \
      if self._weekly_cumulative_reaches else 0

  def get_number_of_frequencies(self) -> int:
    k_reach_lengths = {
        len(measurement_set.k_reach)
        for measurement_set in self._whole_campaign_measurements.values()
        if measurement_set.k_reach
    }

    if len(k_reach_lengths) > 0:
      return next(iter(k_reach_lengths))
    else:
      return 0

  def get_cumulative_subset_relationships(self) -> list[
    Tuple[EdpCombination, EdpCombination]]:
    return get_subset_relationships(list(self._weekly_cumulative_reaches))

  def get_whole_campaign_reach_subset_relationships(self) -> list[
    Tuple[EdpCombination, EdpCombination]]:
    return get_subset_relationships(
        list(self.get_whole_campaign_reach_edp_combinations()))

  def get_weekly_non_cumulative_reach_subset_relationships(self) -> list[
    Tuple[EdpCombination, EdpCombination]]:
    return get_subset_relationships(
        list(self.get_weekly_non_cumulative_reach_edp_combinations()))

  def get_cumulative_cover_relationships(self) -> list[
    Tuple[EdpCombination, list[EdpCombination]]]:
    return get_cover_relationships(list(self._weekly_cumulative_reaches))

  def get_whole_campaign_reach_cover_relationships(
      self,
  ) -> list[Tuple[EdpCombination, list[EdpCombination]]]:
    return get_cover_relationships(
        list(self.get_whole_campaign_reach_edp_combinations()))

  def get_weekly_non_cumulative_reach_cover_relationships(self) -> list[
     Tuple[EdpCombination, list[EdpCombination]]]:
     return get_cover_relationships(list(self.get_weekly_non_cumulative_reach_edp_combinations()))

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
    if metric_subsets_by_parent:
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

    periods = set()
    for metric in metric_reports.keys():
      if metric_reports[metric].get_num_periods() > 0:
        periods.add(metric_reports[metric].get_num_periods())

    if len(periods) > 1:
      raise ValueError("All weekly measurements must have the same number of periods.")

    self._num_periods = periods.pop() if len(periods) == 1 else 0

    frequencies = set()
    for metric in metric_reports.keys():
      if metric_reports[metric].get_num_frequencies() > 0:
        frequencies.add(metric_reports[metric].get_num_frequencies())

    if len(frequencies) > 1:
      raise ValueError("All k-reach measurements must have the same number of frequencies.")

    self._num_frequencies = frequencies.pop() if len(frequencies) == 1 else 0

    # Assigns an index to each measurement and keeps track of the max standard
    # deviation. This max standard deviation will be used to normalized the
    # standard deviation of the measurements when the report is corrected.
    measurement_index = 0
    self._measurement_name_to_index = {}
    self._index_to_measurement_name = {}
    self._measurement_name_to_measurement = {}
    self._max_standard_deviation = UNIT_SCALING_FACTOR
    for metric in metric_reports.keys():
      metric_report = metric_reports[metric]
      # Assigns an index for whole campaign reaches.
      for edp_combination in metric_report.get_whole_campaign_reach_edp_combinations():
        measurement = metric_report.get_whole_campaign_reach_measurement(
          edp_combination)
        self._measurement_name_to_index[measurement.name] = measurement_index
        self._index_to_measurement_name[measurement_index] = measurement.name
        self._measurement_name_to_measurement[measurement.name] = measurement
        self._max_standard_deviation = max(self._max_standard_deviation,
                                           measurement.sigma)
        measurement_index += 1

      # Assigns an index for cumulative reaches.
      for edp_combination in metric_report.get_weekly_cumulative_reach_edp_combinations():
        for period in range(0, self._num_periods):
          measurement = metric_report.get_weekly_cumulative_reach_measurement(
            edp_combination, period)
          self._measurement_name_to_index[measurement.name] = measurement_index
          self._index_to_measurement_name[measurement_index] = measurement.name
          self._measurement_name_to_measurement[measurement.name] = measurement
          self._max_standard_deviation = max(self._max_standard_deviation,
                                             measurement.sigma)
          measurement_index += 1

      # Assign an index for whole campaign k_reach.
      for edp_combination in metric_report.get_whole_campaign_k_reach_edp_combinations():
        for frequency in range(1, self._num_frequencies + 1):
          measurement = metric_report.get_whole_campaign_k_reach_measurement(
                  edp_combination, frequency)
          self._measurement_name_to_index[measurement.name] = measurement_index
          self._index_to_measurement_name[measurement_index] = measurement.name
          self._measurement_name_to_measurement[measurement.name] = measurement
          self._max_standard_deviation = max(self._max_standard_deviation,
                                             measurement.sigma)
          measurement_index += 1

      # Assigns an index for whole campaign impressions.
      for edp_combination in metric_report.get_whole_campaign_impression_edp_combinations():
        measurement = metric_report.get_whole_campaign_impression_measurement(
          edp_combination)
        self._measurement_name_to_index[measurement.name] = measurement_index
        self._index_to_measurement_name[measurement_index] = measurement.name
        self._measurement_name_to_measurement[measurement.name] = measurement
        self._max_standard_deviation = max(self._max_standard_deviation,
                                           measurement.sigma)
        measurement_index += 1

      # Assign an index for weekly non cumulative reach.
      for edp_combination in metric_report.get_weekly_non_cumulative_reach_edp_combinations():
        for period in range(0, self._num_periods):
          measurement = metric_report.get_weekly_non_cumulative_reach_measurement(
            edp_combination, period)
          self._measurement_name_to_index[measurement.name] = measurement_index
          self._index_to_measurement_name[measurement_index] = measurement.name
          self._measurement_name_to_measurement[measurement.name] = measurement
          self._max_standard_deviation = max(self._max_standard_deviation,
                                             measurement.sigma)
          measurement_index += 1

      # Assign an index for weekly non cumulative k-reach.
      for edp_combination in metric_report.get_weekly_non_cumulative_k_reach_edp_combinations():
        for period in range(0, self._num_periods):
          for frequency in range(1, self._num_frequencies + 1):
            measurement = metric_report.get_weekly_non_cumulative_k_reach_measurement(
              edp_combination, period, frequency)
            self._measurement_name_to_index[measurement.name] = measurement_index
            self._index_to_measurement_name[measurement_index] = measurement.name
            self._measurement_name_to_measurement[measurement.name] = measurement
            self._max_standard_deviation = max(self._max_standard_deviation,
                                               measurement.sigma)
            measurement_index += 1

      # Assign an index for weekly non cumulative impression.
      for edp_combination in metric_report.get_weekly_non_cumulative_impression_edp_combinations():
        for period in range(0, self._num_periods):
          measurement = metric_report.get_weekly_non_cumulative_impression_measurement(
            edp_combination, period)
          self._measurement_name_to_index[measurement.name] = measurement_index
          self._index_to_measurement_name[measurement_index] = measurement.name
          self._measurement_name_to_measurement[measurement.name] = measurement
          self._max_standard_deviation = max(self._max_standard_deviation,
                                             measurement.sigma)
          measurement_index += 1

    self._num_vars = measurement_index

  def get_all_measurement_names(self) -> list[str]:
    return list(self._measurement_name_to_measurement.keys())

  def get_measurement_from_name(self, measurement_name: str) -> Measurement:
    if measurement_name not in self._measurement_name_to_measurement:
      return None
    return self._measurement_name_to_measurement[measurement_name]

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

    report_post_processor_result = ReportPostProcessorResult(
        updated_measurements={},
        status=report_post_processor_status,
        pre_correction_quality=pre_correction_quality,
        post_correction_quality=post_correction_quality,
    )

    if corrected_report:
      for measurement_name in self._measurement_name_to_measurement.keys():
        measurement = self.get_measurement_from_name(measurement_name)
        corrected_measurement = corrected_report.get_measurement_from_name(
            measurement_name
        )
        if measurement is None or corrected_measurement is None:
          continue

        difference = abs(corrected_measurement.value - measurement.value)

        # LARGE_CORRECTION_THRESHOLD is used to prevent the false positive cases
        # where sigma is zero. For metric with zero standard deviation, its
        # corrected value must be far enough from the original value to be
        # considered a large correction.
        if difference > max(
          STANDARD_DEVIATION_TEST_THRESHOLD*measurement.sigma,
          LARGE_CORRECTION_THRESHOLD
        ):
          logging.warning(
            f"Measurement {measurement.name} has a large correction: original="
            f"{measurement.value}, corrected={corrected_measurement.value}, "
            f"sigma={measurement.sigma}."
          )
          report_post_processor_result.large_corrections.add(
            metric_title=measurement_name,
            original_value=round(measurement.value),
            corrected_value=round(corrected_measurement.value),
            sigma=measurement.sigma,
          )

    return corrected_report, report_post_processor_result

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
        metric].get_weekly_cumulative_reach_edp_combinations():
        for period in range(0, self._num_periods):
          array.put(
              self._get_measurement_index(
                  self._metric_reports[metric]
                  .get_weekly_cumulative_reach_measurement(
                      edp_combination, period)
              ),
              self._metric_reports[metric]
              .get_weekly_cumulative_reach_measurement(edp_combination, period)
              .value,
          )
      for edp_combination in self._metric_reports[
        metric].get_whole_campaign_reach_edp_combinations():
        array.put(
            self._get_measurement_index(
                self._metric_reports[metric]
                .get_whole_campaign_reach_measurement(edp_combination)
            ),
            self._metric_reports[metric]
            .get_whole_campaign_reach_measurement(edp_combination)
            .value,
        )
      for edp_combination in self._metric_reports[
        metric].get_whole_campaign_k_reach_edp_combinations():
        for frequency in range(1, self._num_frequencies + 1):
          array.put(
              self._get_measurement_index(
                  self._metric_reports[
                      metric].get_whole_campaign_k_reach_measurement(
                          edp_combination, frequency)
              ),
              self._metric_reports[
                  metric].get_whole_campaign_k_reach_measurement(
                      edp_combination, frequency).value
          )

      for edp_combination in self._metric_reports[
        metric].get_whole_campaign_impression_edp_combinations():
        array.put(
            self._get_measurement_index(
                self._metric_reports[
                    metric].get_whole_campaign_impression_measurement(
                        edp_combination)
            ),
            self._metric_reports[
                metric].get_whole_campaign_impression_measurement(
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
                  self._get_weekly_cumulative_reach_measurement_index(
                      metric, covering_child, period)
                  for covering_child in covering_children),
              parent=self._get_weekly_cumulative_reach_measurement_index(metric,
                                                            covered_parent,
                                                            period),
          )
      for cover_relationship in self._metric_reports[
        metric].get_whole_campaign_reach_cover_relationships():
        logging.debug(
            f"Adding {metric} cover relations for total campaign measurements."
        )
        covered_parent = cover_relationship[0]
        covering_children = cover_relationship[1]
        spec.add_cover(
            children=list(self._get_whole_campaign_reach_measurement_index(
                metric, covering_child)
                          for covering_child in covering_children),
            parent=self._get_whole_campaign_reach_measurement_index(
                metric, covered_parent),
        )
      for cover_relationship in self._metric_reports[
        metric].get_weekly_non_cumulative_reach_cover_relationships():
        logging.debug(
            f"Adding {metric} cover relations for weekly non cumulative reach "
            f"measurements."
        )
        covered_parent = cover_relationship[0]
        covering_children = cover_relationship[1]
        for period in range(0, self._num_periods):
          spec.add_cover(
              children=list(
                  self._get_weekly_non_cumulative_reach_measurement_index(
                      metric, covering_child, period)
                  for covering_child in covering_children),
              parent=self._get_weekly_non_cumulative_reach_measurement_index(
                  metric, covered_parent, period),
          )
    logging.info("Finished adding cover relations to spec.")

  def _add_subset_relations_to_spec(self, spec: SetMeasurementsSpec):
    for metric in self._metric_reports:
      metric_report = self._metric_reports[metric]
      # Adds relations for cumulative measurements.
      for subset_relationship in \
          metric_report.get_cumulative_subset_relationships():
        parent_edp_combination = subset_relationship[0]
        child_edp_combination = subset_relationship[1]
        for period in range(0, self._num_periods):
          spec.add_subset_relation(
              child_set_id=self._get_measurement_index(
                  metric_report.get_weekly_cumulative_reach_measurement(
                      child_edp_combination, period
                  )
              ),
              parent_set_id=self._get_measurement_index(
                  metric_report.get_weekly_cumulative_reach_measurement(
                      parent_edp_combination, period
                  )
              ),
          )

      # Adds relations for whole campaign measurements.
      for subset_relationship in \
          metric_report.get_whole_campaign_reach_subset_relationships():
        parent_edp_combination = subset_relationship[0]
        child_edp_combination = subset_relationship[1]
        spec.add_subset_relation(
            child_set_id=self._get_measurement_index(
                metric_report.get_whole_campaign_reach_measurement(
                    child_edp_combination
                )
            ),
            parent_set_id=self._get_measurement_index(
                metric_report.get_whole_campaign_reach_measurement(
                    parent_edp_combination
                )
            ),
        )

      # Adds relations for weekly non cumulative measurements.
      for subset_relationship in \
          metric_report.get_weekly_non_cumulative_reach_subset_relationships():
        parent_edp_combination = subset_relationship[0]
        child_edp_combination = subset_relationship[1]
        for period in range(0, self._num_periods):
            spec.add_subset_relation(
                child_set_id=self._get_measurement_index(
                    metric_report.get_weekly_non_cumulative_reach_measurement(
                        child_edp_combination, period
                    )
                ),
                parent_set_id=self._get_measurement_index(
                    metric_report.get_weekly_non_cumulative_reach_measurement(
                        parent_edp_combination, period
                    )
                ),
            )

    logging.info("Finished adding subset relations to spec.")

  def _get_ordered_sets(
      self,
      union_getter_smaller: callable,
      singles_getter_smaller: callable,
      union_getter_greater: callable,
      singles_getter_greater: callable,
  ) -> OrderedSets:
    """Creates an OrderedSets object representing an overlap constraint.

    This function constructs two sets of measurement indices, a "smaller" set
    and a "greater" set.

    Let A_1, ..., A_k be one collection of sets and B_1, ..., B_k be another,
    where A_i is a subset of B_i for all i. The overlap constraint is:
    sum(|A_i|) - |union(A_i)| <= sum(|B_i|) - |union(B_i)|

    This can be rearranged to:
    sum(|A_i|) + |union(B_i)| <= sum(|B_i|) + |union(A_i)|

    This function builds the 'smaller_set' (left side) and 'greater_set'
    (right side) of this inequality.

    Args:
        union_getter_smaller: A function that returns the measurement for the
          union of the smaller collection (A_i).
        singles_getter_smaller: A function that returns the list of measurements
          for each individual set in the smaller collection.
        union_getter_greater: A function that returns the measurement for the
          union of the greater collection (B_i).
        singles_getter_greater: A function that returns the list of
          measurements for each individual set in the greater collection.

    Returns:
        An OrderedSets object representing the constraint.
    """
    smaller_set = [self._get_measurement_index(union_getter_greater())]
    smaller_set.extend(
        self._get_measurement_index(measurement)
        for measurement in singles_getter_smaller()
    )
    greater_set = [self._get_measurement_index(union_getter_smaller())]
    greater_set.extend(
        self._get_measurement_index(measurement)
        for measurement in singles_getter_greater()
    )
    return OrderedSets(set(greater_set), set(smaller_set))

  def _add_overlap_relations_to_spec(self, spec: SetMeasurementsSpec):
    # Overlap constraints within a metric.
    for metric in self._metric_reports:
      metric_report = self._metric_reports[metric]
      cumulative_edp_combinations = \
        metric_report.get_weekly_cumulative_reach_edp_combinations()
      for edp_combination in cumulative_edp_combinations:
        if len(edp_combination) <= 1:
          continue
        edps: list[EdpCombination] = get_edps_from_edp_combination(
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
          spec.add_ordered_sets_relation(
              self._get_ordered_sets(
                  union_getter_smaller=lambda: metric_report.get_weekly_cumulative_reach_measurement(
                      edp_combination, period
                  ),
                  singles_getter_smaller=lambda: [
                      metric_report.get_weekly_cumulative_reach_measurement(edp, period)
                      for edp in edps
                  ],
                  union_getter_greater=lambda: metric_report.get_weekly_cumulative_reach_measurement(
                      edp_combination, period + 1
                  ),
                  singles_getter_greater=lambda: [
                      metric_report.get_weekly_cumulative_reach_measurement(edp, period + 1)
                      for edp in edps
                  ],
              ))

  def _add_cumulative_whole_campaign_relations_to_spec(self,
      spec: SetMeasurementsSpec):
    # Adds relations between cumulative and whole campaign measurements.
    # For an edp combination, the last cumulative reach is equal to the whole
    # campaign reach.
    for metric in self._metric_reports:
      metric_report = self._metric_reports[metric]
      common_edp_combinations = \
        metric_report.get_weekly_cumulative_reach_edp_combinations().intersection(
            metric_report.get_whole_campaign_reach_edp_combinations())
      for edp_combination in common_edp_combinations:
        spec.add_equal_relation(
            set_id_one=self._get_measurement_index(
                metric_report.get_weekly_cumulative_reach_measurement(
                    edp_combination, (self._num_periods - 1))),
            set_id_two=[
                self._get_measurement_index(
                    metric_report.get_whole_campaign_reach_measurement(
                        edp_combination))
            ],
        )
    logging.info(
        "Finished adding the relationship between cumulative and total "
        "campaign measurements to spec."
    )

  def _add_k_reach_and_reach_relations_to_spec(self,
      spec: SetMeasurementsSpec):
    for metric in self._metric_reports:
      metric_report = self._metric_reports[metric]
      # Gets relations for whole campaign.
      common_whole_campaign_edp_combinations = \
        metric_report.get_whole_campaign_reach_edp_combinations().intersection(
            metric_report.get_whole_campaign_k_reach_edp_combinations())
      for edp_combination in common_whole_campaign_edp_combinations:
        spec.add_equal_relation(
            set_id_one=self._get_measurement_index(
                metric_report.get_whole_campaign_reach_measurement(
                    edp_combination)),
            set_id_two=[
                self._get_measurement_index(
                    metric_report.get_whole_campaign_k_reach_measurement(
                       edp_combination, frequency)
                )
                for frequency in range(1, self._num_frequencies + 1)
            ]
        )
      # Gets relations for weekly non cumulative.
      common_weekly_non_cumulative_edp_combinations = \
        metric_report.get_weekly_non_cumulative_reach_edp_combinations().intersection(
            metric_report.get_weekly_non_cumulative_k_reach_edp_combinations())
      for edp_combination in common_weekly_non_cumulative_edp_combinations:
        for period in range(0, self._num_periods):
            spec.add_equal_relation(
                set_id_one=self._get_measurement_index(
                    metric_report.get_weekly_non_cumulative_reach_measurement(
                        edp_combination, period)),
                set_id_two=[
                    self._get_measurement_index(
                        metric_report.get_weekly_non_cumulative_k_reach_measurement(
                           edp_combination, period, frequency)
                    )
                    for frequency in range(1, self._num_frequencies + 1)
                ]
            )

  def _add_impression_relations_to_spec(self, spec: SetMeasurementsSpec):
    for metric in self._metric_reports:
      metric_report = self._metric_reports[metric]
      # Gets relations for whole campaign impression.
      whole_campaign_edp_combinations = metric_report.get_whole_campaign_impression_edp_combinations()
      for edp_combination in whole_campaign_edp_combinations:
        if len(edp_combination) > 1:
          single_edp_subset = [
              comb for comb in whole_campaign_edp_combinations
              if len(comb) == 1 and comb.issubset(edp_combination)
          ]
          spec.add_equal_relation(
              set_id_one=self._get_measurement_index(
                  metric_report.get_whole_campaign_impression_measurement(
                      edp_combination)),
              set_id_two=[
                  self._get_measurement_index(
                      metric_report.get_whole_campaign_impression_measurement(
                          child_edp)
                  )
                  for child_edp in single_edp_subset
              ]
          )

      # Gets relations for weekly non cumulative impression.
      weekly_non_cumulative_edp_combinations = metric_report.get_weekly_non_cumulative_impression_edp_combinations()
      for edp_combination in weekly_non_cumulative_edp_combinations:
        if len(edp_combination) > 1:
          single_edp_subset = [
              comb for comb in weekly_non_cumulative_edp_combinations
              if len(comb) == 1 and comb.issubset(edp_combination)
          ]
          for period in range(0, self._num_periods):
              spec.add_equal_relation(
                  set_id_one=self._get_measurement_index(
                      metric_report.get_weekly_non_cumulative_impression_measurement(
                          edp_combination, period)),
                  set_id_two=[
                      self._get_measurement_index(
                          metric_report.get_weekly_non_cumulative_impression_measurement(
                              child_edp, period)
                      )
                      for child_edp in single_edp_subset
                  ]
              )


  def _add_reach_impression_relations_to_spec(self,
      spec: SetMeasurementsSpec):
    for metric in self._metric_reports:
      metric_report = self._metric_reports[metric]
      # Gets relations for whole campaign impression.
      common_whole_campaign_edp_combinations = \
        metric_report.get_whole_campaign_reach_edp_combinations().intersection(
            metric_report.get_whole_campaign_impression_edp_combinations())
      for edp_combination in common_whole_campaign_edp_combinations:
        spec.add_subset_relation(
            child_set_id=self._get_measurement_index(
                metric_report.get_whole_campaign_reach_measurement(
                    edp_combination)),
            parent_set_id=self._get_measurement_index(
                metric_report.get_whole_campaign_impression_measurement(
                    edp_combination)),
        )

      # Gets relations for weekly non cumulative impression.
      common_weekly_non_cumulative_edp_combinations = \
        metric_report.get_weekly_non_cumulative_reach_edp_combinations().intersection(
            metric_report.get_weekly_non_cumulative_impression_edp_combinations())
      for edp_combination in common_weekly_non_cumulative_edp_combinations:
        for period in range(0, self._num_periods):
          spec.add_subset_relation(
              child_set_id=self._get_measurement_index(
                  metric_report.get_weekly_non_cumulative_reach_measurement(
                      edp_combination, period)),
              parent_set_id=self._get_measurement_index(
                  metric_report.get_weekly_non_cumulative_impression_measurement(
                      edp_combination, period)),
          )


  def _add_k_reach_impression_relations_to_spec(self,
      spec: SetMeasurementsSpec):
    for metric in self._metric_reports:
      metric_report = self._metric_reports[metric]
      # Gets relations for whole campaign measurements.
      common_whole_campaign_edp_combinations = \
        metric_report.get_whole_campaign_k_reach_edp_combinations().intersection(
            metric_report.get_whole_campaign_impression_edp_combinations())
      for edp_combination in common_whole_campaign_edp_combinations:
        spec.add_weighted_sum_upperbound_relation(
            weighted_id_set=[
                [
                    self._get_measurement_index(
                        metric_report.get_whole_campaign_k_reach_measurement(
                            edp_combination, frequency)),
                    frequency
                ]
                for frequency in range(1, self._num_frequencies + 1)
            ],
            upperbound_id=self._get_measurement_index(
                metric_report.get_whole_campaign_impression_measurement(
                    edp_combination))
        )

      # Gets relations for weekly non cumulative measurements.
      common_weekly_non_cumulative_edp_combinations = \
        metric_report.get_weekly_non_cumulative_k_reach_edp_combinations().intersection(
            metric_report.get_weekly_non_cumulative_impression_edp_combinations())
      for edp_combination in common_weekly_non_cumulative_edp_combinations:
        for period in range(0, self._num_periods):
            spec.add_weighted_sum_upperbound_relation(
                weighted_id_set=[
                    [
                        self._get_measurement_index(
                            metric_report.get_weekly_non_cumulative_k_reach_measurement(
                                edp_combination, period, frequency)),
                        frequency
                    ]
                    for frequency in range(1, self._num_frequencies + 1)
                ],
                upperbound_id=self._get_measurement_index(
                    metric_report.get_weekly_non_cumulative_impression_measurement(
                        edp_combination, period))
            )

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

        # Enforce that for any EDP combination, the child metric's cumulative
        # reach is a less than or equal to the parent's.
        common_cumulative_edp_combinations = \
          parent_metric_report.get_weekly_cumulative_reach_edp_combinations().intersection(
              child_metric_report.get_weekly_cumulative_reach_edp_combinations()
          )
        for edp_combination in common_cumulative_edp_combinations:
          # For each period, child_reach <= parent_reach.
          for period in range(0, self._num_periods):
            spec.add_subset_relation(
                child_set_id=self._get_measurement_index(
                    child_metric_report.get_weekly_cumulative_reach_measurement(
                        edp_combination, period)),
                parent_set_id=self._get_measurement_index(
                    parent_metric_report.get_weekly_cumulative_reach_measurement(
                        edp_combination, period)),
            )

        # Enforce overlap constraints for cumulative reach across metrics.
        for edp_combination in common_cumulative_edp_combinations:
          if len(edp_combination) <= 1:
            continue
          edps: list[EdpCombination] = get_edps_from_edp_combination(
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
            spec.add_ordered_sets_relation(self._get_ordered_sets(
                union_getter_smaller=lambda: child_metric_report.get_weekly_cumulative_reach_measurement(
                    edp_combination, period
                ),
                singles_getter_smaller=lambda: [
                    child_metric_report.get_weekly_cumulative_reach_measurement(edp, period)
                    for edp in edps
                ],
                union_getter_greater=lambda: parent_metric_report.get_weekly_cumulative_reach_measurement(
                    edp_combination, period
                ),
                singles_getter_greater=lambda: [
                    parent_metric_report.get_weekly_cumulative_reach_measurement(edp, period)
                    for edp in edps
                ],
            ))

        # Enforce that for any EDP combination, the child metric's whole
        # campaign reach is is less than or equal to the parent's.
        common_whole_campaign_edp_combinations = \
          parent_metric_report.get_whole_campaign_reach_edp_combinations().intersection(
              child_metric_report.get_whole_campaign_reach_edp_combinations())
        for edp_combination in common_whole_campaign_edp_combinations:
          spec.add_subset_relation(
              # child_reach <= parent_reach
              child_set_id=self._get_measurement_index(
                  child_metric_report.get_whole_campaign_reach_measurement(
                      edp_combination)),
              parent_set_id=self._get_measurement_index(
                  parent_metric_report.get_whole_campaign_reach_measurement(
                      edp_combination)),
          )

        # Enforce overlap constraints for whole campaign reach across metrics.
        for edp_combination in common_whole_campaign_edp_combinations:
          if len(edp_combination) <= 1:
            continue

          edps: list[EdpCombination] = get_edps_from_edp_combination(
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

          spec.add_ordered_sets_relation(self._get_ordered_sets(
              union_getter_smaller=lambda: child_metric_report.get_whole_campaign_reach_measurement(
                  edp_combination
              ),
              singles_getter_smaller=lambda: [
                  child_metric_report.get_whole_campaign_reach_measurement(edp)
                  for edp in edps
              ],
              union_getter_greater=lambda: parent_metric_report.get_whole_campaign_reach_measurement(
                  edp_combination
              ),
              singles_getter_greater=lambda: [
                  parent_metric_report.get_whole_campaign_reach_measurement(edp)
                  for edp in edps
              ],
          ))

        # Enforce that for any EDP combination, the child metric's impression
        # is less than or equal to the parent's.
        common_impression_edp_combinations = \
          parent_metric_report.get_whole_campaign_impression_edp_combinations().intersection(
              child_metric_report.get_whole_campaign_impression_edp_combinations())
        for edp_combination in common_impression_edp_combinations:
          spec.add_subset_relation(
              # child_impression <= parent_impression
              child_set_id=self._get_measurement_index(
                  child_metric_report.get_whole_campaign_impression_measurement(
                      edp_combination)),
              parent_set_id=self._get_measurement_index(
                  parent_metric_report.get_whole_campaign_impression_measurement(
                      edp_combination)),
          )

        # Enforce that for any EDP combination, the child metric's weekly
        # non-cumulative reach is less than or equal to the parent's.
        common_weekly_non_cumulative_edp_combinations = (
            parent_metric_report.get_weekly_non_cumulative_reach_edp_combinations().intersection(
                child_metric_report.get_weekly_non_cumulative_reach_edp_combinations()
            )
        )
        for edp_combination in common_weekly_non_cumulative_edp_combinations:
          for period in range(self._num_periods):
            spec.add_subset_relation(
                # child_reach <= parent_reach for each week.
                child_set_id=self._get_measurement_index(
                    child_metric_report.get_weekly_non_cumulative_reach_measurement(
                        edp_combination, period
                    )
                ),
                parent_set_id=self._get_measurement_index(
                    parent_metric_report.get_weekly_non_cumulative_reach_measurement(
                        edp_combination, period
                    )
                ),
            )

        # Enforce overlap constraints for weekly non-cumulative reach across
        # metrics.
        for edp_combination in common_weekly_non_cumulative_edp_combinations:
          if len(edp_combination) <= 1:
            continue

          edps: list[EdpCombination] = get_edps_from_edp_combination(
              edp_combination, common_weekly_non_cumulative_edp_combinations
          )
          if len(edps) != len(edp_combination):
            continue

          for period in range(self._num_periods):
            spec.add_ordered_sets_relation(self._get_ordered_sets(
                union_getter_smaller=lambda: child_metric_report.get_weekly_non_cumulative_reach_measurement(
                    edp_combination, period
                ),
                singles_getter_smaller=lambda: [
                    child_metric_report.get_weekly_non_cumulative_reach_measurement(edp, period)
                    for edp in edps
                ],
                union_getter_greater=lambda: parent_metric_report.get_weekly_non_cumulative_reach_measurement(
                    edp_combination, period
                ),
                singles_getter_greater=lambda: [
                    parent_metric_report.get_weekly_non_cumulative_reach_measurement(edp, period)
                    for edp in edps
                ],
            ))

        # Enforce that for any EDP combination, the child metric's weekly
        # non-cumulative impression is less than or equal to the parent's.
        common_weekly_non_cumulative_impression_edp_combinations = (
            parent_metric_report.get_weekly_non_cumulative_impression_edp_combinations().intersection(
                child_metric_report.get_weekly_non_cumulative_impression_edp_combinations()
            )
        )
        for edp_combination in common_weekly_non_cumulative_impression_edp_combinations:
          for period in range(self._num_periods):
              spec.add_subset_relation(
                  # child_impression <= parent_impression for each week.
                  child_set_id=self._get_measurement_index(
                      child_metric_report.get_weekly_non_cumulative_impression_measurement(
                          edp_combination, period
                      )
                  ),
                  parent_set_id=self._get_measurement_index(
                      parent_metric_report.get_weekly_non_cumulative_impression_measurement(
                          edp_combination, period
                      )
                  ),
              )

  logging.info(
      "Finished adding the relationship for measurements from different "
      "metrics."
  )

  def _add_cumulative_relations_to_spec(self, spec: SetMeasurementsSpec):
    for metric in self._metric_reports.keys():
      for edp_combination in self._metric_reports[
        metric].get_weekly_cumulative_reach_edp_combinations():
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
                    metric].get_weekly_cumulative_reach_measurement(
                      edp_combination, period)),
              parent_set_id=self._get_measurement_index(
                  self._metric_reports[
                    metric].get_weekly_cumulative_reach_measurement(
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

    self._add_k_reach_and_reach_relations_to_spec(spec)

    self._add_impression_relations_to_spec(spec)

    self._add_reach_impression_relations_to_spec(spec)

    self._add_k_reach_impression_relations_to_spec(spec)

    # Last cumulative measurement <= whole campaign measurement.
    self._add_cumulative_whole_campaign_relations_to_spec(spec)

    # metric#1>=metric#2.
    self._add_metric_relations_to_spec(spec)

    logging.info("Finished adding set relations to spec.")

  def _add_weekly_cumulative_measurements_to_spec(self,
                                                  spec: SetMeasurementsSpec):
    for metric in self._metric_reports.keys():
      metric_report = self._metric_reports[metric]
      for edp_combination in metric_report.get_weekly_cumulative_reach_edp_combinations():
        for period in range(self._num_periods):
          measurement = metric_report.get_weekly_cumulative_reach_measurement(
              edp_combination, period)
          spec.add_measurement(
              self._get_measurement_index(measurement),
              Measurement(measurement.value,
                          self._normalized_sigma(measurement.sigma),
                          measurement.name),
          )

  def _add_whole_campaign_measurements_to_spec(self,
                                               spec: SetMeasurementsSpec):
    for metric in self._metric_reports.keys():
      metric_report = self._metric_reports[metric]
      # Whole campaign reach measurements.
      for edp_combination in metric_report.get_whole_campaign_reach_edp_combinations():
        measurement = metric_report.get_whole_campaign_reach_measurement(
            edp_combination)
        spec.add_measurement(
            self._get_measurement_index(measurement),
            Measurement(measurement.value,
                        self._normalized_sigma(measurement.sigma),
                        measurement.name),
        )

      # Whole campaign k-reach measurements.
      for edp_combination in metric_report.get_whole_campaign_k_reach_edp_combinations():
        for frequency in range(1, self._num_frequencies + 1):
          measurement = metric_report.get_whole_campaign_k_reach_measurement(
              edp_combination, frequency)
          spec.add_measurement(
              self._get_measurement_index(measurement),
              Measurement(measurement.value,
                          self._normalized_sigma(measurement.sigma),
                          measurement.name),
          )

      # Whole campaign impression measurements.
      for edp_combination in metric_report.get_whole_campaign_impression_edp_combinations():
        measurement = metric_report.get_whole_campaign_impression_measurement(
            edp_combination)
        spec.add_measurement(
            self._get_measurement_index(measurement),
            Measurement(measurement.value,
                        self._normalized_sigma(measurement.sigma),
                        measurement.name),
        )


  def _add_weekly_non_cumulative_measurements_to_spec(self, spec: SetMeasurementsSpec):
    for metric in self._metric_reports.keys():
      metric_report = self._metric_reports[metric]
      for edp_combination in metric_report.get_weekly_non_cumulative_reach_edp_combinations():
        for period in range(self._num_periods):
          measurement = metric_report.get_weekly_non_cumulative_reach_measurement(
            edp_combination, period)
          spec.add_measurement(
              self._get_measurement_index(measurement),
              Measurement(measurement.value,
                          self._normalized_sigma(measurement.sigma),
                          measurement.name),
          )

      for edp_combination in metric_report.get_weekly_non_cumulative_k_reach_edp_combinations():
        for period in range(self._num_periods):
          for frequency in range(1, self._num_frequencies + 1):
            measurement = metric_report.get_weekly_non_cumulative_k_reach_measurement(
              edp_combination, period, frequency)
            spec.add_measurement(
                self._get_measurement_index(measurement),
                Measurement(measurement.value,
                            self._normalized_sigma(measurement.sigma),
                            measurement.name),
            )

      for edp_combination in metric_report.get_weekly_non_cumulative_impression_edp_combinations():
        for period in range(self._num_periods):
          measurement = metric_report.get_weekly_non_cumulative_impression_measurement(
            edp_combination, period)
          spec.add_measurement(
              self._get_measurement_index(measurement),
              Measurement(measurement.value,
                          self._normalized_sigma(measurement.sigma),
                          measurement.name),
          )


  def _add_measurements_to_spec(self, spec: SetMeasurementsSpec):
    self._add_weekly_cumulative_measurements_to_spec(spec)
    self._add_whole_campaign_measurements_to_spec(spec)
    self._add_weekly_non_cumulative_measurements_to_spec(spec)
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

  def _get_weekly_cumulative_reach_measurement_index(
      self, metric: str, edp_combination: EdpCombination, period: int
  ) -> int:
    return self._get_measurement_index(
        self._metric_reports[metric].get_weekly_cumulative_reach_measurement(
            edp_combination, period)
    )

  def _get_whole_campaign_reach_measurement_index(
      self, metric: str, edp_combination: EdpCombination
  ) -> int:
    return self._get_measurement_index(
        self._metric_reports[metric].get_whole_campaign_reach_measurement(
            edp_combination)
    )

  def _get_weekly_non_cumulative_reach_measurement_index(
      self, metric: str, edp_combination: EdpCombination, period: int
    ) -> int:
    return self._get_measurement_index(
        self._metric_reports[metric].get_weekly_non_cumulative_reach_measurement(
            edp_combination, period)
    )

  def _metric_report_from_solution(self, metric: str,
      solution: Solution) -> "MetricReport":
    logging.debug(f"Generating the metric report for {metric}.")
    solution_time_series = {}
    solution_whole_campaign = {}
    solution_k_reach = {}
    solution_impression = {}
    solution_weekly_non_cumulative = {}

    metric_report = self._metric_reports[metric]
    for edp_combination in metric_report.get_weekly_cumulative_reach_edp_combinations():
      solution_time_series[edp_combination] = [
          Measurement(
              solution[
                self._get_measurement_index(
                    metric_report.get_weekly_cumulative_reach_measurement(
                        edp_combination, period))
              ],
              metric_report.get_weekly_cumulative_reach_measurement(
                  edp_combination, period).sigma,
              metric_report.get_weekly_cumulative_reach_measurement(
                  edp_combination, period).name,
          )
          for period in range(0, self._num_periods)
      ]
    for edp_combination in metric_report.get_whole_campaign_reach_edp_combinations():
      solution_whole_campaign[edp_combination] = Measurement(
          solution[
            self._get_measurement_index(
                metric_report.get_whole_campaign_reach_measurement(
                    edp_combination)
            )
          ],
          metric_report.get_whole_campaign_reach_measurement(
              edp_combination).sigma,
          metric_report.get_whole_campaign_reach_measurement(
              edp_combination).name,
      )
    for edp_combination in metric_report.get_whole_campaign_k_reach_edp_combinations():
      solution_k_reach[edp_combination] = {
          frequency: Measurement(
              solution[
                self._get_measurement_index(
                    metric_report.get_whole_campaign_k_reach_measurement(
                        edp_combination, frequency))
              ],
              metric_report.get_whole_campaign_k_reach_measurement(
                  edp_combination, frequency).sigma,
              metric_report.get_whole_campaign_k_reach_measurement(
                 edp_combination, frequency).name
          )
          for frequency in range(1, self._num_frequencies + 1)
      }

    for edp_combination in metric_report.get_whole_campaign_impression_edp_combinations():
      solution_impression[edp_combination] = Measurement(
          solution[
            self._get_measurement_index(
                metric_report.get_whole_campaign_impression_measurement(
                    edp_combination))
          ],
          metric_report.get_whole_campaign_impression_measurement(
              edp_combination).sigma,
          metric_report.get_whole_campaign_impression_measurement(
              edp_combination).name,
      )

    all_edps = (
        set(solution_whole_campaign.keys())
        | set(solution_k_reach.keys())
        | set(solution_impression.keys())
    )

    solution_whole_campaign_measurements = {}

    for edp in all_edps:
      reach = solution_whole_campaign.get(edp)
      k_reach = solution_k_reach.get(edp, {})
      impression = solution_impression.get(edp)

      if reach or impression or len(k_reach) > 0:
        solution_whole_campaign_measurements[edp] = MeasurementSet(
            reach=reach, k_reach=k_reach, impression=impression
        )

    solution_weekly_non_cumulative = \
        self._get_weekly_non_cumulative_from_solution(metric, solution)
    return MetricReport(
        weekly_cumulative_reaches=solution_time_series,
        whole_campaign_measurements=solution_whole_campaign_measurements,
        weekly_non_cumulative_measurements=solution_weekly_non_cumulative,
    )

  def _get_weekly_non_cumulative_from_solution(
      self, metric: str, solution: Solution
  ) -> dict[EdpCombination, list[MeasurementSet]]:
    """Reconstructs weekly non-cumulative measurements from a solver solution."""
    metric_report = self._metric_reports[metric]
    solution_weekly_non_cumulative = {}

    all_edps = (
        metric_report.get_weekly_non_cumulative_reach_edp_combinations()
        | metric_report.get_weekly_non_cumulative_k_reach_edp_combinations()
        | metric_report.get_weekly_non_cumulative_impression_edp_combinations()
    )

    for edp_combination in all_edps:
      solution_weekly_non_cumulative[edp_combination] = []
      for period in range(self._num_periods):
        reach = metric_report.get_weekly_non_cumulative_reach_measurement(
            edp_combination, period
        )
        if reach:
          reach = Measurement(
              solution[self._get_measurement_index(reach)],
              reach.sigma,
              reach.name,
          )

        k_reach = {
            freq: Measurement(
                solution[self._get_measurement_index(measurement)],
                measurement.sigma,
                measurement.name,
            )
            for freq, measurement in metric_report._weekly_non_cumulative_measurements[
                edp_combination
            ][period].k_reach.items()
        }

        impression = metric_report.get_weekly_non_cumulative_impression_measurement(
            edp_combination, period
        )
        if impression:
          impression = Measurement(
              solution[self._get_measurement_index(impression)],
              impression.sigma,
              impression.name,
          )

        solution_weekly_non_cumulative[edp_combination].append(
            MeasurementSet(reach=reach, k_reach=k_reach, impression=impression)
        )
    return solution_weekly_non_cumulative

  def _is_zero_variance_edp(self, edp_combination: EdpCombination) -> bool:
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
      if edp_combination in metric_report.get_weekly_cumulative_reach_edp_combinations():
        standard_deviations.extend(
            [
                measurement.sigma
                for measurement in
                metric_report.get_weekly_cumulative_reach_measurements(
                    edp_combination)
            ]
        )
      if edp_combination in metric_report.get_whole_campaign_reach_edp_combinations():
        standard_deviations.append(
            metric_report.get_whole_campaign_reach_measurement(
                edp_combination).sigma
        )
      if edp_combination in metric_report.get_whole_campaign_impression_edp_combinations():
        standard_deviations.append(
            metric_report.get_whole_campaign_impression_measurement(
                edp_combination).sigma
        )
      if edp_combination in metric_report.get_whole_campaign_k_reach_edp_combinations():
        standard_deviations.extend(
            [
                measurement.sigma
                for measurement in
                metric_report.get_whole_campaign_k_reach_measurements(
                    edp_combination)
            ]
        )

    return all(val == 0.0 for val in standard_deviations)

  def _are_edp_measurements_consistent(
      self, edp_combination: EdpCombination
  ) -> bool:
    """Checks if all measurements of a specific edp combination are consistent."""
    # Check for the consistency within a metric.
    for metric in self._metric_reports.keys():
      metric_report = self._metric_reports[metric]

      # Gets cumulative and whole campaign measurements.
      cumulative_reach_measurements = (
          metric_report.get_weekly_cumulative_reach_measurements(
              edp_combination
          )
      )
      whole_campaign_reach = metric_report.get_whole_campaign_reach_measurement(
          edp_combination
      )
      whole_campaign_impression = (
          metric_report.get_whole_campaign_impression_measurement(
              edp_combination
          )
      )
      whole_campaign_k_reaches = {
          frequency: metric_report.get_whole_campaign_k_reach_measurement(
              edp_combination, frequency
          )
          for frequency in range(1, self._num_frequencies + 1)
      }

      # Perform consistency checks for cumulative measurements.
      if cumulative_reach_measurements:
        # Cumulative measurements are non-decreasing.
        for period in range(0, self._num_periods - 1):
          if not fuzzy_less_equal(
              cumulative_reach_measurements[period].value,
              cumulative_reach_measurements[period + 1].value,
              CONSISTENCY_TEST_TOLERANCE,
          ):
            return False

        # Whole campaign reach matches last cumulative reach.
        if cumulative_reach_measurements and whole_campaign_reach and not fuzzy_equal(
            cumulative_reach_measurements[-1].value,
            whole_campaign_reach.value,
            CONSISTENCY_TEST_TOLERANCE,
        ):
          return False

      if whole_campaign_reach and all(whole_campaign_k_reaches.values()):
        # Whole campaign reach equals to the sum of k-reaches.
        sum_of_k_reach_values = sum(
            measurement.value
            for measurement in whole_campaign_k_reaches.values()
        )
        if not fuzzy_equal(
            whole_campaign_reach.value,
            sum_of_k_reach_values,
            sum(whole_campaign_k_reaches.keys()) * CONSISTENCY_TEST_TOLERANCE,
        ):
          return False

      if whole_campaign_impression and all(whole_campaign_k_reaches.values()):
        # Impression is greater than or equal to weighted sum of k-reaches.
        weighted_sum_of_k_reach_values = sum(
            freq * measurement.value
            for freq, measurement in whole_campaign_k_reaches.items()
        )
        if not fuzzy_less_equal(
            weighted_sum_of_k_reach_values,
            whole_campaign_impression.value,
            sum(whole_campaign_k_reaches.keys()) * CONSISTENCY_TEST_TOLERANCE,
        ):
          return False

      # Check for weekly non-cumulative measurements.
      for period in range(0, self._num_periods):
        weekly_reach = metric_report.get_weekly_non_cumulative_reach_measurement(
            edp_combination, period)
        weekly_impression = metric_report.get_weekly_non_cumulative_impression_measurement(
            edp_combination, period)
        weekly_k_reaches = {
            frequency: metric_report.get_weekly_non_cumulative_k_reach_measurement(
                edp_combination, period, frequency)
            for frequency in range(1, self._num_frequencies + 1)
        }

        if weekly_reach and all(weekly_k_reaches.values()):
          sum_of_k_reach_values = sum(
              measurement.value
              for measurement in weekly_k_reaches.values())
          # Weekly non-cumulative reach equals to the sum of k-reaches.
          if not fuzzy_equal(
              weekly_reach.value,
              sum_of_k_reach_values,
              sum(weekly_k_reaches.keys()) * CONSISTENCY_TEST_TOLERANCE,
          ):
            return False

        if weekly_impression and all(weekly_k_reaches.values()):
          weighted_sum_of_k_reach_values = sum(
              freq * measurement.value
              for freq, measurement in weekly_k_reaches.items())
          # Impression is >= weighted sum of k-reaches for each week.
          if not fuzzy_less_equal(
              weighted_sum_of_k_reach_values, weekly_impression.value,
              sum(weekly_k_reaches.keys()) * CONSISTENCY_TEST_TOLERANCE,
          ):
            return False

    # Check for the consistency between ordered metrics.
    for parent_metric in self._metric_subsets_by_parent:
      for child_metric in self._metric_subsets_by_parent[parent_metric]:
        parent_metric_report = self._metric_reports[parent_metric]
        child_metric_report = self._metric_reports[child_metric]

        # Handles cumulative measurements.
        cumulative_edp_combinations = \
          parent_metric_report.get_weekly_cumulative_reach_edp_combinations().intersection(
              child_metric_report.get_weekly_cumulative_reach_edp_combinations())
        if edp_combination in cumulative_edp_combinations:
          for period in range(0, self._num_periods):
            if not fuzzy_less_equal(
                child_metric_report.get_weekly_cumulative_reach_measurement(
                    edp_combination, period).value,
                parent_metric_report.get_weekly_cumulative_reach_measurement(
                    edp_combination, period).value,
                CONSISTENCY_TEST_TOLERANCE):
              return False

        # Handles whole campaign measurements.
        whole_campaign_edp_combinations = \
          parent_metric_report.get_whole_campaign_reach_edp_combinations().intersection(
              child_metric_report.get_whole_campaign_reach_edp_combinations())
        if edp_combination in whole_campaign_edp_combinations:
          if not fuzzy_less_equal(
              child_metric_report.get_whole_campaign_reach_measurement(
                  edp_combination).value,
              parent_metric_report.get_whole_campaign_reach_measurement(
                  edp_combination).value,
              CONSISTENCY_TEST_TOLERANCE):
            return False

        # Handles impression measurements.
        impression_edp_combinations = \
          parent_metric_report.get_whole_campaign_impression_edp_combinations().intersection(
              child_metric_report.get_whole_campaign_impression_edp_combinations())
        if edp_combination in impression_edp_combinations:
          if not fuzzy_less_equal(
              child_metric_report.get_whole_campaign_impression_measurement(
                  edp_combination).value,
              parent_metric_report.get_whole_campaign_impression_measurement(
                  edp_combination).value,
              CONSISTENCY_TEST_TOLERANCE):
            return False

    return True

  def _validate_independence_checks(
      self, edp_combination: EdpCombination
  ) -> bool:
    """Verifies if the observed union measurements for the edp_combination meet
    the independence check.
    """
    edps = [frozenset({element}) for element in edp_combination]

    for metric in self._metric_reports.keys():
      metric_report = self._metric_reports[metric]

      # Check if the whole campaign measurements follow the CI model.
      if edp_combination in metric_report.get_whole_campaign_reach_edp_combinations():
        union_measurement = metric_report.get_whole_campaign_reach_measurement(
            edp_combination)
        component_measurements = [
            metric_report.get_whole_campaign_reach_measurement(single_edp)
            for single_edp in edps
        ]

        if not is_union_reach_consistent(
            union_measurement, component_measurements, self._population_size):
          return False

      # Check if the cumulative measurements follow the CI model.
      if edp_combination in metric_report.get_weekly_cumulative_reach_edp_combinations():
        for period in range(self._num_periods):
          union_measurement = metric_report.get_weekly_cumulative_reach_measurement(
              edp_combination, period)
          component_measurements = [
              metric_report.get_weekly_cumulative_reach_measurement(
                  single_edp, period)
              for single_edp in edps
          ]

          if not is_union_reach_consistent(
              union_measurement, component_measurements, self._population_size):
            return False

    return True

  def _get_zero_variance_edps(self) -> list[EdpCombination]:
    """Get the zero variance EDPs.

    Returns:
       A list of zero variance EDPs.
    """
    zero_variance_edp_combinations: list[EdpCombination] = []
    if not self._metric_reports.keys():
      raise ValueError("The report does not contain any measurements.")

    edp_combinations = self._metric_reports[
      next(iter(self._metric_reports.keys()))].get_weekly_cumulative_reach_edp_combinations()

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
    zero_variance_edp_combinations = self._get_zero_variance_edps()
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
      ].get_whole_campaign_reach_edp_combinations()
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
