# Copyright 2026 The Cross-Media Measurement Authors
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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from itertools import combinations

from src.main.python.wfa.measurement.reporting.postprocessingv2.report import (
  DataProviderMetricSetMap,
)
from wfa.measurement.internal.reporting.postprocessing import constraint_pb2

Constraint = constraint_pb2.Constraints.Constraint
ConstraintType = constraint_pb2.Constraints.ConstraintType


@dataclass
class CoverRelationship:
  target_set: frozenset[str]
  subset_cover: list[frozenset[str]]


def get_minimal_cover_relationships(
  edp_combinations: list[frozenset[str]],
) -> list[CoverRelationship]:
  """Computes minimal subset covers for each set in the input list.

  A cover is minimal if no proper subset of the cover is also a cover.

  Returns:
    A list of CoverRelationship objects.
  """
  all_covers = []
  for target_set in edp_combinations:
    if len(target_set) <= 1:
      continue

    other_sets = [
      edp_combination
      for edp_combination in edp_combinations
      if edp_combination != target_set and edp_combination.issubset(target_set)
    ]

    current_target_covers = []
    # Find all covers.
    for combination_size in range(2, len(other_sets) + 1):
      for combo in combinations(other_sets, combination_size):
        union = set().union(*combo)
        if union == target_set:
          current_target_covers.append(set(combo))

    # Filter for minimal covers.
    for cover in current_target_covers:
      is_minimal = True
      cover_list = list(cover)
      for i in range(len(cover_list)):
        # Try removing one item and check if it's still a cover.
        test_cover = cover_list[:i] + cover_list[i + 1:]
        if set().union(*test_cover) == target_set:
          is_minimal = False
          break
      if is_minimal:
        all_covers.append(
          CoverRelationship(target_set=target_set, subset_cover=cover_list)
        )

  return all_covers


class ConstraintGenerator(ABC):
  """Abstract base class for constraint generators."""

  def __init__(self, num_metric_sets: int, max_frequency: int):
    self.num_metric_sets = num_metric_sets
    self.max_frequency = max_frequency

  def _validate_metric_set(self, metric_set: "MetricSet"):
    """Validates that a metric set is valid.

    For each metric in the metric set:
      - the value and sigma are non-negative
      - the index is within [0, num_metric_sets)
      - k-reach is either empty or has length of max_frequency
    """
    if not metric_set:
      raise ValueError("Metric set cannot be None.")

    if not (metric_set.reach or metric_set.impression or metric_set.k_reach):
      raise ValueError(
        "Metric set must have at least one of reach, impression, or k-reach.")

    metrics = []
    if metric_set.reach:
      metrics.append(metric_set.reach)
    if metric_set.impression:
      metrics.append(metric_set.impression)
    for k_reach_metric in metric_set.k_reach.values():
      metrics.append(k_reach_metric)

    for metric in metrics:
      if metric.value < 0:
        raise ValueError(f"Metric value {metric.value} must be non-negative.")
      if metric.sigma < 0:
        raise ValueError(f"Metric sigma {metric.sigma} must be non-negative.")
      if not (0 <= metric.index < self.num_metric_sets):
        raise ValueError(
          f"Metric index {metric.index} must be in [0, {self.num_metric_sets})."
        )

    if metric_set.k_reach and len(metric_set.k_reach) != self.max_frequency:
      raise ValueError(
        f"K-reach length {len(metric_set.k_reach)} must be {self.max_frequency}."
      )

  def _validate_data_provider_metric_set_map(
    self, data_provider_metric_set: DataProviderMetricSetMap
  ):
    """Validates that all metric sets in the map are valid."""
    if not data_provider_metric_set:
      raise ValueError("Data provider metric set cannot be None.")

    for metric_set in data_provider_metric_set.values():
      self._validate_metric_set(metric_set)

  @abstractmethod
  def get_constraints(self) -> list[Constraint]:
    """Generates constraints."""


class LowerBoundRelationGenerator(ConstraintGenerator):
  """Generates lower bound constraints for each metric."""

  def __init__(
    self,
    num_metric_sets: int,
    max_frequency: int,
    data_provider_metric_set: DataProviderMetricSetMap,
  ):
    super().__init__(num_metric_sets, max_frequency)
    self._validate_data_provider_metric_set_map(data_provider_metric_set)
    self.data_provider_metric_set = data_provider_metric_set

  def get_constraints(self) -> list[Constraint]:
    constraints = []
    for metric_set in self.data_provider_metric_set.values():
      indices = []
      if metric_set.reach:
        indices.append(metric_set.reach.index)
      if metric_set.impression:
        indices.append(metric_set.impression.index)
      for k_reach_metric in metric_set.k_reach.values():
        indices.append(k_reach_metric.index)

      for index in indices:
        constraints.append(
          Constraint(
            coefficients={index: 1},
            type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
            constant=0,
          )
        )
    return constraints


class UnnoisedRelationGenerator(ConstraintGenerator):
  """Generates constraints for unnoised metrics (sigma=0)."""

  def __init__(
    self,
    num_metric_sets: int,
    max_frequency: int,
    data_provider_metric_set: DataProviderMetricSetMap,
  ):
    super().__init__(num_metric_sets, max_frequency)
    self._validate_data_provider_metric_set_map(data_provider_metric_set)
    self.data_provider_metric_set = data_provider_metric_set

  def get_constraints(self) -> list[Constraint]:
    constraints = []
    for metric_set in self.data_provider_metric_set.values():
      metrics = []
      if metric_set.reach:
        metrics.append(metric_set.reach)
      if metric_set.impression:
        metrics.append(metric_set.impression)
      for k_reach_metric in metric_set.k_reach.values():
        metrics.append(k_reach_metric)

      for metric in metrics:
        if metric.sigma == 0:
          constraints.append(
            Constraint(
              coefficients={metric.index: 1},
              type=ConstraintType.CONSTRAINT_TYPE_EQUAL,
              constant=int(metric.value),
            )
          )
    return constraints


class CoverRelationGenerator(ConstraintGenerator):
  """Generates constraints based on subset cover relationships."""

  def __init__(
    self,
    num_metric_sets: int,
    max_frequency: int,
    data_provider_metric_set: DataProviderMetricSetMap,
    cover_relationships: list[CoverRelationship],
  ):
    super().__init__(num_metric_sets, max_frequency)
    self._validate_data_provider_metric_set_map(data_provider_metric_set)
    self.data_provider_metric_set = data_provider_metric_set
    self.cover_relationships = cover_relationships

  def get_constraints(self) -> list[Constraint]:
    constraints = []
    for relationship in self.cover_relationships:
      target_metric_set = self.data_provider_metric_set.get(
        relationship.target_set
      )
      if not target_metric_set or not target_metric_set.reach:
        continue

      subset_metric_sets = [
        self.data_provider_metric_set.get(subset_key)
        for subset_key in relationship.subset_cover
      ]
      if any(
        not subset_metric_set or not subset_metric_set.reach
        for subset_metric_set in subset_metric_sets
      ):
        continue

      coefficients = {target_metric_set.reach.index: -1}
      for subset_metric_set in subset_metric_sets:
        coefficients[subset_metric_set.reach.index] = 1

      constraints.append(
        Constraint(
          coefficients=coefficients,
          type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
          constant=0,
        )
      )
    return constraints


class ImpressionsSumRelationGenerator(ConstraintGenerator):
  """Generates constraints based on impression sums."""

  def __init__(
    self,
    num_metric_sets: int,
    max_frequency: int,
    data_provider_metric_set: DataProviderMetricSetMap,
  ):
    super().__init__(num_metric_sets, max_frequency)
    self._validate_data_provider_metric_set_map(data_provider_metric_set)
    self.data_provider_metric_set = data_provider_metric_set

  def get_constraints(self) -> list[Constraint]:
    constraints = []
    edp_combinations = [
      edp_combination
      for edp_combination in self.data_provider_metric_set.keys()
      if len(edp_combination) > 1
    ]

    for edp_combination in edp_combinations:
      components = [
        frozenset([edp]) for edp in edp_combination
      ]
      if not all(
        component in self.data_provider_metric_set for component in components
      ):
        continue

      target_metric_set = self.data_provider_metric_set[edp_combination]
      if not target_metric_set.impression:
        continue

      component_metric_sets = [
        self.data_provider_metric_set[component] for component in components
      ]
      if any(
        not metric_set.impression for metric_set in component_metric_sets
      ):
        continue

      coefficients = {target_metric_set.impression.index: 1}
      for metric_set in component_metric_sets:
        coefficients[metric_set.impression.index] = -1

      constraints.append(
        Constraint(
          coefficients=coefficients,
          type=ConstraintType.CONSTRAINT_TYPE_EQUAL,
          constant=0,
        )
      )
    return constraints


class SubsetRelationGenerator(ConstraintGenerator):
  """Generates reaching and impression subset constraints."""

  def __init__(
    self,
    num_metric_sets: int,
    max_frequency: int,
    data_provider_metric_set: DataProviderMetricSetMap,
  ):
    super().__init__(num_metric_sets, max_frequency)
    self._validate_data_provider_metric_set_map(data_provider_metric_set)
    self.data_provider_metric_set = data_provider_metric_set

  def get_constraints(self) -> list[Constraint]:
    constraints = []
    edp_combinations = list(self.data_provider_metric_set.keys())
    for set1, set2 in combinations(edp_combinations, 2):
      if set1.issubset(set2) or set2.issubset(set1):
        child = min(set1, set2, key=len)
        parent = max(set1, set2, key=len)
      else:
        continue

      parent_metric_set = self.data_provider_metric_set[parent]
      child_metric_set = self.data_provider_metric_set[child]

      # Child reach <= parent reach.
      if parent_metric_set.reach and child_metric_set.reach:
        constraints.append(
          Constraint(
            coefficients={
              parent_metric_set.reach.index: 1,
              child_metric_set.reach.index: -1,
            },
            type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
            constant=0,
          )
        )
      
      # Child k+ reach <= parent k+ reach.
      if parent_metric_set.k_reach and child_metric_set.k_reach:
        for k in range(1, self.max_frequency + 1):
          coefficients = {}
          for frequency in range(k, self.max_frequency + 1):
            if frequency in parent_metric_set.k_reach:
              coefficients[parent_metric_set.k_reach[frequency].index] = 1
            if frequency in child_metric_set.k_reach:
              coefficients[child_metric_set.k_reach[frequency].index] = -1
          if coefficients:
            constraints.append(
              Constraint(
                coefficients=coefficients,
                type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
                constant=0,
              )
            )
            
      # Child impression <= parent impression.
      if parent_metric_set.impression and child_metric_set.impression:
        constraints.append(
          Constraint(
            coefficients={
              parent_metric_set.impression.index: 1,
              child_metric_set.impression.index: -1,
            },
            type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
            constant=0,
          )
        )
    return constraints


class ReachFrequencyRelationGenerator(ConstraintGenerator):
  """Generates reach = sum(k-reach) constraints."""

  def __init__(
    self,
    num_metric_sets: int,
    max_frequency: int,
    data_provider_metric_set: DataProviderMetricSetMap,
  ):
    super().__init__(num_metric_sets, max_frequency)
    self._validate_data_provider_metric_set_map(data_provider_metric_set)
    self.data_provider_metric_set = data_provider_metric_set

  def get_constraints(self) -> list[Constraint]:
    constraints = []
    for metric_set in self.data_provider_metric_set.values():
      if not metric_set.reach or not metric_set.k_reach:
        continue

      coefficients = {metric_set.reach.index: 1}
      for k_reach_metric in metric_set.k_reach.values():
        coefficients[k_reach_metric.index] = -1

      constraints.append(
        Constraint(
          coefficients=coefficients,
          type=ConstraintType.CONSTRAINT_TYPE_EQUAL,
          constant=0,
        )
      )
    return constraints


class ReachImpressionsRelationGenerator(ConstraintGenerator):
  """Generates reach <= impressions constraints."""

  def __init__(
    self,
    num_metric_sets: int,
    max_frequency: int,
    data_provider_metric_set: DataProviderMetricSetMap,
  ):
    super().__init__(num_metric_sets, max_frequency)
    self._validate_data_provider_metric_set_map(data_provider_metric_set)
    self.data_provider_metric_set = data_provider_metric_set

  def get_constraints(self) -> list[Constraint]:
    constraints = []
    for metric_set in self.data_provider_metric_set.values():
      if not metric_set.reach or not metric_set.impression:
        continue

      constraints.append(
        Constraint(
          coefficients={
            metric_set.impression.index: 1,
            metric_set.reach.index: -1,
          },
          type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
          constant=0,
        )
      )
    return constraints


class FrequencyImpressionsRelationGenerator(ConstraintGenerator):
  """Generates sum(k * k-reach) <= impressions constraints."""

  def __init__(
    self,
    num_metric_sets: int,
    max_frequency: int,
    data_provider_metric_set: DataProviderMetricSetMap,
  ):
    super().__init__(num_metric_sets, max_frequency)
    self._validate_data_provider_metric_set_map(data_provider_metric_set)
    self.data_provider_metric_set = data_provider_metric_set

  def get_constraints(self) -> list[Constraint]:
    constraints = []
    for metric_set in self.data_provider_metric_set.values():
      if not metric_set.k_reach or not metric_set.impression:
        continue

      coefficients = {metric_set.impression.index: 1}
      for frequency, k_reach_metric in metric_set.k_reach.items():
        coefficients[k_reach_metric.index] = -frequency

      constraints.append(
        Constraint(
          coefficients=coefficients,
          type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
          constant=0,
        )
      )
    return constraints

# --- Constraints between two comparable DataProviderMetricSetMaps --- #

class EqualRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring child and parent contain identical
  measurements where applicable.

  E.g. Last cumulative week is the same as whole_campaign, first cumulative week
  is the same as first non-cumulative week, etc.
  """
  def __init__(
    self,
    num_metric_sets: int,
    max_frequency: int,
    child: DataProviderMetricSetMap,
    parent: DataProviderMetricSetMap
  ):
    super().__init__(num_metric_sets, max_frequency)
    self.child = child
    self.parent = parent

  def get_constraints(self) -> list[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

class OverlapRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring Overlap(child) <= Overlap(parent)."""
  def __init__(
    self,
    num_metric_sets: int,
    max_frequency: int,
    child: DataProviderMetricSetMap,
    parent: DataProviderMetricSetMap
  ):
    super().__init__(num_metric_sets, max_frequency)
    self.child = child
    self.parent = parent

  def get_constraints(self) -> list[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

class ReachRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring child reach <= parent reach."""
  def __init__(
    self,
    num_metric_sets: int,
    max_frequency: int,
    child: DataProviderMetricSetMap,
    parent: DataProviderMetricSetMap
  ):
    super().__init__(num_metric_sets, max_frequency)
    self.child = child
    self.parent = parent

  def get_constraints(self) -> list[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

class ImpressionsRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring child impressions <= parent impressions."""
  def __init__(
    self,
    num_metric_sets: int,
    max_frequency: int,
    child: DataProviderMetricSetMap,
    parent: DataProviderMetricSetMap
  ):
    super().__init__(num_metric_sets, max_frequency)
    self.child = child
    self.parent = parent

  def get_constraints(self) -> list[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

class FrequencyRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring child k+ reach <= parent k+ reach."""
  def __init__(
    self,
    num_metric_sets: int,
    max_frequency: int,
    child: DataProviderMetricSetMap,
    parent: DataProviderMetricSetMap
  ):
    super().__init__(num_metric_sets, max_frequency)
    self.child = child
    self.parent = parent

  def get_constraints(self) -> list[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

# ---------- Constraints among a list of DataProviderMetricSetMaps -- #

class CumulativeAndNonCumulativeConstraints(ConstraintGenerator):
  """
  Generates constraints relating cumulative and non-cumulative measurements.
  For week i:
    - Each non-cumulative reach at week i <= cumulative reach at week i.
    - Sum of non-cumulative reaches of the first i weeks >= cumulative reach at
      week i.
    - Sum of non-cumulative impressions of the first i weeks = cumulative
      impressions at week i.
  """
  def __init__(
    self,
    num_metric_sets: int,
    max_frequency: int,
    non_cumulatives: list[DataProviderMetricSetMap],
    cumulatives: list[DataProviderMetricSetMap],
  ):
    super().__init__(num_metric_sets, max_frequency)
    self.non_cumulatives = non_cumulatives
    self.cumulatives = cumulatives

  def get_constraints(self) -> list[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

class NonCumulativeAndWholeCampaignConstraints(ConstraintGenerator):
  """
  Generates constraints relating whole whole_campaign and non-cumulative
  measurements.
    - Each non-cumulative reach <= whole_campaign reach.
    - Sum of non-cumulative reaches >= whole_campaign reach reach.
    - Sum of non-cumulative impressions = whole_campaign impressions.
  """
  def __init__(
    self,
    num_metric_sets: int,
    max_frequency: int,
    non_cumulatives: list[DataProviderMetricSetMap],
    whole_campaign: DataProviderMetricSetMap,
  ):
    super().__init__(num_metric_sets, max_frequency)
    self.non_cumulatives = non_cumulatives
    self.whole_campaign = whole_campaign

  def get_constraints(self) -> list[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )
