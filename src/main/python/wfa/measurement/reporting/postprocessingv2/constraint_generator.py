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
from typing import List

from wfa.measurement.reporting.postprocessing.report.report_v2 import DataProviderMetricSetMap
from wfa.measurement.internal.reporting.postprocessing import constraint_pb2

Constraint = constraint_pb2.Constraints.Constraint

class ConstraintGenerator(ABC):
  """Abstract base class for constraint generators."""

  @abstractmethod
  def get_constraints(self) -> List[Constraint]:
    """Generates a list of constraints."""
    pass

class CompositeConstraintGenerator(ConstraintGenerator):
  """A generator that aggregates constraints from multiple sub-generators."""
  def __init__(self, subgenerators: List[ConstraintGenerator]):
    self.subgenerators = subgenerators

  def get_constraints(self) -> List[Constraint]:
    constraints = []
    for generator in self.subgenerators:
      constraints.extend(generator.get_constraints())
    return constraints

# ---------- Constraints within a DataProviderMetricSetMap ---------- #

class LowerBoundRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring reach, k-reach, and impressions are
  non-negative.
  """
  def __init__(self, data_provider_metric_set: DataProviderMetricSetMap):
    self.data_provider_metric_set = data_provider_metric_set

  def get_constraints(self) -> List[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

class CoverRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring the sum of subset reaches covering a union
  set is >= union reach.
  """
  def __init__(self, data_provider_metric_set: DataProviderMetricSetMap):
    self.data_provider_metric_set = data_provider_metric_set

  def get_constraints(self) -> List[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

class ImpressionsSumRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring impressions of a union equal the sum of
  impressions of single sets.
  """
  def __init__(self, data_provider_metric_set: DataProviderMetricSetMap):
    self.data_provider_metric_set = data_provider_metric_set

  def get_constraints(self) -> List[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

class SubsetRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring subset reach/impression <= superset
  reach/impression.
  """
  def __init__(self, data_provider_metric_set: DataProviderMetricSetMap):
    self.data_provider_metric_set = data_provider_metric_set

  def get_constraints(self) -> List[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

class ReachFrequencyRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring reach equals the sum of frequencies."""
  def __init__(self, data_provider_metric_set: DataProviderMetricSetMap):
    self.data_provider_metric_set = data_provider_metric_set

  def get_constraints(self) -> List[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

class ReachImpressionsRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring reach <= impressions."""
  def __init__(self, data_provider_metric_set: DataProviderMetricSetMap):
    self.data_provider_metric_set = data_provider_metric_set

  def get_constraints(self) -> List[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

class FrequencyImpressionsRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring weighted sum of frequencies <= impressions."""
  def __init__(self, data_provider_metric_set: DataProviderMetricSetMap):
    self.data_provider_metric_set = data_provider_metric_set

  def get_constraints(self) -> List[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

# --- Constraints between two comparable DataProviderMetricSetMaps --- #

class EqualRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring child and parent contain identical
  measurements where applicable.

  E.g. Last cumulative week is the same as whole_campaign, first cumulative week
  is the same as first non-cumulative week, etc.
  """
  def __init__(
    self,
    child: DataProviderMetricSetMap,
    parent: DataProviderMetricSetMap
  ):
    self.child = child
    self.parent = parent

  def get_constraints(self) -> List[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

class OverlapRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring Overlap(child) <= Overlap(parent)."""
  def __init__(
    self,
    child: DataProviderMetricSetMap,
    parent: DataProviderMetricSetMap
  ):
    self.child = child
    self.parent = parent

  def get_constraints(self) -> List[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

class ReachRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring child reach <= parent reach."""
  def __init__(
    self,
    child: DataProviderMetricSetMap,
    parent: DataProviderMetricSetMap
  ):
    self.child = child
    self.parent = parent

  def get_constraints(self) -> List[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

class ImpressionsRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring child impressions <= parent impressions."""
  def __init__(
    self,
    child: DataProviderMetricSetMap,
    parent: DataProviderMetricSetMap
  ):
    self.child = child
    self.parent = parent

  def get_constraints(self) -> List[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )

class FrequencyRelationGenerator(ConstraintGenerator):
  """Generates constraints ensuring child k+ reach <= parent k+ reach."""
  def __init__(
    self,
    child: DataProviderMetricSetMap,
    parent: DataProviderMetricSetMap
  ):
    self.child = child
    self.parent = parent

  def get_constraints(self) -> List[Constraint]:
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
    non_cumulatives: List[DataProviderMetricSetMap],
    cumulatives: List[DataProviderMetricSetMap],
  ):
    self.non_cumulatives = non_cumulatives
    self.cumulatives = cumulatives

  def get_constraints(self) -> List[Constraint]:
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
    non_cumulatives: List[DataProviderMetricSetMap],
    whole_campaign: DataProviderMetricSetMap,
  ):
    self.non_cumulatives = non_cumulatives
    self.whole_campaign = whole_campaign

  def get_constraints(self) -> List[Constraint]:
    # TODO(@ple13): Implement constraint generator.
    raise NotImplementedError(
      f"get_constraints for {self.__class__.__name__} not implemented yet."
    )
