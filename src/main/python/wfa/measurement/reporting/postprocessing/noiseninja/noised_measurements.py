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

from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field
from typing import Optional
from typing import TypeAlias

DEFAULT_ABSOLUTE_TOLERANCE = 1e-1

class Measurement:
  """Represents a measurement with a mean value and a standard deviation"""
  value: float
  sigma: float
  name: str

  def __init__(self, value: float, sigma: float, name: str):
    self.value = value
    self.sigma = sigma
    self.name = name

  def __eq__(self, other):
   """Returns True if two Measurement objects are equal.

   Uses math.isclose for float comparisons to handle minor precision
   differences.
   """
   if not isinstance(other, Measurement):
     raise ValueError("Can not compare Measurement with non-Measurement.")
   return (
       math.isclose(self.value, other.value, abs_tol=DEFAULT_ABSOLUTE_TOLERANCE)
       and math.isclose(self.sigma, other.sigma, abs_tol=DEFAULT_ABSOLUTE_TOLERANCE)
       and self.name == other.name
   )

  def __repr__(self):
    return 'Measurement({:.2f}, {:.2f}, {})\n'.format(self.value, self.sigma,
                                                      self.name)


KReachMeasurements: TypeAlias = dict[int, Measurement]


@dataclass
class MeasurementSet:
  reach: Optional[Measurement] = None
  k_reach: KReachMeasurements = field(default_factory=dict)
  impression: Optional[Measurement] = None

  def __post_init__(self):
    """
    Validates the object's state after initialization.
    """
    # At least one of the field is not empty.
    if self.reach is None and self.impression is None and not self.k_reach:
      raise ValueError(
          "At least one field (reach, impression, or k_reach) must be "
          "initialized."
      )

  def __eq__(self, other):
    """Returns True if two MeasurementSet objects are equal."""
    if not isinstance(other, MeasurementSet):
      raise ValueError(
          "Can not compare MeasurementSet with non-MeasurementSet."
      )
    return (
      self.reach == other.reach
      and self.k_reach == other.k_reach
      and self.impression == other.impression
    )

  def __repr__(self):
    return (
      f"MeasurementSet({self.reach=}, {self.k_reach=}, {self.impression=})"
    )



@dataclass
class OrderedSets:
  """Represents an ordered pair of sets.

    The sum of the measurements in larger set is greater or equal to that of
    the smaller set
  """
  larger_set: set[int]
  smaller_set: set[int]


class SetMeasurementsSpec:
  """Stores information about relationships between sets and measurements.

    This class maintains data about subset relationships, cover relationships,
    and measurements associated with sets. It provides methods to add and
    retrieve this information.

    Attributes:
        _equal_sets: A list of tuples, each containing two set IDs that have
                     equal measurement values.
        _subsets_by_set: A dictionary mapping a set ID to a list of its subset
                         set IDs.
        _covers_by_set: A dictionary mapping a set ID to a list of its covers,
                        where each cover is a list of set IDs. See
                        https://en.wikipedia.org/wiki/Cover_(topology).
        _measurements_by_set: A dictionary mapping a set ID to a list of
                              Measurement objects associated with that set.
  """

  def __init__(self):
    self._equal_sets = []
    self._ordered_sets = []
    self._weighted_sum_upperbound_sets = defaultdict(list[list[int]])
    self._subsets_by_set = defaultdict(list[int])
    self._covers_by_set = defaultdict(list[list[int]])
    self._measurements_by_set = defaultdict(list[Measurement])

  def add_equal_relation(self, set_id_one: int, set_id_two: list[int]):
    self._equal_sets.append([set_id_one, set_id_two])

  def add_weighted_sum_upperbound_relation(self, upperbound_id: int,
      weighted_id_set: list[list[int]]):
    self._weighted_sum_upperbound_sets[upperbound_id] = weighted_id_set

  def add_subset_relation(self, parent_set_id: int, child_set_id: int):
    self._subsets_by_set[parent_set_id].append(child_set_id)

  def add_ordered_sets_relation(self, ordered_set: OrderedSets):
    self._ordered_sets.append(ordered_set)

  def add_cover(self, parent: int, children: list[int]):
    self._covers_by_set[parent].append(children)

  def add_measurement(self, set_id: int, measurement: Measurement):
    self._measurements_by_set[set_id].append(measurement)

  def all_sets(self) -> set[int]:
    return set(i for i in self._measurements_by_set.keys())

  def get_equal_sets(self):
    return self._equal_sets

  def get_weighted_sum_upperbound_sets(self):
    return self._weighted_sum_upperbound_sets

  def get_covers_of_set(self, set_id: int) -> list[list[int]]:
    return self._covers_by_set[set_id]

  def get_subsets(self, parent_set_id: int) -> list[int]:
    return self._subsets_by_set[parent_set_id]

  def get_ordered_sets(self):
    return self._ordered_sets

  def get_measurements(self, measured_set_id: int) -> list[Measurement]:
    return self._measurements_by_set.get(measured_set_id)

  def get_measurement_metric(self, measured_set_id: int) -> str:
    measurement = self._measurements_by_set.get(measured_set_id)
    return measurement[0].name

  def __repr__(self):
    return (('SetMeasurementsSpec('
             'subsets_by_set={},covers_by_set={},measurements_by_set={})')
            .format(self._subsets_by_set, self._covers_by_set,
                    self._measurements_by_set))
