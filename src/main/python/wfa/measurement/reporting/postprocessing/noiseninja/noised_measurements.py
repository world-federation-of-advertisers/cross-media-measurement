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

from collections import defaultdict


class Measurement:
  """Represents a measurement with a mean value and a standard deviation"""
  value: float
  sigma: float
  name: str

  def __init__(self, value: float, sigma: float, name: str):
    self.value = value
    self.sigma = sigma
    self.name = name

  def __repr__(self):
    return 'Measurement({:.2f}, {:.2f}, {})\n'.format(self.value, self.sigma,
                                                      self.name)


class SetMeasurementsSpec:
  """Stores information about relationships between sets and measurements.

    This class maintains data about subset relationships, cover relationships,
    and measurements associated with sets. It provides methods to add and
    retrieve this information.

    Attributes:
        _subsets_by_set: A dictionary mapping a set ID to a list of its subset
                         set IDs.
        _covers_by_set: A dictionary mapping a set ID to a list of its covers,
                        where each cover is a list of set IDs. See
                        https://en.wikipedia.org/wiki/Cover_(topology).
        _measurements_by_set: A dictionary mapping a set ID to a list of
                              Measurement objects associated with that set.
    """

  def __init__(self):
    self._subsets_by_set = defaultdict(list[int])
    self._covers_by_set = defaultdict(list[list[int]])
    self._measurements_by_set = defaultdict(list[Measurement])

  def add_subset_relation(self, parent_set_id: int, child_set_id: int):
    self._subsets_by_set[parent_set_id].append(child_set_id)

  def add_cover(self, parent: int, children: list[int]):
    self._covers_by_set[parent].append(children)

  def add_measurement(self, set_id: int, measurement: Measurement):
    self._measurements_by_set[set_id].append(measurement)

  def all_sets(self) -> set[int]:
    return set(i for i in self._measurements_by_set.keys())

  def get_covers_of_set(self, set_id: int):
    return self._covers_by_set[set_id]

  def get_subsets(self, parent_set_id):
    return self._subsets_by_set[parent_set_id]

  def get_measurements(self, measured_set_id):
    return self._measurements_by_set.get(measured_set_id)

  def get_measurement_metric(self, measured_set_id):
    measurement = self._measurements_by_set.get(measured_set_id)
    return measurement[0].name

  def __repr__(self):
    return (('SetMeasurementsSpec('
             'subsets_by_set={},covers_by_set={},measurements_by_set={})')
            .format(self._subsets_by_set, self._covers_by_set,
                    self._measurements_by_set))
