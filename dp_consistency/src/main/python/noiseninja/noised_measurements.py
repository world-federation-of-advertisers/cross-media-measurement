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

    def __init__(self, value: float, sigma: float):
        self.value = value
        self.sigma = sigma

    def __repr__(self):
        return 'Measurement({:.2f}, {:.2f})'.format(self.value, self.sigma)


class SetMeasurementsSpec:
    """Stores information about the relationships between sets and their
    measurements."""

    __subsets_by_set: dict[int, list[int]]
    # https://en.wikipedia.org/wiki/Cover_(topology)
    __covers_by_set: dict[int, list[list[int]]]
    __measurements_by_set: dict[int, list[Measurement]]

    def __init__(self):
        self.__subsets_by_set = defaultdict(list[int])
        self.__covers_by_set = defaultdict(list[list[int]])
        self.__measurements_by_set = defaultdict(list[Measurement])

    def add_subset_relation(self, parent_set_id: int, child_set_id: int):
        self.__subsets_by_set[parent_set_id].append(child_set_id)

    def add_cover(self, parent: int, children: list[int]):
        self.__covers_by_set[parent].append(children)
        for child in children:
            self.add_subset_relation(parent, child)

    def add_measurement(self, set_id: int, measurement: Measurement):
        self.__measurements_by_set[set_id].append(measurement)

    def all_sets(self) -> set[int]:
        return set(i for i in self.__measurements_by_set.keys())

    def get_covers_of_set(self, set_id: int):
        return self.__covers_by_set[set_id]

    def get_subsets(self, parent_set_id):
        return self.__subsets_by_set[parent_set_id]

    def get_measurements(self, measured_set_id):
        return self.__measurements_by_set.get(measured_set_id)

    def __repr__(self):
        return (('SetMeasurementsSpec('
                 'subsets_by_set={},covers_by_set={},measurements_by_set={})')
                .format(self.__subsets_by_set, self.__covers_by_set,
                        self.__measurements_by_set))
