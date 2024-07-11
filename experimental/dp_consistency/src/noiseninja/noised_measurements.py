from collections import defaultdict


class Measurement:
    """Represents a measurement with a mean value and a standard deviation (sigma)"""
    value: float
    sigma: float

    def __init__(self, value: float, sigma: float):
        self.value = value
        self.sigma = sigma

    def __repr__(self):
        return 'Measurement({:.2f}, {:.2f})'.format(self.value, self.sigma)


class SetMeasurementsSpec:
    subsets_by_set: dict[int, list[int]]
    # https://en.wikipedia.org/wiki/Cover_(topology)
    covers_by_set: dict[int, list[list[int]]]
    measurements_by_set: dict[int, list[Measurement]]

    def __init__(self):
        self.subsets_by_set = defaultdict(list[int])
        self.covers_by_set = defaultdict(list[list[int]])
        self.measurements_by_set = defaultdict(list[Measurement])

    def add_subset_relation(self, parent: int, child: int):
        self.subsets_by_set[parent].append(child)

    def add_cover(self, parent: int, children: list[int]):
        self.covers_by_set[parent].append(children)
        for child in children:
            self.add_subset_relation(parent, child)

    def add_measurement(self, measusured_set: int, measurement: Measurement):
        self.measurements_by_set[measusured_set].append(measurement)

    def all_sets(self) -> set[int]:
        return set(i for i in self.measurements_by_set.keys())

    def __repr__(self):
        return 'SetMeasurementsSpec(subsets_by_set={},covers_by_set={},measurements_by_set={})'.format(
            self.subsets_by_set, self.covers_by_set, self.measurements_by_set)
