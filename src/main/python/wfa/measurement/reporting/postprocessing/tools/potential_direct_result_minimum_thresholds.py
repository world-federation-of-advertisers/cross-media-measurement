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

import math
from dataclasses import dataclass

from noiseninja.noised_measurements import Measurement
from noiseninja.noised_measurements import MeasurementSet


@dataclass(frozen=True)
class PotentialDirectResultMinimumThresholds:
    """Adds correction uncertainty for potentially suppressed Direct zeros.

    Thresholds are treated as standard-deviation components and combined with
    reported noise in quadrature.
    """

    min_users: int
    min_impressions: int

    def __post_init__(self):
        if self.min_users <= 0:
            raise ValueError("min_users must be greater than 0.")
        if self.min_impressions <= 0:
            raise ValueError("min_impressions must be greater than 0.")

    def add_reach_uncertainty(self, measurement: Measurement) -> Measurement:
        """Adds minimum-user uncertainty to a zero reach measurement."""
        return self._add_uncertainty(measurement, self.min_users)

    def add_measurement_set_uncertainty(
        self, measurement_set: MeasurementSet
    ) -> MeasurementSet:
        """Adds potential thresholding uncertainty to a measurement set."""
        reach = measurement_set.reach
        if reach is not None:
            reach = self.add_reach_uncertainty(reach)

        impression = measurement_set.impression
        if impression is not None:
            impression = self._add_uncertainty(
                impression, self.min_impressions
            )

        k_reach = dict(measurement_set.k_reach)
        if k_reach and all(
            measurement.value == 0 for measurement in k_reach.values()
        ):
            # Fold-down reaches the lowest bin before suppression. Relax only
            # that bin so reach can be restored without inventing
            # higher-frequency users.
            first_frequency = min(k_reach)
            k_reach[first_frequency] = self._add_uncertainty(
                k_reach[first_frequency],
                max(self.min_users, self.min_impressions),
            )

        return MeasurementSet(
            reach=reach,
            k_reach=k_reach,
            impression=impression,
        )

    @staticmethod
    def _add_uncertainty(
        measurement: Measurement, threshold: int
    ) -> Measurement:
        if measurement.value != 0:
            return measurement
        return Measurement(
            measurement.value,
            math.hypot(measurement.sigma, threshold),
            measurement.name,
        )
