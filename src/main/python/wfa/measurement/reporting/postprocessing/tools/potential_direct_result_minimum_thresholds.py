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

    The fields are conservative upper bounds across every threshold policy
    that could have produced a result processed by this instance. They are not
    necessarily the exact current policy of any one EDP. Thresholds are treated
    as standard-deviation components and combined with reported noise in
    quadrature. The maximum frequency bounds impressions hidden when the
    minimum-user gate suppresses a capped impression result.

    Operators must follow the rollout contract documented in the Reporting V2
    deployment guide so these bounds cannot be lower than a producer's policy.
    """

    min_users: int
    min_impressions: int
    maximum_frequency_per_user: int
    applies_to_multi_publisher_results: bool

    def __post_init__(self):
        if self.min_users <= 0:
            raise ValueError("min_users must be greater than 0.")
        if self.min_impressions <= 0:
            raise ValueError("min_impressions must be greater than 0.")
        if self.maximum_frequency_per_user <= 0:
            raise ValueError(
                "maximum_frequency_per_user must be greater than 0."
            )

    def add_reach_uncertainty(self, measurement: Measurement) -> Measurement:
        """Adds uncertainty for either threshold that can suppress reach."""
        return self._add_uncertainty(
            measurement, max(self.min_users, self.min_impressions)
        )

    def add_reach_and_frequency_uncertainty(
        self, measurement_set: MeasurementSet
    ) -> MeasurementSet:
        """Adds potential thresholding uncertainty to reach and frequency."""
        reach = measurement_set.reach
        if reach is not None:
            reach = self.add_reach_uncertainty(reach)

        k_reach = self._add_frequency_uncertainty(measurement_set.k_reach)
        return MeasurementSet(
            reach=reach,
            k_reach=k_reach,
            impression=measurement_set.impression,
        )

    def add_measurement_set_uncertainty(
        self, measurement_set: MeasurementSet
    ) -> MeasurementSet:
        """Adds potential thresholding uncertainty to a measurement set."""
        reach_and_frequency = self.add_reach_and_frequency_uncertainty(
            measurement_set
        )

        impression = measurement_set.impression
        if impression is not None:
            impression = self._add_uncertainty(
                impression,
                max(
                    self.min_impressions,
                    self.min_users * self.maximum_frequency_per_user,
                ),
            )

        return MeasurementSet(
            reach=reach_and_frequency.reach,
            k_reach=reach_and_frequency.k_reach,
            impression=impression,
        )

    def _add_frequency_uncertainty(
        self, k_reach: dict[int, Measurement]
    ) -> dict[int, Measurement]:
        k_reach = dict(k_reach)
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

        return k_reach

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
