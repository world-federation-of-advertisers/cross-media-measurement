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

from dataclasses import dataclass

from noiseninja.noised_measurements import Measurement
from noiseninja.noised_measurements import MeasurementSet


# Uncapped impression results do not have a strict per-user maximum. This is a
# correction-model assumption about the largest plausible average frequency of
# a threshold-suppressed result, not an enforced contribution bound.
_MAXIMUM_EXPECTED_AVERAGE_FREQUENCY = 127


@dataclass(frozen=True)
class PotentialDirectResultMinimumThresholds:
    """Adds correction uncertainty for potentially suppressed Direct zeros.

    The fields are conservative upper bounds across every threshold policy
    that could have produced a result processed by this instance. They are not
    necessarily the exact current policy of any one EDP. Thresholds are treated
    as correction scales only when the reported standard deviation is zero.
    A nonzero standard deviation is assumed to represent a DP-only result and
    is preserved unchanged. Impression uncertainty assumes that a suppressed
    result's average frequency does not exceed [_MAXIMUM_EXPECTED_AVERAGE_FREQUENCY].

    Operators must follow the rollout contract documented in the Reporting V2
    deployment guide so these bounds cannot be lower than a producer's policy.
    """

    min_users: int
    min_impressions: int

    def __post_init__(self):
        if self.min_users <= 0:
            raise ValueError("min_users must be greater than 0.")
        if self.min_impressions <= 0:
            raise ValueError("min_impressions must be greater than 0.")

    def get_uncertainty_applications(
        self, measurement_set: MeasurementSet
    ) -> tuple[str, ...]:
        """Returns the measurement fields whose uncertainty will be changed."""
        applications = []
        if self._should_add_uncertainty(measurement_set.reach):
            applications.append("REACH")

        frequency = self._frequency_uncertainty_target(measurement_set.k_reach)
        if frequency is not None:
            applications.append(f"FREQUENCY_BIN_{frequency}")

        if self._should_add_uncertainty(measurement_set.impression):
            applications.append("IMPRESSION_COUNT")
        return tuple(applications)

    def reach_uncertainty_applies(self, measurement: Measurement) -> bool:
        """Returns whether reach uncertainty will be changed."""
        return self._should_add_uncertainty(measurement)

    def add_reach_uncertainty(self, measurement: Measurement) -> Measurement:
        """Adds uncertainty for either threshold that can suppress reach."""
        return self._add_uncertainty(
            measurement, max(self.min_users, self.min_impressions)
        )

    def add_measurement_set_uncertainty(
        self, measurement_set: MeasurementSet
    ) -> MeasurementSet:
        """Adds potential thresholding uncertainty to a measurement set."""
        reach = measurement_set.reach
        if reach is not None:
            reach = self.add_reach_uncertainty(reach)

        k_reach = self._add_frequency_uncertainty(measurement_set.k_reach)

        impression = measurement_set.impression
        if impression is not None:
            impression = self._add_uncertainty(
                impression,
                max(
                    self.min_impressions,
                    self.min_users * _MAXIMUM_EXPECTED_AVERAGE_FREQUENCY,
                ),
            )

        return MeasurementSet(
            reach=reach,
            k_reach=k_reach,
            impression=impression,
        )

    def _add_frequency_uncertainty(
        self, k_reach: dict[int, Measurement]
    ) -> dict[int, Measurement]:
        k_reach = dict(k_reach)
        first_frequency = self._frequency_uncertainty_target(k_reach)
        if first_frequency is not None:
            k_reach[first_frequency] = self._add_uncertainty(
                k_reach[first_frequency],
                max(self.min_users, self.min_impressions),
            )
        return k_reach

    @classmethod
    def _frequency_uncertainty_target(
        cls, k_reach: dict[int, Measurement]
    ) -> int | None:
        # TODO(world-federation-of-advertisers/cross-media-measurement#4464):
        # Model partial fold-down if correction is expected to reconstruct the
        # pre-threshold histogram rather than preserve displayed values.
        if k_reach and all(
            measurement.value == 0 for measurement in k_reach.values()
        ):
            # Fold-down reaches the lowest bin before suppression. Relax only
            # that bin so reach can be restored without inventing
            # higher-frequency users.
            first_frequency = min(k_reach)
            if cls._should_add_uncertainty(k_reach[first_frequency]):
                return first_frequency
        return None

    @staticmethod
    def _should_add_uncertainty(measurement: Measurement | None) -> bool:
        return (
            measurement is not None
            and measurement.value == 0
            and measurement.sigma == 0
        )

    @staticmethod
    def _add_uncertainty(
        measurement: Measurement, threshold: int
    ) -> Measurement:
        # TODO(world-federation-of-advertisers/cross-media-measurement#4466):
        # Model a Direct result that uses DP noise and minimum thresholding.
        if not PotentialDirectResultMinimumThresholds._should_add_uncertainty(
            measurement
        ):
            return measurement
        return Measurement(
            measurement.value,
            threshold,
            measurement.name,
        )
