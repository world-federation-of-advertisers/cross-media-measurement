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

import unittest

from noiseninja.noised_measurements import Measurement, MeasurementSet
from tools.potential_direct_result_minimum_thresholds import (
    PotentialDirectResultMinimumThresholds,
)


class PotentialDirectResultMinimumThresholdsTest(unittest.TestCase):

    def test_add_reach_uncertainty_preserves_dp_noise(self):
        thresholds = PotentialDirectResultMinimumThresholds(
            min_users=100,
            min_impressions=1000,
        )
        measurement = Measurement(value=0, sigma=30, name="reach")

        result = thresholds.add_reach_uncertainty(measurement)

        self.assertEqual(result, measurement)

    def test_add_measurement_set_uncertainty_updates_threshold_only_zeros(self):
        thresholds = PotentialDirectResultMinimumThresholds(
            min_users=100,
            min_impressions=1000,
        )

        result = thresholds.add_measurement_set_uncertainty(
            MeasurementSet(
                reach=Measurement(value=0, sigma=0, name="reach"),
                k_reach={
                    1: Measurement(value=0, sigma=0, name="frequency-1"),
                    2: Measurement(value=0, sigma=0, name="frequency-2"),
                },
                impression=Measurement(value=0, sigma=0, name="impressions"),
            )
        )

        self.assertEqual(result.reach.sigma, 1000)
        self.assertEqual(result.impression.sigma, 12700)
        self.assertEqual(result.k_reach[1].sigma, 1000)
        self.assertEqual(result.k_reach[2].sigma, 0)

    def test_add_measurement_set_uncertainty_preserves_dp_noised_zeros(self):
        thresholds = PotentialDirectResultMinimumThresholds(
            min_users=100,
            min_impressions=1000,
        )
        measurement_set = MeasurementSet(
            reach=Measurement(value=0, sigma=30, name="reach"),
            k_reach={
                1: Measurement(value=0, sigma=40, name="frequency-1"),
                2: Measurement(value=0, sigma=50, name="frequency-2"),
            },
            impression=Measurement(value=0, sigma=60, name="impressions"),
        )

        result = thresholds.add_measurement_set_uncertainty(measurement_set)

        self.assertEqual(result, measurement_set)

    def test_add_measurement_set_uncertainty_does_not_change_nonzero_results(self):
        thresholds = PotentialDirectResultMinimumThresholds(
            min_users=100,
            min_impressions=1000,
        )
        measurement_set = MeasurementSet(
            reach=Measurement(value=50, sigma=10, name="reach"),
            k_reach={
                1: Measurement(value=50, sigma=20, name="frequency-1"),
                2: Measurement(value=0, sigma=30, name="frequency-2"),
            },
            impression=Measurement(value=75, sigma=40, name="impressions"),
        )

        result = thresholds.add_measurement_set_uncertainty(measurement_set)

        self.assertEqual(result, measurement_set)

    def test_constructor_rejects_nonpositive_thresholds(self):
        with self.assertRaisesRegex(ValueError, "min_users"):
            PotentialDirectResultMinimumThresholds(
                min_users=0,
                min_impressions=1000,
            )
        with self.assertRaisesRegex(ValueError, "min_impressions"):
            PotentialDirectResultMinimumThresholds(
                min_users=100,
                min_impressions=0,
            )


if __name__ == "__main__":
    unittest.main()
