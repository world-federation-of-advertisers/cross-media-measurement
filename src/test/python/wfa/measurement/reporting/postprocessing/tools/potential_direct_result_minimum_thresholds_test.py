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
import unittest

from noiseninja.noised_measurements import Measurement, MeasurementSet
from tools.potential_direct_result_minimum_thresholds import (
    PotentialDirectResultMinimumThresholds,
)


class PotentialDirectResultMinimumThresholdsTest(unittest.TestCase):

    def test_add_reach_uncertainty_combines_existing_noise(self):
        thresholds = PotentialDirectResultMinimumThresholds(
            min_users=100,
            min_impressions=1000,
            applies_to_multi_publisher_results=False,
        )

        result = thresholds.add_reach_uncertainty(
            Measurement(value=0, sigma=30, name="reach")
        )

        self.assertEqual(result.value, 0)
        self.assertAlmostEqual(result.sigma, math.hypot(30, 1000))

    def test_add_reach_and_frequency_uncertainty_leaves_impression_unchanged(
        self,
    ):
        thresholds = PotentialDirectResultMinimumThresholds(
            min_users=100,
            min_impressions=1000,
            applies_to_multi_publisher_results=True,
        )
        impression = Measurement(
            value=0, sigma=60, name="impressions"
        )

        result = thresholds.add_reach_and_frequency_uncertainty(
            MeasurementSet(
                reach=Measurement(value=0, sigma=30, name="reach"),
                k_reach={
                    1: Measurement(value=0, sigma=40, name="frequency-1"),
                    2: Measurement(value=0, sigma=50, name="frequency-2"),
                },
                impression=impression,
            )
        )

        self.assertAlmostEqual(result.reach.sigma, math.hypot(30, 1000))
        self.assertAlmostEqual(
            result.k_reach[1].sigma, math.hypot(40, 1000)
        )
        self.assertEqual(result.k_reach[2].sigma, 50)
        self.assertEqual(result.impression, impression)

    def test_add_measurement_set_uncertainty_updates_zero_results(self):
        thresholds = PotentialDirectResultMinimumThresholds(
            min_users=100,
            min_impressions=1000,
            applies_to_multi_publisher_results=False,
        )

        result = thresholds.add_measurement_set_uncertainty(
            MeasurementSet(
                reach=Measurement(value=0, sigma=30, name="reach"),
                k_reach={
                    1: Measurement(value=0, sigma=40, name="frequency-1"),
                    2: Measurement(value=0, sigma=50, name="frequency-2"),
                },
                impression=Measurement(
                    value=0, sigma=60, name="impressions"
                ),
            )
        )

        self.assertAlmostEqual(result.reach.sigma, math.hypot(30, 1000))
        self.assertAlmostEqual(
            result.impression.sigma, math.hypot(60, 12700)
        )
        self.assertAlmostEqual(
            result.k_reach[1].sigma, math.hypot(40, 1000)
        )
        self.assertEqual(result.k_reach[2].sigma, 50)

    def test_add_measurement_set_uncertainty_does_not_change_nonzero_results(self):
        thresholds = PotentialDirectResultMinimumThresholds(
            min_users=100,
            min_impressions=1000,
            applies_to_multi_publisher_results=False,
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
                applies_to_multi_publisher_results=False,
            )
        with self.assertRaisesRegex(ValueError, "min_impressions"):
            PotentialDirectResultMinimumThresholds(
                min_users=100,
                min_impressions=0,
                applies_to_multi_publisher_results=False,
            )


if __name__ == "__main__":
    unittest.main()
