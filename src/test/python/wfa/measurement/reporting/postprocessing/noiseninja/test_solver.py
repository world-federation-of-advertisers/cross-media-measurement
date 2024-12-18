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

import unittest

from noiseninja.noised_measurements import SetMeasurementsSpec, Measurement
from noiseninja.solver import Solver


class SolverTest(unittest.TestCase):
  def test_solve_same_sigma_one_constraint(self):
    spec = SetMeasurementsSpec()
    spec.add_subset_relation(1, 2)
    spec.add_subset_relation(1, 3)
    spec.add_cover(1, [2, 3])
    spec.add_measurement(1, Measurement(50, 1, "measurement_01"))
    spec.add_measurement(2, Measurement(48, 0, "measurement_02"))
    spec.add_measurement(3, Measurement(1, 1, "measurement_03"))
    solution = Solver(spec).solve_and_translate()
    self.assertAlmostEqual(solution[1], 49.5, places=4, msg=solution)
    self.assertAlmostEqual(solution[2], 48, msg=solution)
    self.assertAlmostEqual(solution[3], 1.5, places=4, msg=solution)

  def test_solve_with_different_sigma_one_constraint(self):
    spec = SetMeasurementsSpec()
    spec.add_subset_relation(1, 2)
    spec.add_subset_relation(1, 3)
    spec.add_cover(1, [2, 3])
    spec.add_measurement(1, Measurement(50, 1, "measurement_01"))
    spec.add_measurement(2, Measurement(48, 0, "measurement_02"))
    spec.add_measurement(3, Measurement(1, 1e-6, "measurement_03"))
    solution = Solver(spec).solve_and_translate()
    self.assertAlmostEqual(solution[2], 48, msg=solution)
    # set 3 has very small sigma, therefore should not change much.
    self.assertAlmostEqual(solution[3], 1, places=4, msg=solution)
    self.assertTrue(solution[1] <= solution[2] + solution[3])


if __name__ == "__main__":
  unittest.main()
