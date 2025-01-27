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

import numpy as np
import unittest

from noiseninja.noised_measurements import SetMeasurementsSpec, Measurement
from noiseninja.solver import Solver
from qpsolvers import Solution, Problem
from unittest.mock import MagicMock

HIGHS_SOLVER = "highs"


class SolverTest(unittest.TestCase):
  def test_solve_when_highs_solver_fails_to_converge(self):
    spec = SetMeasurementsSpec()
    spec.add_subset_relation(1, 2)
    spec.add_subset_relation(1, 3)
    spec.add_cover(1, [2, 3])
    spec.add_equal_relation(1, 4)
    spec.add_measurement(1, Measurement(50, 1, "measurement_01"))
    spec.add_measurement(2, Measurement(48, 0, "measurement_02"))
    spec.add_measurement(3, Measurement(1, 1, "measurement_03"))
    spec.add_measurement(4, Measurement(51, 1, "measurement_04"))


    solver = Solver(spec)

    # Stores the original function.
    original_solve = solver._solve

    def side_effect(solver_name):
      if solver_name == HIGHS_SOLVER:
        return Solution(x=None, found=False, problem=solver._problem())
      else:
        # Call the original function for other solvers.
        return original_solve(solver_name)

    # The mock function executes the method _solve() normally for all solvers
    # other than HIGHS. For the HIGHS solver, it returns a non-solution.
    mock_solve = MagicMock(side_effect=side_effect)
    solver._solve = mock_solve

    # Verifies that the HIGHS solver returns a non-solution.
    highs_solution = solver._solve(HIGHS_SOLVER)
    self.assertFalse(highs_solution.found)

    # Due to the fact that HIGHS solver returns a non-solution, the back-up
    # solver (OSQP) will be called.
    solution = solver.solve_and_translate()

    # Verifies that a valid solution is obtained.
    self.assertAlmostEqual(solution[1], 50.000, places=3, msg=solution)
    self.assertAlmostEqual(solution[2], 48.000, places=3, msg=solution)
    self.assertAlmostEqual(solution[3], 2.000, places=3, msg=solution)
    self.assertAlmostEqual(solution[1], solution[4], places=3, msg=solution)

  def test_solve_same_sigma_one_constraint(self):
    spec = SetMeasurementsSpec()
    spec.add_subset_relation(1, 2)
    spec.add_subset_relation(1, 3)
    spec.add_cover(1, [2, 3])
    spec.add_equal_relation(1, 4)
    spec.add_measurement(1, Measurement(50, 1, "measurement_01"))
    spec.add_measurement(2, Measurement(48, 0, "measurement_02"))
    spec.add_measurement(3, Measurement(1, 1, "measurement_03"))
    spec.add_measurement(4, Measurement(51, 1, "measurement_04"))
    solution = Solver(spec).solve_and_translate()
    self.assertAlmostEqual(solution[1], 50.000, places=3, msg=solution)
    self.assertAlmostEqual(solution[2], 48.000, places=3, msg=solution)
    self.assertAlmostEqual(solution[3], 2.000, places=3, msg=solution)
    self.assertAlmostEqual(solution[1], solution[4], places=3, msg=solution)

  def test_solve_with_different_sigma_one_constraint(self):
    spec = SetMeasurementsSpec()
    spec.add_subset_relation(1, 2)
    spec.add_subset_relation(1, 3)
    spec.add_cover(1, [2, 3])
    spec.add_equal_relation(1, 4)
    spec.add_measurement(1, Measurement(50, 1, "measurement_01"))
    spec.add_measurement(2, Measurement(48, 0, "measurement_02"))
    spec.add_measurement(3, Measurement(1, 1e-6, "measurement_03"))
    spec.add_measurement(4, Measurement(51, 1, "measurement_04"))
    solution = Solver(spec).solve_and_translate()
    self.assertAlmostEqual(solution[2], 48, msg=solution)
    self.assertAlmostEqual(solution[1], solution[4], places=3, msg=solution)
    # set 3 has very small sigma, therefore should not change much.
    self.assertAlmostEqual(solution[3], 1, places=4, msg=solution)
    self.assertTrue(solution[1] <= solution[2] + solution[3])


if __name__ == "__main__":
  unittest.main()
