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
import numpy as np

from unittest.mock import MagicMock

from qpsolvers import Solution

from noiseninja.noised_measurements import Measurement
from noiseninja.noised_measurements import SetMeasurementsSpec
from noiseninja.solver import Solver
from wfa.measurement.internal.reporting.postprocessing import (
    report_post_processor_result_pb2)

ReportPostProcessorStatus = report_post_processor_result_pb2.ReportPostProcessorStatus
StatusCode = ReportPostProcessorStatus.StatusCode

HIGHS_SOLVER = "highs"
TOLERANCE = 1e-1


class SolverTest(unittest.TestCase):

    def test_solve_when_highs_solver_fails_to_converge(self):
        spec = SetMeasurementsSpec()
        spec.add_subset_relation(1, 2)
        spec.add_subset_relation(1, 3)
        spec.add_cover(1, [2, 3])
        spec.add_equal_relation(1, [4])
        spec.add_measurement(1, Measurement(50, 1, "measurement_01"))
        spec.add_measurement(2, Measurement(48, 0, "measurement_02"))
        spec.add_measurement(3, Measurement(1, 1, "measurement_03"))
        spec.add_measurement(4, Measurement(51, 1, "measurement_04"))

        solver = Solver(spec)

        # Stores the original function.
        original_solve = solver._solve

        def side_effect(solver_name):
            if solver_name == HIGHS_SOLVER:
                return Solution(
                    x=None, found=False,
                    problem=solver._problem()), ReportPostProcessorStatus(
                        status_code=StatusCode.SOLUTION_NOT_FOUND)
            else:
                # Call the original function for other solvers.
                return original_solve(solver_name)

        # The mock function executes the method _solve() normally for all solvers
        # other than HIGHS. For the HIGHS solver, it returns a non-solution.
        mock_solve = MagicMock(side_effect=side_effect)
        solver._solve = mock_solve

        # Verifies that the HIGHS solver returns a non-solution.
        highs_solution, report_post_processor_status = solver._solve(
            HIGHS_SOLVER)
        self.assertFalse(highs_solution.found)
        self.assertEqual(report_post_processor_status.status_code,
                         StatusCode.SOLUTION_NOT_FOUND)

        # Due to the fact that HIGHS solver returns a non-solution, the back-up
        # solver (OSQP) will be called.
        solution, report_post_processor_status = solver.solve_and_translate()

        self.assertEqual(report_post_processor_status.status_code,
                         StatusCode.SOLUTION_FOUND_WITH_OSQP)
        self.assertLess(
            max(report_post_processor_status.primal_equality_residual,
                report_post_processor_status.primal_inequality_residual),
            TOLERANCE)

        # Verifies that a valid solution is obtained.
        self.assertAlmostEqual(solution[1], 50.000, places=2, msg=solution)
        self.assertAlmostEqual(solution[2], 48.000, places=2, msg=solution)
        self.assertAlmostEqual(solution[3], 2.000, places=2, msg=solution)
        self.assertAlmostEqual(solution[1],
                               solution[4],
                               places=2,
                               msg=solution)

    def test_solve_same_sigma_one_constraint(self):
        spec = SetMeasurementsSpec()
        spec.add_subset_relation(1, 2)
        spec.add_subset_relation(1, 3)
        spec.add_cover(1, [2, 3])
        spec.add_equal_relation(1, [2, 4])
        spec.add_weighted_sum_upperbound_relation(
            5, [[1, 1.0], [2, 2.0], [3, 3.0]])

        spec.add_measurement(1, Measurement(50, 1, "measurement_01"))
        spec.add_measurement(2, Measurement(48, 0, "measurement_02"))
        spec.add_measurement(3, Measurement(1, 1, "measurement_03"))
        spec.add_measurement(4, Measurement(51, 1, "measurement_04"))
        spec.add_measurement(5, Measurement(51, 1, "measurement_05"))

        solution, report_post_processor_status = Solver(
            spec).solve_and_translate()

        self.assertIn(report_post_processor_status.status_code, [
            StatusCode.SOLUTION_FOUND_WITH_HIGHS,
            StatusCode.SOLUTION_FOUND_WITH_OSQP
        ])
        self.assertLess(
            max(report_post_processor_status.primal_equality_residual,
                report_post_processor_status.primal_inequality_residual),
            TOLERANCE)

        # Verifies tha all constraints are met.
        self.assertAlmostEqual(solution[1], 48.000, places=3, msg=solution)
        self.assertAlmostEqual(solution[2], 48.000, places=3, msg=solution)
        self.assertAlmostEqual(solution[3], 0.000, places=3, msg=solution)
        self.assertAlmostEqual(solution[1],
                               solution[2] + solution[4],
                               places=3,
                               msg=solution)
        self.assertGreaterEqual(
            solution[5], solution[1] + 2 * solution[2] + 3 * solution[3])

    def test_solve_with_different_sigma_one_constraint(self):
        spec = SetMeasurementsSpec()
        spec.add_subset_relation(1, 2)
        spec.add_subset_relation(1, 3)
        spec.add_cover(1, [2, 3])
        spec.add_equal_relation(1, [2, 4])
        spec.add_weighted_sum_upperbound_relation(
            5, [[1, 1.0], [2, 2.0], [3, 3.0]])

        spec.add_measurement(1, Measurement(50, 1, "measurement_01"))
        spec.add_measurement(2, Measurement(48, 0, "measurement_02"))
        spec.add_measurement(3, Measurement(1, 1e-6, "measurement_03"))
        spec.add_measurement(4, Measurement(51, 1, "measurement_04"))
        spec.add_measurement(5, Measurement(51, 1, "measurement_05"))

        solution, report_post_processor_status = Solver(
            spec).solve_and_translate()

        self.assertIn(report_post_processor_status.status_code, [
            StatusCode.SOLUTION_FOUND_WITH_HIGHS,
            StatusCode.SOLUTION_FOUND_WITH_OSQP
        ])

        self.assertLess(
            max(report_post_processor_status.primal_equality_residual,
                report_post_processor_status.primal_inequality_residual),
            TOLERANCE)

        # Verifies tha all constraints are met.
        self.assertAlmostEqual(solution[1], 48, msg=solution)
        self.assertAlmostEqual(solution[2], 48, msg=solution)
        # set 3 has very small sigma, therefore should not change much.
        self.assertAlmostEqual(solution[3], 1, places=4, msg=solution)
        self.assertAlmostEqual(solution[1],
                               solution[2] + solution[4],
                               places=3,
                               msg=solution)
        self.assertGreaterEqual(
            solution[5], solution[1] + 2 * solution[2] + 3 * solution[3])

    def test_solve_solution_does_not_have_negative_values(self):
        spec = SetMeasurementsSpec()
        spec.add_subset_relation(1, 2)
        spec.add_subset_relation(1, 3)
        spec.add_cover(1, [2, 3])
        spec.add_equal_relation(1, [4])

        # The measurement values are set to meet the above constraints.
        spec.add_measurement(1, Measurement(-1, 1, "measurement_01"))
        spec.add_measurement(2, Measurement(-1, 1, "measurement_02"))
        spec.add_measurement(3, Measurement(-1, 1, "measurement_03"))
        spec.add_measurement(4, Measurement(-1, 1, "measurement_04"))

        solution, report_post_processor_status = Solver(
            spec).solve_and_translate()

        self.assertIn(report_post_processor_status.status_code, [
            StatusCode.SOLUTION_FOUND_WITH_HIGHS,
            StatusCode.SOLUTION_FOUND_WITH_OSQP
        ])

        self.assertLess(
            max(report_post_processor_status.primal_equality_residual,
                report_post_processor_status.primal_inequality_residual),
            TOLERANCE)

        # Verifies that the solutions are greater than or equal to 0 due to the
        # lower bound constraints.
        self.assertGreaterEqual(solution[1], 0.0)
        self.assertGreaterEqual(solution[2], 0.0)
        self.assertGreaterEqual(solution[3], 0.0)
        self.assertGreaterEqual(solution[4], 0.0)

    def test_solve_fails_when_solution_has_negative_results(self):
        spec = SetMeasurementsSpec()
        # x1 = x2 + x3
        spec.add_equal_relation(1, [2, 3])
        spec.add_measurement(1, Measurement(10, 1, "measurement_01"))
        spec.add_measurement(2, Measurement(20, 1, "measurement_02"))
        spec.add_measurement(3, Measurement(0, 1, "measurement_03"))

        solver = Solver(spec)

        # Verifies that a real solution can be found without mocking and the
        # solution is valid.
        real_solution, real_status = solver.solve_and_translate()
        self.assertIn(real_status.status_code, [
            StatusCode.SOLUTION_FOUND_WITH_HIGHS,
            StatusCode.SOLUTION_FOUND_WITH_OSQP,
            StatusCode.PARTIAL_SOLUTION_FOUND_WITH_HIGHS,
            StatusCode.PARTIAL_SOLUTION_FOUND_WITH_OSQP
        ])
        self.assertTrue(real_solution)
        self.assertTrue(all(value >= 0 for value in real_solution.values()))

        # Mocks the internal _solve_with_initial_value to return a solution with
        # negative values to simulate the case where the QP solver found a solution
        # that violates the non-negativity constraint.
        mock_solution_with_negatives = Solution(x=np.array([10.0, 20.0, -0.11]),
                                                found=True,
                                                problem=solver._problem())
        solver._solve_with_initial_value = MagicMock(
            return_value=mock_solution_with_negatives)

        solution, report_post_processor_status = solver.solve_and_translate()

        self.assertEqual(report_post_processor_status.status_code,
                         StatusCode.SOLUTION_NOT_FOUND)
        self.assertFalse(solution)

    def test_solve_accepts_solution_that_has_small_negative_results(self):
        spec = SetMeasurementsSpec()
        # x1 = x2 + x3
        spec.add_equal_relation(1, [2, 3])
        spec.add_measurement(1, Measurement(10, 1, "measurement_01"))
        spec.add_measurement(2, Measurement(20, 1, "measurement_02"))
        spec.add_measurement(3, Measurement(0, 1, "measurement_03"))

        solver = Solver(spec)

        # Mocks the internal _solve_with_initial_value to return a solution with
        # small negative values that is greater than the tolerance of -1e-1.
        mock_solution_with_negatives = Solution(x=np.array([10.0, 20.0, -0.09]),
                                                found=True,
                                                problem=solver._problem())
        solver._solve_with_initial_value = MagicMock(
            return_value=mock_solution_with_negatives)

        solution, report_post_processor_status = solver.solve_and_translate()

        self.assertEqual(report_post_processor_status.status_code,
                         StatusCode.PARTIAL_SOLUTION_FOUND_WITH_HIGHS)

        self.assertGreaterEqual(solution[1], 10.0)
        self.assertGreaterEqual(solution[2], 20.0)
        self.assertGreaterEqual(solution[3], -0.09)

if __name__ == "__main__":
    unittest.main()
