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

import logging
import numpy as np
import sys

from noiseninja.noised_measurements import SetMeasurementsSpec
from qpsolvers import solve_problem, Problem, Solution
from threading import Semaphore

logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

HIGHS_SOLVER = "highs"
OSQP_SOLVER = "osqp"
MAX_ATTEMPTS = 10
SEMAPHORE = Semaphore()


class SolutionNotFoundError(ValueError):
  _non_solution: Solution

  def __init__(self, non_solution: Solution):
    super().__init__(non_solution)
    self._non_solution = non_solution

  def get_non_solution_details(self):
    return self._non_solution


class Solver:

  def __init__(self, set_measurement_spec: SetMeasurementsSpec):
    logger.info(
        "Initialize the solver with constraints obtained from the set "
        "measurement spec."
    )
    variable_index_by_set_id = Solver._map_sets_to_variables(
        set_measurement_spec)
    self.num_variables = len(variable_index_by_set_id)
    self._init_qp(self.num_variables)
    self._add_covers(set_measurement_spec, variable_index_by_set_id)
    self._add_subsets(set_measurement_spec, variable_index_by_set_id)
    self._add_measurement_targets(set_measurement_spec,
                                  variable_index_by_set_id)
    self._init_base_value(set_measurement_spec, variable_index_by_set_id)

    self.variable_map = dict(
        (variable_index_by_set_id[i], i) for i in variable_index_by_set_id)

  def _init_base_value(self, set_measurement_spec: SetMeasurementsSpec,
      variable_index_by_set_id: dict[int, int]):
    logger.info(
        "Uses the measurements' value as the initial values for the solver."
    )
    mean_measurement_by_variable: dict[int, float] = {}
    for measured_set in set_measurement_spec.all_sets():
      mean_measurement_by_variable[
        variable_index_by_set_id[measured_set]] = (
          sum(v.value
              for v in set_measurement_spec.get_measurements(
              measured_set)) / len(
          set_measurement_spec.get_measurements(measured_set)))
    self.base_value = np.array(list(
        (mean_measurement_by_variable[i]
         for i in range(0, self.num_variables))))

  def _add_measurement_targets(self, set_measurement_spec: SetMeasurementsSpec,
      variable_index_by_set_id: dict[int, int]):
    logger.info(
        "Calculate the loss and equality terms from each measurement's value "
        "and variance."
    )
    for (measured_set, variable) in variable_index_by_set_id.items():
      variables = np.zeros(self.num_variables)
      variables[variable] = 1
      for measurement in set_measurement_spec.get_measurements(
          measured_set):
        if abs(measurement.sigma) == 0:
          self._add_eq_term(variables, measurement.value)
        else:
          self._add_loss_term(
              np.multiply(variables, 1 / measurement.sigma),
              -measurement.value / measurement.sigma)

  def _map_sets_to_variables(set_measurement_spec: SetMeasurementsSpec) -> dict[
    int, int]:
    logger.info("Assigns an ID for each measurement set.")
    variable_index_by_set_id: dict[int, int] = {}
    num_variables = 0
    for measured_set in set_measurement_spec.all_sets():
      variable_index_by_set_id[measured_set] = num_variables
      num_variables += 1
    return variable_index_by_set_id

  def _init_qp(self, num_variables: int):
    self.num_variables = num_variables
    # Minimize 1/2 x^T P x + q^T x
    self.P = np.zeros(shape=(num_variables, num_variables))
    self.q = np.zeros(shape=(1, num_variables))
    # subject to G x <= h
    self.G = []
    self.h = []
    # and A x = h
    self.A = []
    self.b = []

  def _add_subsets(self, set_measurement_spec: SetMeasurementsSpec,
      variable_index_by_set_id: dict[int, int]):
    logger.info("Adding subset constraints.")
    for measured_set in set_measurement_spec.all_sets():
      for subset in set(set_measurement_spec.get_subsets(measured_set)):
        self._add_parent_gt_child_term(
            variable_index_by_set_id[measured_set],
            variable_index_by_set_id[subset])

  def _add_covers(self, set_measurement_spec: SetMeasurementsSpec,
      variable_index_by_set_id: dict[int, int]):
    logger.info("Adding cover set constraints.")
    for measured_set in set_measurement_spec.all_sets():
      for cover in set_measurement_spec.get_covers_of_set(measured_set):
        self._add_cover_set_constraint(
            list(variable_index_by_set_id[i] for i in cover),
            variable_index_by_set_id[measured_set])

  def _add_cover_set_constraint(self, cover_variables: set[int],
      set_variable: int):
    variables = np.zeros(self.num_variables)
    variables.put(cover_variables, -1)
    variables[set_variable] = 1
    self._add_gt_term(variables)

  def _is_feasible(self, vector: np.array) -> bool:
    for i, g in enumerate(self.G):
      if np.dot(vector, g) > self.h[i][0]:
        return False
    return True

  def _add_parent_gt_child_term(self, parent: int, child: int):
    variables = np.zeros(self.num_variables)
    variables.put(parent, -1)
    variables[child] = 1
    self._add_gt_term(variables)

  def _add_loss_term(self, variables: np.array, k: float):
    for v1, coeff1 in enumerate(variables):
      self.q[0][v1] += coeff1 * k
      for v2, coeff2 in enumerate(variables):
        self.P[v1][v2] += coeff1 * coeff2

  def _add_eq_term(self, variables: np.array, k: float):
    self.A.append(variables)
    self.b.append(k)

  def _add_gt_term(self, variables: np.array):
    self.G.append(variables)
    self.h.append([0])

  def _solve_with_initial_value(self, solver_name, x0) -> Solution:
    problem = self._problem()
    solution = solve_problem(problem, solver=solver_name, initvals=x0,
                             verbose=False)
    return solution

  def _problem(self):
    problem: Problem
    if len(self.A) > 0:
      problem = Problem(
          self.P, self.q, np.array(self.G), np.array(self.h),
          np.array(self.A), np.array(self.b))
    else:
      problem = Problem(
          self.P, self.q, np.array(self.G), np.array(self.h))
    return problem

  def solve(self) -> Solution:
    logger.info(
        "Solves the quadratic program with the constraints extracted from set "
        "measurement spec."
    )
    attempt_count = 0
    if self._is_feasible(self.base_value):
      logger.info(
          "The set measurement spec meets all the constraints, no action needed."
      )
      solution = Solution(x=self.base_value,
                          found=True,
                          extras={'status': 'trivial'},
                          problem=self._problem())
    else:
      logger.info(
          "Attemps to solve the quadratic program with the HIGHS solver."
      )
      while attempt_count < MAX_ATTEMPTS:
        # TODO: check if qpsolvers is thread safe,
        #  and remove this semaphore.
        SEMAPHORE.acquire()
        solution = self._solve_with_initial_value(HIGHS_SOLVER, self.base_value)
        SEMAPHORE.release()

        if solution.found:
          break
        else:
          attempt_count += 1

      # If the highs solver does not converge, switch to the osqp solver which
      # is more robust.
      if not solution.found:
        logger.info("HIGHS solver does not converge, retry with OSQP solver.")
        SEMAPHORE.acquire()
        solution = self._solve_with_initial_value(OSQP_SOLVER, self.base_value)
        SEMAPHORE.release()

    # Raise the exception when both solvers do not converge.
    if not solution.found:
      logger.critical("No solution found for the quadratic program.")
      raise SolutionNotFoundError(solution)

    return solution

  def translate_solution(self, solution: Solution) -> dict[int, float]:
    result: dict[int, float] = {}
    for var in range(0, self.num_variables):
      result[self.variable_map[var]] = solution.x[var]
    return result

  def solve_and_translate(self) -> dict[int, float]:
    solution = self.solve()
    return self.translate_solution(solution)
