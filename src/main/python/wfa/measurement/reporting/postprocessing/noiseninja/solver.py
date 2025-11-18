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
from threading import Semaphore

from absl import logging

from qpsolvers import Problem
from qpsolvers import Solution
from qpsolvers import solve_problem

from noiseninja.noised_measurements import OrderedSets
from noiseninja.noised_measurements import SetMeasurementsSpec
from wfa.measurement.internal.reporting.postprocessing import report_post_processor_result_pb2

ReportPostProcessorStatus = report_post_processor_result_pb2.ReportPostProcessorStatus
StatusCode = ReportPostProcessorStatus.StatusCode

TOLERANCE = 1e-1
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
    logging.info("Initializing the solver.")
    variable_index_by_set_id = Solver._map_sets_to_variables(
        set_measurement_spec)
    self.num_variables = len(variable_index_by_set_id)
    self._init_qp(self.num_variables)
    self._add_equals(set_measurement_spec, variable_index_by_set_id)
    self._add_weighted_sum_upperbounds(set_measurement_spec,
                                       variable_index_by_set_id)
    self._add_covers(set_measurement_spec, variable_index_by_set_id)
    self._add_subsets(set_measurement_spec, variable_index_by_set_id)
    self._add_measurement_targets(set_measurement_spec,
                                  variable_index_by_set_id)
    self._add_lower_bounds()
    self._init_base_value(set_measurement_spec, variable_index_by_set_id)

    self.variable_map = dict(
        (variable_index_by_set_id[i], i) for i in variable_index_by_set_id)

  def _init_base_value(self, set_measurement_spec: SetMeasurementsSpec,
      variable_index_by_set_id: dict[int, int]):
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
    logging.debug(f"The base values are {self.base_value}.")

  def _add_measurement_targets(self, set_measurement_spec: SetMeasurementsSpec,
      variable_index_by_set_id: dict[int, int]):
    logging.info("Calculating the loss and equality terms.")
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

  def _add_equals(self, set_measurement_spec: SetMeasurementsSpec,
      variable_index_by_set_id: dict[int, int]):
    logging.info("Adding equal set constraints.")
    for equal_set in set_measurement_spec.get_equal_sets():
      variables = np.zeros(self.num_variables)
      variables[variable_index_by_set_id[equal_set[0]]] = 1
      variables.put([variable_index_by_set_id[i] for i in equal_set[1]], -1)
      self._add_eq_term(variables, 0)

  def _add_weighted_sum_upperbounds(self,
      set_measurement_spec: SetMeasurementsSpec,
      variable_index_by_set_id: dict[int, int]):
    logging.info("Adding weighted sum upperbound constraints.")
    for key, value in set_measurement_spec.get_weighted_sum_upperbound_sets().items():
      variables = np.zeros(self.num_variables)
      variables[variable_index_by_set_id[key]] = -1
      for entry in value:
        variables[variable_index_by_set_id[entry[0]]] = entry[1]
      self._add_gt_term(variables)

  def _add_subsets(self, set_measurement_spec: SetMeasurementsSpec,
      variable_index_by_set_id: dict[int, int]):
    logging.info("Adding subset constraints.")
    for measured_set in set_measurement_spec.all_sets():
      for subset in set(set_measurement_spec.get_subsets(measured_set)):
        self._add_parent_gt_child_term(
            variable_index_by_set_id[measured_set],
            variable_index_by_set_id[subset])

  def _add_ordered_sets(self, set_measurement_spec: SetMeasurementsSpec,
      variable_index_by_set_id: dict[int, int]):
    logging.infor("Adding ordered sets constraints.")
    ordered_sets: list[OrderedSets] = set_measurement_spec.get_ordered_sets()
    for i in range(len(ordered_sets)):
      if len(ordered_sets[i].larger_set) == 0 or len(
          ordered_sets[i].smaller_set) == 0:
        raise ValueError(
            f'The {i}-th ordered set {ordered_sets[i]} has empty larger set or '
            f'empty smaller set.'
        )

      if len(ordered_pair.larger_set) != len(ordered_pair.smaller_set):
        raise ValueError(
            f'The {i}-th ordered set {ordered_sets[i]} has sets of different '
            f'length.'
        )

      variables = np.zeros(self.num_variables)
      for id in ordered_pair.larger_set:
        variables[variable_index_by_set_id[id]] = -1
      for id in ordered_pair.smaller_set:
        variables[variable_index_by_set_id[id]] = 1
      self._add_gt_term(variables)

  def _add_covers(self, set_measurement_spec: SetMeasurementsSpec,
      variable_index_by_set_id: dict[int, int]):
    logging.info("Adding cover set constraints.")
    for measured_set in set_measurement_spec.all_sets():
      for cover in set_measurement_spec.get_covers_of_set(measured_set):
        self._add_cover_set_constraint(
            list(variable_index_by_set_id[i] for i in cover),
            variable_index_by_set_id[measured_set])

  # Enforces that all the variables are non-negative.
  def _add_lower_bounds(self):
    logging.info("Adding lower bounds constraints.")
    for i in range(self.num_variables):
      variables = np.zeros(self.num_variables)
      variables[i] = -1
      self._add_gt_term(variables)

  def _add_cover_set_constraint(self, cover_variables: set[int],
      set_variable: int):
    variables = np.zeros(self.num_variables)
    variables.put(cover_variables, -1)
    variables[set_variable] = 1
    self._add_gt_term(variables)

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
    self.h.append(0.0)

  def _solve_with_initial_value(self, solver_name: str,
      initial_values: np.array) -> Solution:
    problem = self._problem()
    solution = solve_problem(problem, solver=solver_name,
                             initvals=initial_values, verbose=False)
    return solution

  def _problem(self):
    problem: Problem
    if len(self.A) > 0:
      problem = Problem(
          self.P, self.q, np.array(self.G), np.array(self.h),
          np.array(self.A), np.array(self.b))
    else:
      problem = Problem(self.P, self.q, np.array(self.G), np.array(self.h))
    return problem

  def _validate_solution(self, solution: Solution) -> bool:
    """Validates a solution from the QP solver.

    A solution is considered valid if it was found and does not contain any
    negative values beyond a small tolerance. This is acceptable because the
    final results represent reach counts and will be rounded to the nearest
    integer, so negligibly small negative values (e.g., -1e-1) do not impact
    the final result.

    Args:
      solution: The solution object returned by the QP solver.

    Returns:
      True if the solution is valid, False otherwise.
    """
    if not solution.found:
      return False
    if np.any(solution.x < -TOLERANCE):
      logging.warning("Solution contains negative values, invalidating.")
      return False
    return True

  def _solve(self, solver_name: str) -> tuple[
    Solution, ReportPostProcessorStatus]:
    logging.info("Solving the quadratic program.")
    attempt_count = 0
    best_solution = Solution(False)
    smallest_residual = float('inf')
    equality_residual = float('inf')
    inequality_residual = float('inf')
    status_code: StatusCode = StatusCode.SOLUTION_NOT_FOUND

    while attempt_count < MAX_ATTEMPTS:
      # The solver is not thread-safe in general. Synchronization mechanism is
      # needed when it is called concurrently (e.g. in a report processing
      # server).
      SEMAPHORE.acquire()
      solution = self._solve_with_initial_value(solver_name, self.base_value)
      SEMAPHORE.release()

      if self._validate_solution(solution):
        primal_residual = solution.primal_residual()
        if primal_residual < smallest_residual:
          smallest_primal_residual = primal_residual
          best_solution = solution

        # If a solution is found, updates the error code and stops.
        if smallest_primal_residual < TOLERANCE:
          if solver_name == HIGHS_SOLVER:
            status_code = StatusCode.SOLUTION_FOUND_WITH_HIGHS
          elif solver_name == OSQP_SOLVER:
            status_code = StatusCode.SOLUTION_FOUND_WITH_OSQP
          else:
            raise ValueError(f"Unknown solver: {solver_name}")
          break
      attempt_count += 1

    if best_solution.found:
      # Overrides the error code if this is a partial solution.
      if smallest_primal_residual >= TOLERANCE:
        if solver_name == HIGHS_SOLVER:
          status_code = StatusCode.PARTIAL_SOLUTION_FOUND_WITH_HIGHS
        elif solver_name == OSQP_SOLVER:
          status_code = StatusCode.PARTIAL_SOLUTION_FOUND_WITH_OSQP
        else:
          raise ValueError(f"Unknown solver: {solver_name}")

      # Updates the primal equality and inequality residuals.
      equality_residual = np.max(np.abs(
          np.dot(self.A, best_solution.x) - np.array(
              self.b))) if len(self.A) > 0 else 0.0
      inequality_residual = max(0.0, np.max(
          np.dot(self.G, best_solution.x) - np.array(
              self.h))) if len(self.G) > 0 else 0.0

    report_post_processor_status = ReportPostProcessorStatus(
        status_code=status_code,
        primal_equality_residual=equality_residual,
        primal_inequality_residual=inequality_residual,
    )
    return best_solution, report_post_processor_status

  def solve(self) -> tuple[Solution, ReportPostProcessorStatus]:
    logging.info(
        "Solving the quadratic program with the HIGHS solver."
    )
    solution, report_post_processor_status = self._solve(HIGHS_SOLVER)

    # If the highs solver does not converge, switch to the osqp solver which
    # is more robust. However, OSQP in general is less accurate than HIGHS
    # (See https://web.stanford.edu/~boyd/papers/pdf/osqp.pdf).
    if not solution.found:
      logging.info(
          "Switching to OSQP solver as HIGHS solver failed to converge."
      )
      solution, report_post_processor_status = self._solve(OSQP_SOLVER)

    return solution, report_post_processor_status

  def translate_solution(self, solution: Solution) -> dict[int, float]:
    result: dict[int, float] = {}
    if solution.found:
      for var in range(0, self.num_variables):
        result[self.variable_map[var]] = solution.x[var]
    return result

  def solve_and_translate(self) -> tuple[
    dict[int, float], ReportPostProcessorStatus]:
    solution, report_post_processor_status = self.solve()
    return self.translate_solution(solution), report_post_processor_status
