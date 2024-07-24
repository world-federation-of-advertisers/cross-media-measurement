from threading import Semaphore
from typing import Any

import numpy as np
from qpsolvers import solve_problem, Problem, Solution

from src.noiseninja.noised_measurements import SetMeasurementsSpec

SOLVER = "clarabel"

SEMAPHORE = Semaphore()


class SolutionNotFoundError(ValueError):
    __non_solution: Solution

    def __init__(self, non_solution: Solution):
        super().__init__(non_solution)
        self.__non_solution = non_solution

    def get_non_solution_details(self):
        return self.__non_solution


class Solver:

    def __init__(self, set_measurement_spec: SetMeasurementsSpec):
        variable_index_by_set_id = Solver.__map_sets_to_variables(
            set_measurement_spec)
        self.num_variables = len(variable_index_by_set_id)
        self.__init_qp(self.num_variables)
        self.__add_covers(set_measurement_spec, variable_index_by_set_id)
        self.__add_subsets(set_measurement_spec, variable_index_by_set_id)
        self.__add_measurement_targets(set_measurement_spec,
                                       variable_index_by_set_id)
        self.__init_base_value(set_measurement_spec, variable_index_by_set_id)

        self.variable_map = dict(
            (variable_index_by_set_id[i], i) for i in variable_index_by_set_id)

    def __init_base_value(self, set_measurement_spec, variable_index_by_set_id):
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

    def __add_measurement_targets(self, set_measurement_spec,
                                  variable_index_by_set_id):
        for (measured_set, variable) in variable_index_by_set_id.items():
            variables = np.zeros(self.num_variables)
            variables[variable] = 1
            for measurement in set_measurement_spec.get_measurements(
                    measured_set):
                if abs(measurement.sigma) == 0:
                    self.__add_eq_term(variables, measurement.value)
                else:
                    self.__add_loss_term(
                        np.multiply(variables, 1 / measurement.sigma),
                        -measurement.value / measurement.sigma)

    @staticmethod
    def __map_sets_to_variables(set_measurement_spec) -> dict[int, int]:
        variable_index_by_set_id: dict[int, int] = {}
        num_variables = 0
        for measured_set in set_measurement_spec.all_sets():
            variable_index_by_set_id[measured_set] = num_variables
            num_variables += 1
        return variable_index_by_set_id

    def __init_qp(self, num_variables):
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

    def __add_subsets(self, set_measurement_spec, variable_index_by_set_id):
        for measured_set in set_measurement_spec.all_sets():
            for subset in set(set_measurement_spec.get_subsets(measured_set)):
                self.__add_parent_gt_child_term(
                    variable_index_by_set_id[measured_set],
                    variable_index_by_set_id[subset])

    def __add_covers(self, set_measurement_spec, variable_index_by_set_id):
        for measured_set in set_measurement_spec.all_sets():
            for cover in set_measurement_spec.get_covers_of_set(measured_set):
                self.__add_cover_set_constraint(
                    list(variable_index_by_set_id[i] for i in cover),
                    variable_index_by_set_id[measured_set])

    def __add_cover_set_constraint(self, cover_variables: list[int],
                                   set_variable: int):
        variables = np.zeros(self.num_variables)
        variables.put(cover_variables, -1)
        variables[set_variable] = 1
        self.__add_gt_term(variables)

    def __is_feasible(self, vector: np.array) -> bool:
        for i, g in enumerate(self.G):
            if np.dot(vector, g) > self.h[i][0]:
                return False
        return True

    def __add_parent_gt_child_term(self, parent: int, child: int):
        variables = np.zeros(self.num_variables)
        variables.put(parent, -1)
        variables[child] = 1
        self.__add_gt_term(variables)

    def __add_loss_term(self, variables, k: float):
        for v1, coeff1 in enumerate(variables):
            self.q[0][v1] += coeff1 * k
            for v2, coeff2 in enumerate(variables):
                self.P[v1][v2] += coeff1 * coeff2

    def __add_eq_term(self, variables, k: float):
        self.A.append(variables)
        self.b.append(k)

    def __add_gt_term(self, variables):
        self.G.append(variables)
        self.h.append([0])

    def __solve(self):
        x0 = np.random.randn(self.num_variables)
        return self.__solve_with_initial_value(x0)

    def __solve_with_initial_value(self, x0) -> Solution:
        problem = self.__problem()
        solution = solve_problem(problem, solver=SOLVER, verbose=False)
        return solution

    def __problem(self):
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
        if self.__is_feasible(self.base_value):
            solution = Solution(x=self.base_value,
                                found=True,
                                extras={'status': 'trivial'},
                                problem=self.__problem())
        else:
            # TODO: check if qpsolvers is thread safe,
            #  and remove this semaphore.
            SEMAPHORE.acquire()
            solution = self.__solve()
            SEMAPHORE.release()

        if not solution.found:
            raise SolutionNotFoundError(solution)

        return solution

    def translate_solution(self, solution: Solution) -> dict[int, float]:
        result: dict[int, Any] = {}
        for var in range(0, self.num_variables):
            result[self.variable_map[var]] = solution.x[var]
        return result

    def solve_and_translate(self):
        solution = self.solve()
        return self.translate_solution(solution)