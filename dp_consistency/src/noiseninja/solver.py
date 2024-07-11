from threading import Semaphore
from typing import Any

import numpy as np
from qpsolvers import solve_problem, Problem

from src.noiseninja.noised_measurements import SetMeasurementsSpec

SOLVER = "clarabel"

SEMAPHORE = Semaphore()


class Solver:

    def __init__(self, num_variables):
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

    def __add_cover_set_constraint(self, cover_variables: list[int], set_variable: int):
        variables = np.zeros(self.num_variables)
        variables.put(cover_variables, -1)
        variables[set_variable] = 1
        self.__add_gt_term(variables)

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

    def __solve_with_initial_value(self, x0):
        problem: Problem
        if len(self.A) > 0:
            problem = Problem(
                self.P, self.q, np.array(self.G), np.array(self.h), np.array(self.A), np.array(self.b))
        else:
            problem = Problem(
                self.P, self.q, np.array(self.G), np.array(self.b))
        solution = solve_problem(problem, solver=SOLVER, verbose=False)
        return solution.x

    @staticmethod
    def solve(set_measurement_spec: SetMeasurementsSpec) -> dict[int, float]:
        variable_index_by_set_id: dict[int, int] = {}
        num_vars = 0
        for measured_set in set_measurement_spec.all_sets():
            variable_index_by_set_id[measured_set] = num_vars
            num_vars += 1

        solver = Solver(num_vars)
        for measured_set in set_measurement_spec.all_sets():
            for cover in set_measurement_spec.covers_by_set[measured_set]:
                solver.__add_cover_set_constraint(
                    list(variable_index_by_set_id[i] for i in cover), variable_index_by_set_id[measured_set])

            for subset in set(set_measurement_spec.subsets_by_set[measured_set]):
                solver.__add_parent_gt_child_term(
                    variable_index_by_set_id[measured_set], variable_index_by_set_id[subset])

            variables = np.zeros(num_vars)
            variables[variable_index_by_set_id[measured_set]] = 1

            for measurement in set_measurement_spec.measurements_by_set[measured_set]:
                if abs(measurement.sigma) == 0:
                    solver.__add_eq_term(variables, measurement.value)
                else:
                    solver.__add_loss_term(
                        np.multiply(variables, 1 / measurement.sigma), -measurement.value / measurement.sigma)
        # TODO: check if qp-solver is thread safe, and remove this semaphore.
        SEMAPHORE.acquire()
        solution = solver.__solve()
        SEMAPHORE.release()
        result: dict[int, Any] = {}
        for measured_set in set_measurement_spec.all_sets():
            result[measured_set] = solution[variable_index_by_set_id[measured_set]]
        return result
