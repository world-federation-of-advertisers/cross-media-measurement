from unittest import TestCase

from src.noiseninja.noised_measurements import SetMeasurementsSpec, Measurement
from src.noiseninja.solver import Solver


class Test(TestCase):
    def test_solve_same_sigma_one_constraint(self):
        spec = SetMeasurementsSpec()
        spec.add_subset_relation(parent=1, child=2)
        spec.add_subset_relation(parent=1, child=3)
        spec.add_cover(parent=1, children=[2, 3])
        spec.add_measurement(measusured_set=1, measurement=Measurement(50, 1))
        spec.add_measurement(measusured_set=2, measurement=Measurement(48, 0))
        spec.add_measurement(measusured_set=3, measurement=Measurement(1, 1))
        solution = Solver.solve(spec)
        self.assertAlmostEqual(solution[1], 49.5, places=4, msg=solution)
        self.assertAlmostEqual(solution[2], 48, msg=solution)
        self.assertAlmostEqual(solution[3], 1.5, places=4, msg=solution)

    def test_solve_with_different_sigma_one_constraint(self):
        spec = SetMeasurementsSpec()
        spec.add_subset_relation(parent=1, child=2)
        spec.add_subset_relation(parent=1, child=3)
        spec.add_cover(parent=1, children=[2, 3])
        spec.add_measurement(measusured_set=1, measurement=Measurement(50, 1))
        spec.add_measurement(measusured_set=2, measurement=Measurement(48, 0))
        spec.add_measurement(measusured_set=3, measurement=Measurement(1, 1e-6))
        solution = Solver.solve(spec)
        self.assertAlmostEqual(solution[2], 48, msg=solution)
        # set 3 has very small sigma, therefore should not change much.
        self.assertAlmostEqual(solution[3], 1, places=4, msg=solution)
        self.assertTrue(solution[1] <= solution[2] + solution[3])
