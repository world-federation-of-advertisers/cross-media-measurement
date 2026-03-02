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
from collections import defaultdict

from src.main.python.wfa.measurement.reporting.postprocessingv2.report import (
  Metric,
  MetricSet,
  DataProviderMetricSetMap,
)
from src.main.python.wfa.measurement.reporting.postprocessingv2.constraint_generator import (
  LowerBoundRelationGenerator,
  CoverRelationGenerator,
  ImpressionsSumRelationGenerator,
  SubsetRelationGenerator,
  ReachFrequencyRelationGenerator,
  ReachImpressionsRelationGenerator,
  FrequencyImpressionsRelationGenerator,
  UnnoisedRelationGenerator,
  ConstraintType,
  Constraint,
  get_minimal_cover_relationships,
  CoverRelationship,
)


class ConstraintGeneratorTest(unittest.TestCase):

  def test_get_minimal_cover_relationships(self):
    edp_combinations = [
      frozenset(["A"]),
      frozenset(["B"]),
      frozenset(["C"]),
      frozenset(["D"]),
      frozenset(["A", "B"]),
      frozenset(["B", "C"]),
      frozenset(["A", "B", "C"]),
    ]

    cover_relationships = get_minimal_cover_relationships(edp_combinations)

    # Expected covers relations.
    expected_cover_relationships = [
      CoverRelationship(
        target_set=frozenset(["A", "B"]),
        subset_cover=[frozenset(["A"]), frozenset(["B"])],
      ),
      CoverRelationship(
        target_set=frozenset(["B", "C"]),
        subset_cover=[frozenset(["B"]), frozenset(["C"])],
      ),
      CoverRelationship(
        target_set=frozenset(["A", "B", "C"]),
        subset_cover=[frozenset(["A"]), frozenset(["B", "C"])],
      ),
      CoverRelationship(
        target_set=frozenset(["A", "B", "C"]),
        subset_cover=[frozenset(["C"]), frozenset(["A", "B"])],
      ),
      CoverRelationship(
        target_set=frozenset(["A", "B", "C"]),
        subset_cover=[frozenset(["B", "C"]), frozenset(["A", "B"])],
      ),
      CoverRelationship(
        target_set=frozenset(["A", "B", "C"]),
        subset_cover=[frozenset(["A"]), frozenset(["B"]), frozenset(["C"])],
      ),
    ]

    # The output order of get_minimal_cover_relationships is non-deterministic
    # relative to subset_cover contents. We sort the list before comparing.
    actual_covers = []
    for relationship in cover_relationships:
      actual_covers.append(
        CoverRelationship(
          target_set=relationship.target_set,
          subset_cover=sorted(
            relationship.subset_cover, key=lambda x: sorted(list(x))
          ),
        )
      )

    expected_covers = []
    for relationship in expected_cover_relationships:
      expected_covers.append(
        CoverRelationship(
          target_set=relationship.target_set,
          subset_cover=sorted(
            relationship.subset_cover, key=lambda x: sorted(list(x))
          ),
        )
      )

    self.assertCountEqual(actual_covers, expected_covers)

  def test_lower_bound_relation_generator(self):
    metric_set = MetricSet(
      reach=Metric(10.0, 1.0, "reach_1", index=0),
      k_reach={1: Metric(5.0, 0.5, "k_reach_1", index=1)},
      impression=Metric(20.0, 2.0, "impression_1", index=2),
    )
    data_provider_map = DataProviderMetricSetMap(
      {frozenset(["data_provider1"]): metric_set}
    )
    generator = LowerBoundRelationGenerator(
      num_metric_sets=3,
      max_frequency=1,
      data_provider_metric_set=data_provider_map,
    )
    constraints = generator.get_constraints()

    expected_constraints = [
      Constraint(
        coefficients={0: 1},
        type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
        constant=0,
      ),
      Constraint(
        coefficients={1: 1},
        type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
        constant=0,
      ),
      Constraint(
        coefficients={2: 1},
        type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
        constant=0,
      ),
    ]

    self.assertCountEqual(constraints, expected_constraints)

  def test_unnoised_relation_generator(self):
    metric_set1 = MetricSet(
        reach=Metric(10, 0.0, "reach_1", index=0),  # sigma=0
        k_reach={1: Metric(5, 0.5, "k_reach_1", index=1)},
        impression=Metric(20, 0.0, "impression_1", index=2), # sigma=0
    )
    metric_set2 = MetricSet(
        reach=Metric(10.0, 1.0, "reach_1", index=3),
        k_reach={1: Metric(5.0, 0.5, "k_reach_1", index=4)},
        impression=Metric(20.0, 1.0, "impression_1", index=5),
    )
    data_provider_map = DataProviderMetricSetMap(
        {
          frozenset(["data_provider1"]): metric_set1,
          frozenset(["data_provider2"]): metric_set2,
        }
    )
    generator = UnnoisedRelationGenerator(
        num_metric_sets=6,
        max_frequency=1,
        data_provider_metric_set=data_provider_map)
    constraints = generator.get_constraints()

    expected_constraints = [
      Constraint(
        coefficients={0: 1},
        type=ConstraintType.CONSTRAINT_TYPE_EQUAL,
        constant=10,
      ),
      Constraint(
        coefficients={2: 1},
        type=ConstraintType.CONSTRAINT_TYPE_EQUAL,
        constant=20,
      ),
    ]
    self.assertCountEqual(constraints, expected_constraints)

  def test_cover_relation_generator(self):
    metric_set_abc = MetricSet(
      reach=Metric(20.0, 1.0, "reach_abc", index=0),
      k_reach={},
      impression=None,
    )
    metric_set_a = MetricSet(
      reach=Metric(10.0, 1.0, "reach_a", index=1),
      k_reach={},
      impression=None,
    )
    metric_set_b = MetricSet(
      reach=Metric(10.0, 1.0, "reach_b", index=2),
      k_reach={},
      impression=None,
    )
    metric_set_c = MetricSet(
      reach=Metric(10.0, 1.0, "reach_c", index=3),
      k_reach={},
      impression=None,
    )

    data_provider_map = DataProviderMetricSetMap(
      {
        frozenset(["A", "B", "C"]): metric_set_abc,
        frozenset(["A"]): metric_set_a,
        frozenset(["B"]): metric_set_b,
        frozenset(["C"]): metric_set_c,
      }
    )
    cover_relationships = get_minimal_cover_relationships(
      list(data_provider_map.keys())
    )
    generator = CoverRelationGenerator(
      num_metric_sets=4,
      max_frequency=1,
      data_provider_metric_set=data_provider_map,
      cover_relationships=cover_relationships,
    )
    constraints = generator.get_constraints()

    expected_constraints = [
      Constraint(
        coefficients={0: -1, 1: 1, 2: 1, 3: 1},
        type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
        constant=0,
      )
    ]

    self.assertCountEqual(expected_constraints, constraints)

  def test_subset_relation_generator(self):
    parent_metric_set = MetricSet(
      reach=Metric(15.0, 1.0, "reach_parent", index=0),
      k_reach={
        1: Metric(10.0, 1.0, "k_reach_1_parent", index=1),
        2: Metric(5.0, 1.0, "k_reach_2_parent", index=2),
      },
      impression=Metric(30.0, 2.0, "impression_parent", index=3),
    )
    child_metric_set = MetricSet(
      reach=Metric(10.0, 1.0, "reach_child", index=4),
      k_reach={
        1: Metric(7.0, 1.0, "k_reach_1_child", index=5),
        2: Metric(3.0, 1.0, "k_reach_2_child", index=6),
      },
      impression=Metric(20.0, 2.0, "impression_child", index=7),
    )
    data_provider_map = DataProviderMetricSetMap(
      {
        frozenset(["A", "B"]): parent_metric_set,
        frozenset(["A"]): child_metric_set,
      }
    )
    generator = SubsetRelationGenerator(
      num_metric_sets=8,
      max_frequency=2,
      data_provider_metric_set=data_provider_map,
    )
    constraints = generator.get_constraints()

    expected_constraints = [
      # child reach <= parent reach.
      Constraint(
        coefficients={0: 1, 4: -1},
        type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
        constant=0,
      ),
      # child impression <= parent impression.
      Constraint(
        coefficients={3: 1, 7: -1},
        type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
        constant=0,
      ),
      # child 1+ reach <= parent 1+ reach.
      Constraint(
        coefficients={1: 1, 2: 1, 5: -1, 6: -1},
        type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
        constant=0,
      ),
      # child 2+ reach <= parent 2+ reach.
      Constraint(
        coefficients={2: 1, 6: -1},
        type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
        constant=0,
      ),
    ]

    self.assertCountEqual(constraints, expected_constraints)

  def test_subset_relation_generator_no_overlap(self):
    metric_set1 = MetricSet(
      reach=Metric(10, 1, "r1", 0), k_reach={}, impression=None
    )
    metric_set2 = MetricSet(
      reach=Metric(10, 1, "r2", 1), k_reach={}, impression=None
    )
    data_provider_map = DataProviderMetricSetMap(
      {
        frozenset(["A"]): metric_set1,
        frozenset(["B"]): metric_set2,
      }
    )
    generator = SubsetRelationGenerator(2, 1, data_provider_map)
    self.assertEqual(len(generator.get_constraints()), 0)

  def test_subset_relation_generator_missing_reach(self):
    parent_metric_set = MetricSet(
      reach=None, k_reach={}, impression=Metric(30, 1, "impression_parent", 0)
    )
    child_metric_set = MetricSet(
      reach=Metric(10, 1, "reach_child", 1),
      k_reach={},
      impression=Metric(20, 1, "impression_child", 2),
    )
    data_provider_map = DataProviderMetricSetMap(
      {
        frozenset(["A", "B"]): parent_metric_set,
        frozenset(["A"]): child_metric_set,
      }
    )
    generator = SubsetRelationGenerator(3, 1, data_provider_map)
    constraints = generator.get_constraints()
    expected_constraints = [
      Constraint(
        coefficients={0: 1, 2: -1},
        type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
        constant=0,
      )
    ]

    self.assertCountEqual(constraints, expected_constraints)

  def test_subset_relation_generator_missing_impression(self):
    parent_metric_set = MetricSet(
      reach=Metric(15, 1, "reach_parent", 0), k_reach={}, impression=None
    )
    child_metric_set = MetricSet(
      reach=Metric(10, 1, "reach_child", 1),
      k_reach={},
      impression=Metric(20, 1, "impression_child", 2),
    )
    data_provider_map = DataProviderMetricSetMap(
      {
        frozenset(["A", "B"]): parent_metric_set,
        frozenset(["A"]): child_metric_set,
      }
    )
    generator = SubsetRelationGenerator(3, 1, data_provider_map)
    constraints = generator.get_constraints()
    expected_constraints = [
      Constraint(
        coefficients={0: 1, 1: -1},
        type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
        constant=0,
      )
    ]

    self.assertCountEqual(constraints, expected_constraints)

  def test_impressions_relation_generator(self):
    metric_set_ab = MetricSet(
      reach=None, k_reach={}, impression=Metric(30, 1, "impression_ab", 0)
    )
    metric_set_a = MetricSet(
      reach=None, k_reach={}, impression=Metric(10, 1, "impression_a", 1)
    )
    metric_set_b = MetricSet(
      reach=None, k_reach={}, impression=Metric(10, 1, "impression_b", 2)
    )
    data_provider_map = DataProviderMetricSetMap(
      {
        frozenset(["A", "B"]): metric_set_ab,
        frozenset(["A"]): metric_set_a,
        frozenset(["B"]): metric_set_b,
      }
    )
    generator = ImpressionsSumRelationGenerator(3, 1, data_provider_map)
    constraints = generator.get_constraints()

    expected_constraints = [
      Constraint(
        coefficients={0: 1, 1: -1, 2: -1},
        type=ConstraintType.CONSTRAINT_TYPE_EQUAL,
        constant=0,
      )
    ]

    self.assertCountEqual(constraints, expected_constraints)

  def test_impressions_relation_generator_missing_components(self):
    metric_set_ab = MetricSet(
      reach=None, k_reach={}, impression=Metric(30, 1, "impression_ab", 0)
    )
    metric_set_a = MetricSet(
      reach=None, k_reach={}, impression=Metric(10, 1, "impression_a", 1)
    )
    data_provider_map = DataProviderMetricSetMap(
      {
        frozenset(["A", "B"]): metric_set_ab,
        frozenset(["A"]): metric_set_a,
      }
    )
    generator = ImpressionsSumRelationGenerator(2, 1, data_provider_map)

    self.assertEqual(len(generator.get_constraints()), 0)

  def test_reach_frequency_relation_generator(self):
    metric_set = MetricSet(
      reach=Metric(10.0, 1.0, "reach", index=0),
      k_reach={
        1: Metric(6.0, 0.5, "k_reach_1", index=1),
        2: Metric(4.0, 0.5, "k_reach_2", index=2),
      },
      impression=None,
    )
    data_provider_map = DataProviderMetricSetMap({frozenset(["A"]): metric_set})
    generator = ReachFrequencyRelationGenerator(
      num_metric_sets=3,
      max_frequency=2,
      data_provider_metric_set=data_provider_map,
    )
    constraints = generator.get_constraints()

    expected_constraints = [
      Constraint(
        coefficients={0: 1, 1: -1, 2: -1},
        type=ConstraintType.CONSTRAINT_TYPE_EQUAL,
        constant=0,
      )
    ]

    self.assertCountEqual(constraints, expected_constraints)

  def test_reach_frequency_relation_generator_missing_reach(self):
    metric_set = MetricSet(
      reach=None, k_reach={1: Metric(5, 1, "k_reach1", 0)}, impression=None
    )
    data_provider_map = DataProviderMetricSetMap({frozenset(["A"]): metric_set})
    generator = ReachFrequencyRelationGenerator(1, 1, data_provider_map)
    self.assertEqual(len(generator.get_constraints()), 0)

  def test_reach_frequency_relation_generator_missing_frequency(self):
    metric_set = MetricSet(
      reach=Metric(10, 1, "r", 0), k_reach={}, impression=None
    )
    data_provider_map = DataProviderMetricSetMap({frozenset(["A"]): metric_set})
    generator = ReachFrequencyRelationGenerator(1, 1, data_provider_map)
    self.assertEqual(len(generator.get_constraints()), 0)

  def test_reach_impressions_relation_generator(self):
    metric_set = MetricSet(
      reach=Metric(10.0, 1.0, "reach", index=0),
      k_reach={},
      impression=Metric(15.0, 1.0, "impression", index=1),
    )
    data_provider_map = DataProviderMetricSetMap({frozenset(["A"]): metric_set})
    generator = ReachImpressionsRelationGenerator(
      num_metric_sets=2, max_frequency=1, data_provider_metric_set=data_provider_map
    )
    constraints = generator.get_constraints()

    expected_constraints = [
      Constraint(
        coefficients={1: 1, 0: -1},
        type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
        constant=0,
      )
    ]

    self.assertCountEqual(constraints, expected_constraints)

  def test_reach_impressions_relation_generator_missing_reach(self):
    metric_set = MetricSet(reach=None, k_reach={}, impression=Metric(15, 1, "i", 0))
    generator = ReachImpressionsRelationGenerator(
      1, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set})
    )
    self.assertEqual(len(generator.get_constraints()), 0)

  def test_reach_impressions_relation_generator_missing_impression(self):
    metric_set = MetricSet(reach=Metric(10, 1, "r", 0), k_reach={}, impression=None)
    generator = ReachImpressionsRelationGenerator(
      1, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set})
    )
    self.assertEqual(len(generator.get_constraints()), 0)

  def test_frequency_impressions_relation_generator(self):
    metric_set = MetricSet(
      reach=None,
      k_reach={
        1: Metric(5.0, 1.0, "k_reach_1", index=0),
        2: Metric(3.0, 1.0, "k_reach_2", index=1),
      },
      impression=Metric(20.0, 1.0, "impression", index=2),
    )
    data_provider_map = DataProviderMetricSetMap({frozenset(["A"]): metric_set})
    generator = FrequencyImpressionsRelationGenerator(
      num_metric_sets=3, max_frequency=2, data_provider_metric_set=data_provider_map
    )
    constraints = generator.get_constraints()

    expected_constraints = [
      Constraint(
        coefficients={2: 1, 0: -1, 1: -2},
        type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
        constant=0,
      )
    ]

    self.assertCountEqual(constraints, expected_constraints)

  def test_frequency_impressions_relation_generator_missing_k_reach(self):
    metric_set = MetricSet(
      reach=None, k_reach={}, impression=Metric(20, 1, "i", 0)
    )
    generator = FrequencyImpressionsRelationGenerator(
      1, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set})
    )
    self.assertEqual(len(generator.get_constraints()), 0)

  def test_frequency_impressions_relation_generator_missing_impression(self):
    metric_set = MetricSet(
      reach=None, k_reach={1: Metric(5, 1, "k_reach1", 0)}, impression=None
    )
    generator = FrequencyImpressionsRelationGenerator(
      1, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set})
    )
    self.assertEqual(len(generator.get_constraints()), 0)


class ValidateDataProviderMetricSetMapTest(unittest.TestCase):

  def test_validate_data_provider_metric_set_map_empty_input(self):
    with self.assertRaisesRegex(
      ValueError, r"Data provider metric set cannot be None."
    ):
      LowerBoundRelationGenerator(1, 1, DataProviderMetricSetMap({}))

  def test_validate_data_provider_metric_set_map_with_input_none(self):
    with self.assertRaisesRegex(ValueError, r"Metric set cannot be None."):
      LowerBoundRelationGenerator(
        1, 1, DataProviderMetricSetMap({frozenset(["A"]): None})
      )

  def test_validate_data_provider_metric_set_map_with_empty_metric_set(self):
    metric_set = MetricSet(reach=None, k_reach={}, impression=None)
    with self.assertRaisesRegex(
      ValueError,
      r"Metric set must have at least one of reach, impression, or k-reach.",
    ):
      LowerBoundRelationGenerator(
        1, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set})
      )

  def test_validate_data_provider_metric_set_map_negative_value(self):
    metric_set = MetricSet(
      reach=Metric(-1.0, 1.0, "r", 0), k_reach={}, impression=None
    )
    with self.assertRaisesRegex(
      ValueError, r"Metric value -1\.0 must be non-negative\."
    ):
      LowerBoundRelationGenerator(
        1, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set})
      )

  def test_validate_data_provider_metric_set_map_negative_sigma(self):
    metric_set = MetricSet(
      reach=Metric(10.0, -1.0, "r", 0), k_reach={}, impression=None
    )
    with self.assertRaisesRegex(
      ValueError, r"Metric sigma -1\.0 must be non-negative\."
    ):
      LowerBoundRelationGenerator(
        1, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set})
      )

  def test_validate_data_provider_metric_set_map_index_out_of_bounds(self):
    metric_set = MetricSet(
      reach=Metric(10.0, 1.0, "r", 1), k_reach={}, impression=None
    )
    with self.assertRaisesRegex(
      ValueError, r"Metric index 1 must be in \[0, 1\)\."
    ):
      LowerBoundRelationGenerator(
        1, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set})
      )

  def test_validate_data_provider_metric_set_map_invalid_k_reach_length(self):
    metric_set = MetricSet(
      reach=None,
      k_reach={
        1: Metric(5.0, 1.0, "k_reach1", 0),
        2: Metric(3.0, 1.0, "k_reach2", 1),
      },
      impression=None,
    )
    # num_metric_sets=2, max_frequency=1. k_reach has 2 items.
    with self.assertRaisesRegex(ValueError, r"K-reach length 2 must be 1\."):
      LowerBoundRelationGenerator(
        2, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set})
      )


if __name__ == "__main__":
  unittest.main()
