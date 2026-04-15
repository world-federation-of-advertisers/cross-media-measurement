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

from report import (
    Metric,
    MetricSet,
    DataProviderMetricSetMap,
)
from constraint_generator import (
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


class GetMinimalCoverRelationshipTest(unittest.TestCase):
  def test_get_minimal_cover_relationships_empty_edp_combinations(self):
    cover_relationships = get_minimal_cover_relationships([])
    self.assertCountEqual(cover_relationships, [])

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
            subset_cover=[frozenset(["A", "B"]), frozenset(["C"])],
        ),
        CoverRelationship(
            target_set=frozenset(["A", "B", "C"]),
            subset_cover=[frozenset(["A", "B"]), frozenset(["B", "C"])],
        ),
        CoverRelationship(
            target_set=frozenset(["A", "B", "C"]),
            subset_cover=[frozenset(["A"]), frozenset(["B"]), frozenset(["C"])],
        ),
    ]

    self.assertCountEqual(cover_relationships, expected_cover_relationships)


class LowerBoundRelationGeneratorTest(unittest.TestCase):
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
        lower_bound=0,
    )
    constraints = generator.get_constraints()

    expected_constraints = [
        # reach >= 0
        Constraint(
            coefficients={0: 1},
            type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
            constant=0,
        ),
        # k_reach[1] >= 0
        Constraint(
            coefficients={1: 1},
            type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
            constant=0,
        ),
        # impression >= 0
        Constraint(
            coefficients={2: 1},
            type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
            constant=0,
        ),
    ]

    self.assertCountEqual(constraints, expected_constraints)

  def test_lower_bound_relation_generator_reach_only(self):
    metric_set = MetricSet(
        reach=Metric(10.0, 1.0, "reach_1", index=0),
        k_reach={},
        impression=None,
    )
    data_provider_map = DataProviderMetricSetMap(
        {frozenset(["data_provider1"]): metric_set}
    )
    generator = LowerBoundRelationGenerator(
        num_metric_sets=3,
        max_frequency=1,
        data_provider_metric_set=data_provider_map,
        lower_bound=0,
    )
    constraints = generator.get_constraints()

    expected_constraints = [
        # reach >= 0
        Constraint(
            coefficients={0: 1},
            type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
            constant=0,
        ),
    ]

    self.assertCountEqual(constraints, expected_constraints)

  def test_lower_bound_relation_generator_invalid_data(self):
    metric_set = MetricSet(
        reach=None,
        k_reach={},
        impression=None,
    )
    data_provider_map = DataProviderMetricSetMap(
        {frozenset(["data_provider1"]): metric_set}
    )
    with self.assertRaisesRegex(
        ValueError,
        r"Metric set must have at least one of reach, impression, or k-reach."
    ):
      LowerBoundRelationGenerator(
          num_metric_sets=3,
          max_frequency=1,
          data_provider_metric_set=data_provider_map,
          lower_bound=0,
      )

class UnnoisedRelationGeneratorTest(unittest.TestCase):
  def test_unnoised_relation_generator(self):
    metric_set1 = MetricSet(
        reach=Metric(10, 0.0, "reach_1", index=0),  # sigma=0
        k_reach={1: Metric(5, 0.5, "k_reach_1", index=1)},
        impression=Metric(20, 0.0, "impression_1", index=2),  # sigma=0
    )
    metric_set2 = MetricSet(
        reach=Metric(10.0, 1.0, "reach_1", index=3),
        k_reach={1: Metric(5.0, 0.5, "k_reach_1", index=4)},
        impression=Metric(20.0, 1.0, "impression_1", index=5),
    )
    data_provider_map = DataProviderMetricSetMap({
        frozenset(["data_provider1"]): metric_set1,
        frozenset(["data_provider2"]): metric_set2,
    })
    generator = UnnoisedRelationGenerator(
        num_metric_sets=6,
        max_frequency=1,
        data_provider_metric_set=data_provider_map,
    )
    constraints = generator.get_constraints()

    expected_constraints = [
        # reach = 10
        Constraint(
            coefficients={0: 1},
            type=ConstraintType.CONSTRAINT_TYPE_EQUAL,
            constant=10,
        ),
        # impression = 20
        Constraint(
            coefficients={2: 1},
            type=ConstraintType.CONSTRAINT_TYPE_EQUAL,
            constant=20,
        ),
    ]
    self.assertCountEqual(constraints, expected_constraints)

  def test_unnoised_relation_generator_without_unnoised_measurements(self):
    metric_set1 = MetricSet(
        reach=Metric(10, 1.0, "reach_1", index=0),  # sigma=0
        k_reach={1: Metric(5, 0.5, "k_reach_1", index=1)},
        impression=Metric(20, 1.0, "impression_1", index=2),  # sigma=0
    )
    data_provider_map = DataProviderMetricSetMap({
        frozenset(["data_provider1"]): metric_set1,
    })
    generator = UnnoisedRelationGenerator(
        num_metric_sets=6,
        max_frequency=1,
        data_provider_metric_set=data_provider_map,
    )
    self.assertCountEqual(generator.get_constraints(), [])

  def test_unnoised_relation_generator_invalid_data(self):
    metric_set = MetricSet(
        reach=None,
        k_reach={},
        impression=None,
    )
    data_provider_map = DataProviderMetricSetMap(
        {frozenset(["data_provider1"]): metric_set}
    )
    with self.assertRaisesRegex(
        ValueError,
        r"Metric set must have at least one of reach, impression, or k-reach."
    ):
      UnnoisedRelationGenerator(
          num_metric_sets=3,
          max_frequency=1,
          data_provider_metric_set=data_provider_map,
      )

class CoverRelationGeneratorTest(unittest.TestCase):
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

    data_provider_map = DataProviderMetricSetMap({
        frozenset(["A", "B", "C"]): metric_set_abc,
        frozenset(["A"]): metric_set_a,
        frozenset(["B"]): metric_set_b,
        frozenset(["C"]): metric_set_c,
    })
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
        # -reach(abc) + reach(a) + reach(b) + reach(c) >= 0
        Constraint(
            coefficients={0: -1, 1: 1, 2: 1, 3: 1},
            type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
            constant=0,
        )
    ]

    self.assertCountEqual(expected_constraints, constraints)

  def test_cover_relation_generator_without_cover_relations(self):
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

    data_provider_map = DataProviderMetricSetMap({
        frozenset(["A", "B", "C"]): metric_set_abc,
        frozenset(["A"]): metric_set_a,
        frozenset(["B"]): metric_set_b,
    })
    cover_relationships = get_minimal_cover_relationships(
        list(data_provider_map.keys())
    )
    generator = CoverRelationGenerator(
        num_metric_sets=4,
        max_frequency=1,
        data_provider_metric_set=data_provider_map,
        cover_relationships=cover_relationships,
    )

    self.assertCountEqual(cover_relationships, [])
    self.assertCountEqual(generator.get_constraints(), [])

  def test_cover_relation_generator_invalid_data(self):
    metric_set = MetricSet(
        reach=None,
        k_reach={},
        impression=None,
    )
    data_provider_map = DataProviderMetricSetMap(
        {frozenset(["data_provider1"]): metric_set}
    )
    with self.assertRaisesRegex(
        ValueError,
        r"Metric set must have at least one of reach, impression, or k-reach."
    ):
      CoverRelationGenerator(
          num_metric_sets=3,
          max_frequency=1,
          data_provider_metric_set=data_provider_map,
          cover_relationships=[],
      )


class SubsetRelationGeneratorTest(unittest.TestCase):
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
    data_provider_map = DataProviderMetricSetMap({
        frozenset(["A", "B"]): parent_metric_set,
        frozenset(["A"]): child_metric_set,
    })
    generator = SubsetRelationGenerator(
        num_metric_sets=8,
        max_frequency=2,
        data_provider_metric_set=data_provider_map,
    )
    constraints = generator.get_constraints()

    expected_constraints = [
        # reach(A U B) - reach(A) >= 0.
        Constraint(
            coefficients={0: 1, 4: -1},
            type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
            constant=0,
        ),
        # impression(A U B) - impression(A) >= 0.
        Constraint(
            coefficients={3: 1, 7: -1},
            type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
            constant=0,
        ),
        # child 1+ reach <= parent 1+ reach.
        # k_reach(A U B)[1] + k_reach(A U B)[2] - k_reach(A)[1] + k_reach(A)[2] >= 0.
        Constraint(
            coefficients={1: 1, 2: 1, 5: -1, 6: -1},
            type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
            constant=0,
        ),
        # child 2+ reach <= parent 2+ reach.
        # k_reach(A U B)[2] - k_reach(A)[2] >= 0.
        Constraint(
            coefficients={2: 1, 6: -1},
            type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
            constant=0,
        ),
    ]

    self.assertCountEqual(constraints, expected_constraints)

  def test_subset_relation_generator_without_subset_relations(self):
    parent_metric_set = MetricSet(
        reach=None,
        k_reach={},
        impression=Metric(30.0, 2.0, "impression_parent", index=3),
    )
    child_metric_set = MetricSet(
        reach=Metric(10.0, 1.0, "reach_child", index=4),
        k_reach={
            1: Metric(7.0, 1.0, "k_reach_1_child", index=5),
            2: Metric(3.0, 1.0, "k_reach_2_child", index=6),
        },
        impression=None,
    )
    data_provider_map = DataProviderMetricSetMap({
        frozenset(["A", "B"]): parent_metric_set,
        frozenset(["A"]): child_metric_set,
    })
    generator = SubsetRelationGenerator(
        num_metric_sets=8,
        max_frequency=2,
        data_provider_metric_set=data_provider_map,
    )

    self.assertCountEqual(generator.get_constraints(), [])

  def test_subset_relation_generator_no_edp_combinations_overlap(self):
    metric_set1 = MetricSet(
        reach=Metric(10, 1, "r1", 0), k_reach={}, impression=None
    )
    metric_set2 = MetricSet(
        reach=Metric(10, 1, "r2", 1), k_reach={}, impression=None
    )
    data_provider_map = DataProviderMetricSetMap({
        frozenset(["A"]): metric_set1,
        frozenset(["B"]): metric_set2,
    })
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
    data_provider_map = DataProviderMetricSetMap({
        frozenset(["A", "B"]): parent_metric_set,
        frozenset(["A"]): child_metric_set,
    })
    generator = SubsetRelationGenerator(3, 1, data_provider_map)
    constraints = generator.get_constraints()
    expected_constraints = [
        # impression(A U B) - impression(A) >= 0
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
    data_provider_map = DataProviderMetricSetMap({
        frozenset(["A", "B"]): parent_metric_set,
        frozenset(["A"]): child_metric_set,
    })
    generator = SubsetRelationGenerator(3, 1, data_provider_map)
    constraints = generator.get_constraints()
    expected_constraints = [
        # reach(A U B) - reach(A) >= 0
        Constraint(
            coefficients={0: 1, 1: -1},
            type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
            constant=0,
        )
    ]

    self.assertCountEqual(constraints, expected_constraints)

  def test_subset_relation_generator_invalid_data(self):
    parent_metric_set = MetricSet(
        reach=None,
        k_reach={},
        impression=None,
    )
    child_metric_set = MetricSet(
        reach=Metric(10.0, 1.0, "reach_child", index=4),
        k_reach={
            1: Metric(7.0, 1.0, "k_reach_1_child", index=5),
            2: Metric(3.0, 1.0, "k_reach_2_child", index=6),
        },
        impression=None,
    )
    data_provider_map = DataProviderMetricSetMap({
        frozenset(["A", "B"]): parent_metric_set,
        frozenset(["A"]): child_metric_set,
    })
    with self.assertRaisesRegex(
        ValueError,
        r"Metric set must have at least one of reach, impression, or k-reach."
    ):
      SubsetRelationGenerator(
          num_metric_sets=3,
          max_frequency=1,
          data_provider_metric_set=data_provider_map,
      )


class ImpressionsSumRelationGeneratorTest(unittest.TestCase):
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
    data_provider_map = DataProviderMetricSetMap({
        frozenset(["A", "B"]): metric_set_ab,
        frozenset(["A"]): metric_set_a,
        frozenset(["B"]): metric_set_b,
    })
    generator = ImpressionsSumRelationGenerator(3, 1, data_provider_map)
    constraints = generator.get_constraints()

    expected_constraints = [
        # impression(A U B) - impression(A) - impression(B) = 0
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
    data_provider_map = DataProviderMetricSetMap({
        frozenset(["A", "B"]): metric_set_ab,
        frozenset(["A"]): metric_set_a,
    })
    generator = ImpressionsSumRelationGenerator(2, 1, data_provider_map)

    self.assertEqual(len(generator.get_constraints()), 0)

  def test_impressions_relation_generator_invalid_data(self):
    metric_set_ab = MetricSet(
        reach=None, k_reach={}, impression=Metric(30, 1, "impression_ab", 0)
    )
    metric_set_a = MetricSet(
        reach=None, k_reach={}, impression=Metric(10, 1, "impression_a", 1)
    )
    metric_set_b = MetricSet(
        reach=None, k_reach={}, impression=None
    )
    data_provider_map = DataProviderMetricSetMap({
        frozenset(["A", "B"]): metric_set_ab,
        frozenset(["A"]): metric_set_a,
        frozenset(["B"]): metric_set_b,
    })
    with self.assertRaisesRegex(
        ValueError,
        r"Metric set must have at least one of reach, impression, or k-reach."
    ):
      ImpressionsSumRelationGenerator(
          num_metric_sets=3,
          max_frequency=1,
          data_provider_metric_set=data_provider_map,
      )


class ReachFrequencyRelationGeneratorTest(unittest.TestCase):
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
        # reach - k_reach[1] - k_reach[2] = 0
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

  def test_reach_frequency_relation_generator_invalid_data(self):
    metric_set = MetricSet(
        reach=None,
        k_reach={},
        impression=None,
    )
    data_provider_map = DataProviderMetricSetMap({frozenset(["A"]): metric_set})
    with self.assertRaisesRegex(
        ValueError,
        r"Metric set must have at least one of reach, impression, or k-reach."
    ):
      ReachFrequencyRelationGenerator(
          num_metric_sets=3,
          max_frequency=1,
          data_provider_metric_set=data_provider_map,
      )


class ReachImpressionRelationGeneratorTest(unittest.TestCase):
  def test_reach_impressions_relation_generator(self):
    metric_set = MetricSet(
        reach=Metric(10.0, 1.0, "reach", index=0),
        k_reach={},
        impression=Metric(15.0, 1.0, "impression", index=1),
    )
    data_provider_map = DataProviderMetricSetMap({frozenset(["A"]): metric_set})
    generator = ReachImpressionsRelationGenerator(
        num_metric_sets=2,
        max_frequency=1,
        data_provider_metric_set=data_provider_map,
    )
    constraints = generator.get_constraints()

    expected_constraints = [
        # impression - reach >= 0
        Constraint(
            coefficients={1: 1, 0: -1},
            type=ConstraintType.CONSTRAINT_TYPE_GREATER_THAN_OR_EQUAL,
            constant=0,
        )
    ]

    self.assertCountEqual(constraints, expected_constraints)

  def test_reach_impressions_relation_generator_missing_reach(self):
    metric_set = MetricSet(
        reach=None, k_reach={}, impression=Metric(15, 1, "i", 0)
    )
    generator = ReachImpressionsRelationGenerator(
        1, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set})
    )
    self.assertEqual(len(generator.get_constraints()), 0)

  def test_reach_impressions_relation_generator_missing_impression(self):
    metric_set = MetricSet(
        reach=Metric(10, 1, "r", 0), k_reach={}, impression=None
    )
    generator = ReachImpressionsRelationGenerator(
        1, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set})
    )
    self.assertEqual(len(generator.get_constraints()), 0)

  def test_reach_impressions_relation_generator_invalid_data(self):
    metric_set = MetricSet(
        reach=None,
        k_reach={},
        impression=None,
    )
    data_provider_map = DataProviderMetricSetMap({frozenset(["A"]): metric_set})
    with self.assertRaisesRegex(
        ValueError,
        r"Metric set must have at least one of reach, impression, or k-reach."
    ):
      ReachImpressionsRelationGenerator(
          num_metric_sets=3,
          max_frequency=1,
          data_provider_metric_set=data_provider_map,
      )


class FrequencyImpressionRelationGeneratorTest(unittest.TestCase):
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
        num_metric_sets=3,
        max_frequency=2,
        data_provider_metric_set=data_provider_map,
    )
    constraints = generator.get_constraints()

    expected_constraints = [
        # impression - k_reach[1] - 2*k_reach[2] >= 0
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

  def test_frequency_impressions_relation_generator_invalid_data(self):
    metric_set = MetricSet(
        reach=None,
        k_reach={},
        impression=None,
    )
    data_provider_map = DataProviderMetricSetMap({frozenset(["A"]): metric_set})
    with self.assertRaisesRegex(
        ValueError,
        r"Metric set must have at least one of reach, impression, or k-reach."
    ):
      FrequencyImpressionsRelationGenerator(
          num_metric_sets=3,
          max_frequency=1,
          data_provider_metric_set=data_provider_map,
      )


class ValidateDataProviderMetricSetMapTest(unittest.TestCase):

  def test_validate_data_provider_metric_set_map_empty_input(self):
    with self.assertRaisesRegex(
        ValueError, r"Data provider metric set cannot be None."
    ):
      LowerBoundRelationGenerator(1, 1, DataProviderMetricSetMap({}), 0)

  def test_validate_data_provider_metric_set_map_with_input_none(self):
    with self.assertRaisesRegex(ValueError, r"Metric set cannot be None."):
      LowerBoundRelationGenerator(
          1, 1, DataProviderMetricSetMap({frozenset(["A"]): None}), 0
      )

  def test_validate_data_provider_metric_set_map_with_empty_metric_set(self):
    metric_set = MetricSet(reach=None, k_reach={}, impression=None)
    with self.assertRaisesRegex(
        ValueError,
        r"Metric set must have at least one of reach, impression, or k-reach.",
    ):
      LowerBoundRelationGenerator(
          1, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set}), 0
      )

  def test_validate_data_provider_metric_set_map_negative_value(self):
    metric_set = MetricSet(
        reach=Metric(-1.0, 1.0, "r", 0), k_reach={}, impression=None
    )
    with self.assertRaisesRegex(
        ValueError, r"Metric value -1\.0 must be non-negative\."
    ):
      LowerBoundRelationGenerator(
          1, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set}), 0
      )

  def test_validate_data_provider_metric_set_map_negative_sigma(self):
    metric_set = MetricSet(
        reach=Metric(10.0, -1.0, "r", 0), k_reach={}, impression=None
    )
    with self.assertRaisesRegex(
        ValueError, r"Metric sigma -1\.0 must be non-negative\."
    ):
      LowerBoundRelationGenerator(
          1, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set}), 0
      )

  def test_validate_data_provider_metric_set_map_index_out_of_bounds(self):
    metric_set = MetricSet(
        reach=Metric(10.0, 1.0, "r", 1), k_reach={}, impression=None
    )
    with self.assertRaisesRegex(
        ValueError, r"Metric index 1 must be in \[0, 1\)\."
    ):
      LowerBoundRelationGenerator(
          1, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set}), 0
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
          2, 1, DataProviderMetricSetMap({frozenset(["A"]): metric_set}), 0
      )


if __name__ == "__main__":
  unittest.main()
