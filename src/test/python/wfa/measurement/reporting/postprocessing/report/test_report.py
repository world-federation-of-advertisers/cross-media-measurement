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

from noiseninja.noised_measurements import Measurement, SetMeasurementsSpec
from report.report import Report, MetricReport, is_cover, get_covers

EXPECTED_PRECISION = 3
EDP_ONE = "EDP_ONE"
EDP_TWO = "EDP_TWO"
EDP_THREE = "EDP_THREE"

SAMPLE_REPORT = Report(
    metric_reports={
        "ami": MetricReport(
            reach_time_series={
                frozenset({EDP_ONE}): [Measurement(1, 0, "measurement_01"),
                                       Measurement(1, 0, "measurement_02")],
                frozenset({EDP_TWO}): [Measurement(1, 0, "measurement_03"),
                                       Measurement(1, 0, "measurement_04")],
                frozenset({EDP_THREE}): [
                    Measurement(1, 0, "measurement_05"),
                    Measurement(1, 0, "measurement_06")],
                frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                    Measurement(1, 0, "measurement_07"),
                    Measurement(1, 0, "measurement_08")],
            },
            reach_whole_campaign={
                frozenset({EDP_ONE}): Measurement(1, 0, "measurement_09"),
                frozenset({EDP_TWO}): Measurement(1, 0, "measurement_10"),
                frozenset({EDP_THREE}):
                  Measurement(1, 0, "measurement_11"),
                frozenset({EDP_ONE, EDP_TWO}):
                  Measurement(1, 0, "measurement_12"),
                frozenset({EDP_ONE, EDP_TWO, EDP_THREE}):
                  Measurement(1, 0, "measurement_13"),
            },
        ),
        "mrc": MetricReport(
            reach_time_series={
                frozenset({EDP_ONE}): [Measurement(1, 0, "measurement_14"),
                                       Measurement(1, 0, "measurement_15")],
                frozenset({EDP_TWO}): [Measurement(1, 0, "measurement_16"),
                                       Measurement(1, 0, "measurement_17")],
                frozenset({EDP_THREE}): [
                    Measurement(1, 0, "measurement_18"),
                    Measurement(1, 0, "measurement_19")],
                frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                    Measurement(1, 0, "measurement_20"),
                    Measurement(1, 0, "measurement_21")],
            },
            reach_whole_campaign={
                frozenset({EDP_ONE}): Measurement(1, 0, "measurement_22"),
                frozenset({EDP_TWO}): Measurement(1, 0, "measurement_23"),
                frozenset({EDP_THREE}):
                  Measurement(1, 0, "measurement_24"),
                frozenset({EDP_TWO, EDP_THREE}):
                  Measurement(1, 0, "measurement_25"),
            },
        )
    },
    metric_subsets_by_parent={"ami": ["mrc"]},
    cumulative_inconsistency_allowed_edp_combinations={},
)


class TestReport(unittest.TestCase):
  def test_is_cover_returns_true_for_valid_cover_sets(self):
    self.assertTrue(is_cover(frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
                             (frozenset({"EDP_ONE"}), frozenset({"EDP_TWO"}),
                              frozenset({"EDP_THREE"}))))
    self.assertTrue(is_cover(frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
                             (frozenset({"EDP_ONE"}), frozenset({"EDP_TWO"}),
                              frozenset({"EDP_THREE"}),
                              frozenset({"EDP_ONE", "EDP_TWO"}))))

  def test_is_cover_returns_false_for_invalid_cover_sets(self):
    self.assertFalse(is_cover(frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
                              (frozenset({"EDP_ONE"}),
                               frozenset({"EDP_THREE"}))))

  def test_get_cover_returns_all_cover_sets(self):
    target = frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"})
    other_sets = (frozenset({"EDP_ONE"}), frozenset({"EDP_TWO"}),
                  frozenset({"EDP_THREE"}),
                  frozenset({"EDP_ONE", "EDP_TWO"}))

    expected = [
        (
            frozenset({'EDP_TWO', 'EDP_THREE', 'EDP_ONE'}),
            (frozenset({'EDP_THREE'}), frozenset({'EDP_TWO', 'EDP_ONE'}))
        ),
        (
            frozenset({'EDP_TWO', 'EDP_THREE', 'EDP_ONE'}), (
                frozenset({'EDP_ONE'}), frozenset({'EDP_TWO'}),
                frozenset({'EDP_THREE'}))
        ),
        (
            frozenset({'EDP_TWO', 'EDP_THREE', 'EDP_ONE'}), (
                frozenset({'EDP_ONE'}), frozenset({'EDP_THREE'}),
                frozenset({'EDP_TWO', 'EDP_ONE'}))
        ),
        (
            frozenset({'EDP_TWO', 'EDP_THREE', 'EDP_ONE'}), (
                frozenset({'EDP_TWO'}), frozenset({'EDP_THREE'}),
                frozenset({'EDP_TWO', 'EDP_ONE'}))
        ),
        (
            frozenset({'EDP_TWO', 'EDP_THREE', 'EDP_ONE'}), (
                frozenset({'EDP_ONE'}), frozenset({'EDP_TWO'}),
                frozenset({'EDP_THREE'}), frozenset({'EDP_TWO', 'EDP_ONE'}))
        )
    ]

    cover_relationship = get_covers(target, other_sets)
    self.assertEqual(expected, cover_relationship)

  def test_get_cover_relationships(self):
    metric_report = MetricReport(
        reach_time_series={
            frozenset({EDP_ONE}): [Measurement(1, 0, "measurement_01")],
            frozenset({EDP_TWO}): [Measurement(1, 0, "measurement_02")],
            frozenset({EDP_THREE}): [Measurement(1, 0, "measurement_03")],
            frozenset({EDP_ONE, EDP_TWO}): [
                Measurement(1, 0, "measurement_04")],
            frozenset({EDP_TWO, EDP_THREE}): [
                Measurement(1, 0, "measurement_05")],
            frozenset({EDP_ONE, EDP_THREE}): [
                Measurement(1, 0, "measurement_06")],
            frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                Measurement(1, 0, "measurement_07")],
        },
        reach_whole_campaign={},
    )

    expected = [
        (
            frozenset({"EDP_ONE", "EDP_TWO"}),
            (frozenset({"EDP_ONE"}), frozenset({"EDP_TWO"})),
        ),
        (
            frozenset({"EDP_TWO", "EDP_THREE"}),
            (frozenset({"EDP_TWO"}), frozenset({"EDP_THREE"})),
        ),
        (
            frozenset({"EDP_ONE", "EDP_THREE"}),
            (frozenset({"EDP_ONE"}), frozenset({"EDP_THREE"})),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (frozenset({"EDP_ONE"}), frozenset({"EDP_TWO", "EDP_THREE"})),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (frozenset({"EDP_TWO"}), frozenset({"EDP_ONE", "EDP_THREE"})),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (frozenset({"EDP_THREE"}), frozenset({"EDP_ONE", "EDP_TWO"})),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
        (
            frozenset({"EDP_ONE", "EDP_TWO", "EDP_THREE"}),
            (
                frozenset({"EDP_ONE"}),
                frozenset({"EDP_TWO"}),
                frozenset({"EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_TWO"}),
                frozenset({"EDP_TWO", "EDP_THREE"}),
                frozenset({"EDP_ONE", "EDP_THREE"}),
            ),
        ),
    ]
    self.assertEqual(metric_report.get_cumulative_cover_relationships(),
                     expected)

  def test_add_cover_relationships(self):
    report = SAMPLE_REPORT
    name_to_index = report._measurement_name_to_index

    expected_covers_by_set = {
        name_to_index["measurement_07"]: [
            [name_to_index["measurement_01"],
             name_to_index["measurement_03"],
             name_to_index["measurement_05"]]
        ],
        name_to_index["measurement_08"]: [
            [name_to_index["measurement_02"],
             name_to_index["measurement_04"],
             name_to_index["measurement_06"]]
        ],
        name_to_index["measurement_12"]: [
            [name_to_index["measurement_09"],
             name_to_index["measurement_10"]]
        ],
        name_to_index["measurement_13"]: [
            [name_to_index["measurement_11"],
             name_to_index["measurement_12"]],
            [name_to_index["measurement_09"],
             name_to_index["measurement_10"],
             name_to_index["measurement_11"]],
            [name_to_index["measurement_09"],
             name_to_index["measurement_11"],
             name_to_index["measurement_12"]],
            [name_to_index["measurement_10"],
             name_to_index["measurement_11"],
             name_to_index["measurement_12"]],
            [name_to_index["measurement_10"],
             name_to_index["measurement_09"],
             name_to_index["measurement_11"],
             name_to_index["measurement_12"]]
        ],
        name_to_index["measurement_20"]: [
            [name_to_index["measurement_14"],
             name_to_index["measurement_16"],
             name_to_index["measurement_18"]]
        ],
        name_to_index["measurement_21"]: [
            [name_to_index["measurement_15"],
             name_to_index["measurement_17"],
             name_to_index["measurement_19"]]
        ],
        name_to_index["measurement_25"]: [
            [name_to_index["measurement_23"],
             name_to_index["measurement_24"]]
        ],
    }

    spec = SetMeasurementsSpec()
    report._add_cover_relations_to_spec(spec)
    self.assertEqual(len(spec._subsets_by_set), 0)
    self.assertEqual(expected_covers_by_set.keys(), spec._covers_by_set.keys())
    for key in spec._covers_by_set.keys():
      self.assertEqual({tuple(sorted(inner_list)) for inner_list in
                        expected_covers_by_set[key]},
                       {tuple(sorted(inner_list)) for inner_list in
                        spec._covers_by_set[key]})

  def test_add_subset_relationships(self):
    report = SAMPLE_REPORT
    name_to_index = report._measurement_name_to_index

    expected_subsets_by_set = {
        name_to_index["measurement_07"]: [name_to_index["measurement_01"],
                                          name_to_index["measurement_03"],
                                          name_to_index["measurement_05"]],
        name_to_index["measurement_08"]: [name_to_index["measurement_02"],
                                          name_to_index["measurement_04"],
                                          name_to_index["measurement_06"]],
        name_to_index["measurement_12"]: [name_to_index["measurement_09"],
                                          name_to_index["measurement_10"]],
        name_to_index["measurement_13"]: [name_to_index["measurement_09"],
                                          name_to_index["measurement_10"],
                                          name_to_index["measurement_11"],
                                          name_to_index["measurement_12"]],
        name_to_index["measurement_20"]: [name_to_index["measurement_14"],
                                          name_to_index["measurement_16"],
                                          name_to_index["measurement_18"]],
        name_to_index["measurement_21"]: [name_to_index["measurement_15"],
                                          name_to_index["measurement_17"],
                                          name_to_index["measurement_19"]],
        name_to_index["measurement_25"]: [name_to_index["measurement_23"],
                                          name_to_index["measurement_24"]],
    }

    spec = SetMeasurementsSpec()
    report._add_subset_relations_to_spec(spec)

    self.assertEqual(len(spec._covers_by_set), 0)
    self.assertEqual(expected_subsets_by_set.keys(),
                     spec._subsets_by_set.keys())
    for key in spec._subsets_by_set.keys():
      self.assertEqual(sorted(expected_subsets_by_set[key]),
                       sorted(spec._subsets_by_set[key]))

  def test_add_cumulative_subset_relationships(self):
    report = SAMPLE_REPORT
    name_to_index = report._measurement_name_to_index

    expected_subsets_by_set = {
        name_to_index["measurement_02"]: [name_to_index["measurement_01"]],
        name_to_index["measurement_04"]: [name_to_index["measurement_03"]],
        name_to_index["measurement_06"]: [name_to_index["measurement_05"]],
        name_to_index["measurement_08"]: [name_to_index["measurement_07"]],
        name_to_index["measurement_15"]: [name_to_index["measurement_14"]],
        name_to_index["measurement_17"]: [name_to_index["measurement_16"]],
        name_to_index["measurement_19"]: [name_to_index["measurement_18"]],
        name_to_index["measurement_21"]: [name_to_index["measurement_20"]],
    }

    spec = SetMeasurementsSpec()
    report._add_cumulative_relations_to_spec(spec)

    self.assertEqual(len(spec._covers_by_set), 0)
    self.assertEqual(expected_subsets_by_set.keys(),
                     spec._subsets_by_set.keys())
    for key in spec._subsets_by_set.keys():
      self.assertEqual(sorted(expected_subsets_by_set[key]),
                       sorted(spec._subsets_by_set[key]))

  def test_add_metric_relationships(self):
    report = SAMPLE_REPORT
    name_to_index = report._measurement_name_to_index

    expected_subsets_by_set = {
        name_to_index["measurement_01"]: [name_to_index["measurement_14"]],
        name_to_index["measurement_02"]: [name_to_index["measurement_15"]],
        name_to_index["measurement_03"]: [name_to_index["measurement_16"]],
        name_to_index["measurement_04"]: [name_to_index["measurement_17"]],
        name_to_index["measurement_05"]: [name_to_index["measurement_18"]],
        name_to_index["measurement_06"]: [name_to_index["measurement_19"]],
        name_to_index["measurement_07"]: [name_to_index["measurement_20"]],
        name_to_index["measurement_08"]: [name_to_index["measurement_21"]],
        name_to_index["measurement_09"]: [name_to_index["measurement_22"]],
        name_to_index["measurement_10"]: [name_to_index["measurement_23"]],
        name_to_index["measurement_11"]: [name_to_index["measurement_24"]],
    }

    spec = SetMeasurementsSpec()
    report._add_metric_relations_to_spec(spec)

    self.assertEqual(len(spec._covers_by_set), 0)
    self.assertEqual(expected_subsets_by_set.keys(),
                     spec._subsets_by_set.keys())
    for key in spec._subsets_by_set.keys():
      self.assertEqual(sorted(expected_subsets_by_set[key]),
                       sorted(spec._subsets_by_set[key]))

  def test_add_cumulative_whole_campaign_relationships(self):
    report = SAMPLE_REPORT
    name_to_index = report._measurement_name_to_index

    expected_subsets_by_set = {
        name_to_index["measurement_09"]: [name_to_index["measurement_02"]],
        name_to_index["measurement_10"]: [name_to_index["measurement_04"]],
        name_to_index["measurement_11"]: [name_to_index["measurement_06"]],
        name_to_index["measurement_13"]: [name_to_index["measurement_08"]],
        name_to_index["measurement_22"]: [name_to_index["measurement_15"]],
        name_to_index["measurement_23"]: [name_to_index["measurement_17"]],
        name_to_index["measurement_24"]: [name_to_index["measurement_19"]],
    }

    spec = SetMeasurementsSpec()
    report._add_cumulative_whole_campaign_relations_to_spec(spec)

    self.assertEqual(len(spec._covers_by_set), 0)
    self.assertEqual(expected_subsets_by_set.keys(),
                     spec._subsets_by_set.keys())
    for key in spec._subsets_by_set.keys():
      self.assertEqual(sorted(expected_subsets_by_set[key]),
                       sorted(spec._subsets_by_set[key]))

  def test_get_corrected_single_metric_report(self):
    ami = "ami"

    report = Report(
        metric_reports={
            ami: MetricReport(
                reach_time_series={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(50, 1, "measurement_01")],
                    frozenset({EDP_ONE}): [
                        Measurement(48, 0, "measurement_02")],
                    frozenset({EDP_TWO}): [Measurement(1, 1, "measurement_03")],
                },
                reach_whole_campaign={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    # The corrected report should be consistent:
    # 1. reach[edp1][0] <= reach[edp1 U edp2][0]
    # 2. reach[edp2][0] <= reach[edp1 U edp2][0]
    # 3. reach[edp1 U edp2][0] <= reach[edp1][0] + reach[edp2][0].
    corrected = report.get_corrected_report()

    expected = Report(
        metric_reports={
            ami: MetricReport(
                reach_time_series={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(49.5, 1, "measurement_01")],
                    frozenset({EDP_ONE}): [
                        Measurement(48, 0, "measurement_02")],
                    frozenset({EDP_TWO}): [
                        Measurement(1.5, 1, "measurement_03")],
                },
                reach_whole_campaign={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_can_correct_time_series(self):
    ami = "ami"
    report = Report(
        metric_reports={
            ami: MetricReport(
                reach_time_series={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(0.00, 1, "measurement_01"),
                        Measurement(3.30, 1, "measurement_02"),
                        Measurement(0.00, 1, "measurement_03"),
                    ],
                    frozenset({EDP_ONE}): [
                        Measurement(0.00, 1, "measurement_04"),
                        Measurement(3.30, 1, "measurement_05"),
                        Measurement(0.00, 1, "measurement_06"),
                    ],
                },
                reach_whole_campaign={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    # The corrected report should be consistent:
    # 1. All the time series reaches are monotonic increasing, e.g.
    # reach[edp1][i] <= reach[edp1][i+1].
    # 2. Reach of the child set is less than or equal to reach of the parent set
    # for all period, e.g. reach[edp1][i] <= reach[edp1 U edp2][i].
    corrected = report.get_corrected_report()

    expected = Report(
        metric_reports={
            ami: MetricReport(
                reach_time_series={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(0.00, 1, "measurement_01"),
                        Measurement(1.65, 1, "measurement_02"),
                        Measurement(1.65, 1, "measurement_03"),
                    ],
                    frozenset({EDP_ONE}): [
                        Measurement(0.00, 1, "measurement_04"),
                        Measurement(1.65, 1, "measurement_05"),
                        Measurement(1.65, 1, "measurement_06"),
                    ],
                },
                reach_whole_campaign={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_can_correct_time_series_for_three_edps(self):
    ami = "ami"
    report = Report(
        metric_reports={
            ami: MetricReport(
                reach_time_series={
                    # 1 way comb
                    frozenset({EDP_ONE}): [
                        Measurement(0.00, 1, "measurement_01"),
                        Measurement(3.30, 1, "measurement_02"),
                        Measurement(4.00, 1, "measurement_03"),
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(0.00, 1, "measurement_04"),
                        Measurement(2.30, 1, "measurement_05"),
                        Measurement(3.00, 1, "measurement_06"),
                    ],
                    frozenset({EDP_THREE}): [
                        Measurement(1.00, 1, "measurement_07"),
                        Measurement(3.30, 1, "measurement_08"),
                        Measurement(5.00, 1, "measurement_09"),
                    ],
                    # 2 way combs
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(0.00, 1, "measurement_10"),
                        Measurement(5.30, 1, "measurement_11"),
                        Measurement(6.90, 1, "measurement_12"),
                    ],
                    frozenset({EDP_TWO, EDP_THREE}): [
                        Measurement(0.70, 1, "measurement_13"),
                        Measurement(6.30, 1, "measurement_14"),
                        Measurement(9.00, 1, "measurement_15"),
                    ],
                    frozenset({EDP_ONE, EDP_THREE}): [
                        Measurement(1.20, 1, "measurement_16"),
                        Measurement(7.00, 1, "measurement_17"),
                        Measurement(8.90, 1, "measurement_18"),
                    ],
                    # 3 way comb
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                        Measurement(1.10, 1, "measurement_19"),
                        Measurement(8.0, 1, "measurement_20"),
                        Measurement(11.90, 1, "measurement_21"),
                    ],
                },
                reach_whole_campaign={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    # The corrected report should be consistent:
    # 1. All the time series reaches are monotonic increasing, e.g.
    # reach[edp1][i] <= reach[edp1][i+1].
    # 2. Reach of the cover set is less than or equal to the sum of reach of
    # sets it covers. For example: for each period i it is true that
    # reach[edp1 U edp2][i] <= reach[edp1][i] + reach[edp2][i],
    # or reach[edp1 U edp2 U edp3][i] <= reach[edp1 U edp2][i] + reach[edp3][i],
    # etc.
    # 3. Reach of the child set is less than or equal to reach of the parent set
    # for all period, e.g. reach[edp1][i] <= reach[edp1 U edp2][i].
    corrected = report.get_corrected_report()

    expected = Report(
        metric_reports={
            ami: MetricReport(
                reach_time_series={
                    # 1 way comb
                    frozenset({EDP_ONE}): [
                        Measurement(0.10, 1.00, "measurement_01"),
                        Measurement(3.362, 1.00, "measurement_02"),
                        Measurement(4.00, 1.00, "measurement_03"),
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(0.00, 1.00, "measurement_04"),
                        Measurement(2.512, 1.00, "measurement_05"),
                        Measurement(3.3333, 1.00, "measurement_06"),
                    ],
                    frozenset({EDP_THREE}): [
                        Measurement(0.95, 1.00, "measurement_07"),
                        Measurement(3.5749, 1.00, "measurement_08"),
                        Measurement(5.3333, 1.00, "measurement_09"),
                    ],
                    # 2 way combs
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(0.10, 1.00, "measurement_10"),
                        Measurement(5.30, 1.00, "measurement_11"),
                        Measurement(6.90, 1.00, "measurement_12"),
                    ],
                    frozenset({EDP_TWO, EDP_THREE}): [
                        Measurement(0.95, 1.00, "measurement_13"),
                        Measurement(6.087, 1.00, "measurement_14"),
                        Measurement(8.66666, 1.00, "measurement_15"),
                    ],
                    frozenset({EDP_ONE, EDP_THREE}): [
                        Measurement(1.05, 1.00, "measurement_16"),
                        Measurement(6.937, 1.00, "measurement_17"),
                        Measurement(8.90, 1.00, "measurement_18"),
                    ],
                    # 3 way comb
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                        Measurement(1.05, 1.00, "measurement_19"),
                        Measurement(8.00, 1.00, "measurement_20"),
                        Measurement(11.90, 1.00, "measurement_21"),
                    ],
                },
                reach_whole_campaign={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_correct_report_with_both_time_series_and_whole_campaign_measurements_three_edps(
      self):
    ami = "ami"

    report = Report(
        metric_reports={
            ami: MetricReport(
                reach_time_series={
                    # 1 way comb
                    frozenset({EDP_ONE}): [
                        Measurement(0.00, 1, "measurement_01"),
                        Measurement(3.30, 1, "measurement_02"),
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(0.00, 1, "measurement_04"),
                        Measurement(2.30, 1, "measurement_05"),
                    ],
                    frozenset({EDP_THREE}): [
                        Measurement(1.00, 1, "measurement_07"),
                        Measurement(3.30, 1, "measurement_08"),
                    ],
                    # 2 way combs
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(0.00, 1, "measurement_10"),
                        Measurement(5.30, 1, "measurement_11"),
                    ],
                    frozenset({EDP_TWO, EDP_THREE}): [
                        Measurement(0.70, 1, "measurement_13"),
                        Measurement(6.30, 1, "measurement_14"),
                    ],
                    frozenset({EDP_ONE, EDP_THREE}): [
                        Measurement(1.20, 1, "measurement_16"),
                        Measurement(7.00, 1, "measurement_17"),
                    ],
                    # 3 way comb
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                        Measurement(1.10, 1, "measurement_19"),
                        Measurement(8.0, 1, "measurement_20"),
                    ],
                },
                reach_whole_campaign={
                    # 1 way comb
                    frozenset({EDP_ONE}):
                      Measurement(4.00, 1.00, "measurement_03"),
                    frozenset({EDP_TWO}):
                      Measurement(3.3333, 1.00, "measurement_06"),
                    frozenset({EDP_THREE}):
                      Measurement(5.3333, 1.00, "measurement_09"),
                    # 2 way combs
                    frozenset({EDP_ONE, EDP_TWO}):
                      Measurement(6.90, 1.00, "measurement_12"),
                    frozenset({EDP_TWO, EDP_THREE}):
                      Measurement(8.66666, 1.00, "measurement_15"),
                    frozenset({EDP_ONE, EDP_THREE}):
                      Measurement(8.90, 1.00, "measurement_18"),
                    # 3 way comb
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}):
                      Measurement(11.90, 1.00, "measurement_21"),
                }
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    # The corrected report should be consistent:
    # 1. All the time series reaches are monotonic increasing, e.g.
    # reach[edp1][i] <= reach[edp1][i+1].
    # 2. Reach of the cover set is less than or equal to the sum of reach of
    # sets it covers. For example: for each period i it is true that
    # reach[edp1 U edp2][i] <= reach[edp1][i] + reach[edp2][i],
    # or reach[edp1 U edp2 U edp3][i] <= reach[edp1 U edp2][i] + reach[edp3][i],
    # etc.
    # 3. Reach of the child set is less than or equal to reach of the parent set
    # for all period, e.g. reach[edp1][i] <= reach[edp1 U edp2][i].
    # 4. Time series reaches are less than or equal to whole campaign reach,
    # e.g. cumulative_reach[edp1][1] <= whole_campaign_reach[edp1].
    corrected = report.get_corrected_report()

    expected = Report(
        metric_reports={
            ami: MetricReport(
                reach_time_series={
                    # 1 way comb
                    frozenset({EDP_ONE}): [
                        Measurement(0.10, 1.00, "measurement_01"),
                        Measurement(3.362, 1.00, "measurement_02"),
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(0.00, 1.00, "measurement_04"),
                        Measurement(2.512, 1.00, "measurement_05"),
                    ],
                    frozenset({EDP_THREE}): [
                        Measurement(0.95, 1.00, "measurement_07"),
                        Measurement(3.5749, 1.00, "measurement_08"),
                    ],
                    # 2 way combs
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(0.10, 1.00, "measurement_10"),
                        Measurement(5.30, 1.00, "measurement_11"),
                    ],
                    frozenset({EDP_TWO, EDP_THREE}): [
                        Measurement(0.95, 1.00, "measurement_13"),
                        Measurement(6.087, 1.00, "measurement_14"),
                    ],
                    frozenset({EDP_ONE, EDP_THREE}): [
                        Measurement(1.05, 1.00, "measurement_16"),
                        Measurement(6.937, 1.00, "measurement_17"),
                    ],
                    # 3 way comb
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                        Measurement(1.05, 1.00, "measurement_19"),
                        Measurement(8.00, 1.00, "measurement_20"),
                    ],
                },
                reach_whole_campaign={
                    # 1 way comb
                    frozenset({EDP_ONE}):
                      Measurement(4.00, 1.00, "measurement_03"),
                    frozenset({EDP_TWO}):
                      Measurement(3.3333, 1.00, "measurement_06"),
                    frozenset({EDP_THREE}):
                      Measurement(5.3333, 1.00, "measurement_09"),
                    # 2 way combs
                    frozenset({EDP_ONE, EDP_TWO}):
                      Measurement(6.90, 1.00, "measurement_12"),
                    frozenset({EDP_TWO, EDP_THREE}):
                      Measurement(8.66666, 1.00, "measurement_15"),
                    frozenset({EDP_ONE, EDP_THREE}):
                      Measurement(8.90, 1.00, "measurement_18"),
                    # 3 way comb
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}):
                      Measurement(11.90, 1.00, "measurement_21"),
                },
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_correct_report_with_whole_campaign_has_more_edp_combinations(self):
    ami = "ami"

    report = Report(
        metric_reports={
            ami: MetricReport(
                reach_time_series={
                    # 1 way comb
                    frozenset({EDP_ONE}): [
                        Measurement(0.00, 1, "measurement_01"),
                        Measurement(3.30, 1, "measurement_02"),
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(0.00, 1, "measurement_04"),
                        Measurement(2.30, 1, "measurement_05"),
                    ],
                    frozenset({EDP_THREE}): [
                        Measurement(1.00, 1, "measurement_07"),
                        Measurement(3.30, 1, "measurement_08"),
                    ],
                    # 3 way comb
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                        Measurement(1.10, 1, "measurement_19"),
                        Measurement(8.0, 1, "measurement_20"),
                    ],
                },
                reach_whole_campaign={
                    # 1 way comb
                    frozenset({EDP_ONE}):
                      Measurement(4.00, 1.00, "measurement_03"),
                    frozenset({EDP_TWO}):
                      Measurement(3.3333, 1.00, "measurement_06"),
                    frozenset({EDP_THREE}):
                      Measurement(5.3333, 1.00, "measurement_09"),
                    # 2 way combs
                    frozenset({EDP_ONE, EDP_TWO}):
                      Measurement(6.90, 1.00, "measurement_12"),
                    frozenset({EDP_TWO, EDP_THREE}):
                      Measurement(8.66666, 1.00, "measurement_15"),
                    frozenset({EDP_ONE, EDP_THREE}):
                      Measurement(8.90, 1.00, "measurement_18"),
                    # 3 way comb
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}):
                      Measurement(11.90, 1.00, "measurement_21"),
                },
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    # The corrected report should be consistent between time series reaches and
    # whole campaign reach: time series reaches are less than or equal to whole
    # campaign reach, e.g. cumulative_reach[edp1][1] <=
    # whole_campaign_reach[edp1].
    corrected = report.get_corrected_report()

    expected = Report(
        metric_reports={
            ami: MetricReport(
                reach_time_series={
                    # 1 way comb
                    frozenset({EDP_ONE}): [
                        Measurement(0.025, 1.00, "measurement_01"),
                        Measurement(3.30, 1.00, "measurement_02"),
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(0.025, 1.00, "measurement_04"),
                        Measurement(2.30, 1.00, "measurement_05"),
                    ],
                    frozenset({EDP_THREE}): [
                        Measurement(1.025, 1.00, "measurement_07"),
                        Measurement(3.30, 1.00, "measurement_08"),
                    ],
                    # 3 way comb
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                        Measurement(1.075, 1.00, "measurement_19"),
                        Measurement(8.00, 1.00, "measurement_20"),
                    ],
                },
                reach_whole_campaign={
                    # 1 way comb
                    frozenset({EDP_ONE}):
                      Measurement(4.00, 1.00, "measurement_03"),
                    frozenset({EDP_TWO}):
                      Measurement(3.3333, 1.00, "measurement_06"),
                    frozenset({EDP_THREE}):
                      Measurement(5.3333, 1.00, "measurement_09"),
                    # 2 way combs
                    frozenset({EDP_ONE, EDP_TWO}):
                      Measurement(6.90, 1.00, "measurement_12"),
                    frozenset({EDP_TWO, EDP_THREE}):
                      Measurement(8.66666, 1.00, "measurement_15"),
                    frozenset({EDP_ONE, EDP_THREE}):
                      Measurement(8.90, 1.00, "measurement_18"),
                    # 3 way comb
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}):
                      Measurement(11.90, 1.00, "measurement_21"),
                },
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_can_correct_related_metrics(self):
    ami = "ami"
    mrc = "mrc"
    report = Report(
        metric_reports={
            ami: MetricReport(
                reach_time_series={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(51, 1, "measurement_01")],
                    frozenset({EDP_ONE}): [
                        Measurement(50, 1, "measurement_02")],
                },
                reach_whole_campaign={},
            ),
            mrc: MetricReport(
                reach_time_series={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(52, 1, "measurement_03")],
                    frozenset({EDP_ONE}): [
                        Measurement(51, 1, "measurement_04")],
                },
                reach_whole_campaign={},
            ),
        },
        # AMI is a parent of MRC
        metric_subsets_by_parent={ami: [mrc]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    # The corrected report should be consistent for metric relations: MRC
    # measurements are less than or equal to the AMI measurements, e.g.
    # mrc_reach[edp1][0] <= ami_reach[edp1][0].
    corrected = report.get_corrected_report()

    expected = Report(
        metric_reports={
            ami: MetricReport(
                reach_time_series={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(51.5, 1, "measurement_01")],
                    frozenset({EDP_ONE}): [
                        Measurement(50.5, 1, "measurement_02")],
                },
                reach_whole_campaign={},
            ),
            mrc: MetricReport(
                reach_time_series={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(51.5, 1, "measurement_03")],
                    frozenset({EDP_ONE}): [
                        Measurement(50.5, 1, "measurement_04")],
                },
                reach_whole_campaign={},
            ),
        },
        # AMI is a parent of MRC
        metric_subsets_by_parent={ami: [mrc]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_get_corrected_multiple_metric_report_with_different_edp_combinations(
      self):
    report = Report(
        metric_reports={
            "ami": MetricReport(
                reach_time_series={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(50, 1, "measurement_01")],
                    frozenset({EDP_ONE}): [
                        Measurement(48, 0, "measurement_02")],
                    frozenset({EDP_TWO}): [
                        Measurement(1, 1, "measurement_03")],
                },
                reach_whole_campaign={},
            ),
            "mrc": MetricReport(
                reach_time_series={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(45, 1, "measurement_04")],
                    frozenset({EDP_TWO}): [
                        Measurement(2, 1, "measurement_05")],
                },
                reach_whole_campaign={},
            ),
        },
        metric_subsets_by_parent={"ami": ["mrc"]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    # The corrected report should be consistent for metric relations: MRC
    # measurements are less than or equal to the AMI measurements, e.g.
    # mrc_reach[edp1][0] <= ami_reach[edp1][0].
    corrected = report.get_corrected_report()

    expected = Report(
        metric_reports={
            "ami": MetricReport(
                reach_time_series={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(49.667, 1, "measurement_01")],
                    frozenset({EDP_ONE}): [
                        Measurement(48, 0, "measurement_02")],
                    frozenset({EDP_TWO}): [
                        Measurement(1.667, 1, "measurement_03")],
                },
                reach_whole_campaign={},
            ),
            "mrc": MetricReport(
                reach_time_series={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(45, 1, "measurement_04")],
                    frozenset({EDP_TWO}): [
                        Measurement(1.667, 1, "measurement_05")],
                },
                reach_whole_campaign={},
            ),
        },
        metric_subsets_by_parent={"ami": ["mrc"]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def _assertMeasurementAlmostEquals(
      self, expected: Measurement, actual: Measurement, msg
  ):
    if expected.sigma == 0:
      self.assertAlmostEqual(expected.value, actual.value, msg=msg)
    else:
      self.assertAlmostEqual(
          expected.value, actual.value, places=EXPECTED_PRECISION, msg=msg
      )

  def _assertMetricReportsAlmostEqual(
      self, expected: MetricReport, actual: MetricReport, msg
  ):
    self.assertEqual(expected.get_cumulative_edp_combinations_count(),
                     actual.get_cumulative_edp_combinations_count())
    self.assertEqual(
        expected.get_number_of_periods(), actual.get_number_of_periods()
    )
    for edp_combination in expected.get_cumulative_edp_combinations():
      for period in range(0, expected.get_number_of_periods()):
        self._assertMeasurementAlmostEquals(
            expected.get_cumulative_measurement(edp_combination, period),
            actual.get_cumulative_measurement(edp_combination, period),
            msg,
        )

    self.assertEqual(expected.get_whole_campaign_edp_combinations_count(),
                     actual.get_whole_campaign_edp_combinations_count())
    for edp_combination in expected.get_whole_campaign_edp_combinations():
      self._assertMeasurementAlmostEquals(
          expected.get_whole_campaign_measurement(edp_combination),
          actual.get_whole_campaign_measurement(edp_combination),
          msg,
      )

  def _assertReportsAlmostEqual(self, expected: Report, actual: Report, msg):
    self.assertEqual(expected.get_metrics(), actual.get_metrics())
    for metric in expected.get_metrics():
      self._assertMetricReportsAlmostEqual(
          expected.get_metric_report(metric),
          actual.get_metric_report(metric),
          msg,
      )


if __name__ == "__main__":
  unittest.main()
