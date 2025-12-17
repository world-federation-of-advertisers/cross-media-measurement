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

from unittest.mock import MagicMock

from noiseninja.noised_measurements import Measurement
from noiseninja.noised_measurements import MeasurementSet
from noiseninja.noised_measurements import OrderedSets
from noiseninja.noised_measurements import SetMeasurementsSpec
from noiseninja.solver import Solver

from src.main.python.wfa.measurement.reporting.postprocessing.report.report import (
    EdpCombination,
    MetricReport,
    Report,
    build_measurement_set,
    get_covers,
    is_cover,
    is_union_reach_consistent,
)
from src.main.python.wfa.measurement.reporting.postprocessing.report.testing.validate_report import (
    are_overlap_constraints_consistent,
    get_sorted_list,
    ordered_sets_to_sorted_list,
)

from wfa.measurement.internal.reporting.postprocessing import report_post_processor_result_pb2

StatusCode = report_post_processor_result_pb2.ReportPostProcessorStatus.StatusCode
ReportQuality = report_post_processor_result_pb2.ReportQuality
LargeCorrection = report_post_processor_result_pb2.ReportPostProcessorResult.LargeCorrection

EXPECTED_PRECISION = 1
EDP_ONE = "EDP_ONE"
EDP_TWO = "EDP_TWO"
EDP_THREE = "EDP_THREE"

NOISE_CORRECTION_TOLERANCE = 0.1


SAMPLE_REPORT = Report(
    metric_reports={
        "ami": MetricReport(
            weekly_cumulative_reaches={
                frozenset({EDP_ONE}): [
                    Measurement(1, 0, "measurement_01"),
                    Measurement(1, 0, "measurement_02")
                ],
                frozenset({EDP_TWO}): [
                    Measurement(1, 1, "measurement_03"),
                    Measurement(1, 1, "measurement_04")
                ],
                frozenset({EDP_THREE}): [
                    Measurement(1, 1, "measurement_05"),
                    Measurement(1, 1, "measurement_06")
                ],
                frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                    Measurement(1, 1, "measurement_07"),
                    Measurement(1, 1, "measurement_08")
                ],
            },
            whole_campaign_measurements=build_measurement_set(
                reach={
                    frozenset({EDP_ONE}): Measurement(1, 0, "measurement_09"),
                    frozenset({EDP_TWO}): Measurement(1, 1, "measurement_10"),
                    frozenset({EDP_THREE}): Measurement(1, 1, "measurement_11"),
                    frozenset({EDP_ONE, EDP_TWO}):
                      Measurement(1, 1, "measurement_12"),
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}):
                      Measurement(1, 1, "measurement_13"),
                },
                k_reach={
                    frozenset({EDP_ONE}): {
                        1: Measurement(1, 0, "measurement_14"),
                        2: Measurement(1, 0, "measurement_15"),
                    },
                    frozenset({EDP_TWO}): {
                        1: Measurement(1, 1, "measurement_16"),
                        2: Measurement(1, 1, "measurement_17"),
                    },
                    frozenset({EDP_THREE}): {
                        1: Measurement(1, 1, "measurement_18"),
                        2: Measurement(1, 1, "measurement_19"),
                    },
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): {
                        1: Measurement(1, 1, "measurement_20"),
                        2: Measurement(1, 1, "measurement_21"),
                    },
                },
                impression={
                    frozenset({EDP_ONE}): Measurement(1, 0, "measurement_22"),
                    frozenset({EDP_TWO}): Measurement(1, 1, "measurement_23"),
                    frozenset({EDP_THREE}): Measurement(1, 1, "measurement_24"),
                    frozenset({EDP_ONE, EDP_TWO}):
                      Measurement(1, 1, "measurement_25"),
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}):
                      Measurement(1, 1, "measurement_26"),
                }),
            weekly_non_cumulative_measurements={},
        ),
        "mrc": MetricReport(
            weekly_cumulative_reaches={
                frozenset({EDP_ONE}): [
                    Measurement(1, 0, "measurement_27"),
                    Measurement(1, 0, "measurement_28")
                ],
                frozenset({EDP_TWO}): [
                    Measurement(1, 1, "measurement_29"),
                    Measurement(1, 1, "measurement_30")
                ],
                frozenset({EDP_THREE}): [
                    Measurement(1, 1, "measurement_31"),
                    Measurement(1, 1, "measurement_32")
                ],
                frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                    Measurement(1, 1, "measurement_33"),
                    Measurement(1, 1, "measurement_34")
                ],
            },
            whole_campaign_measurements=build_measurement_set(
                reach={
                    frozenset({EDP_ONE}): Measurement(1, 0, "measurement_35"),
                    frozenset({EDP_TWO}): Measurement(1, 1, "measurement_36"),
                    frozenset({EDP_THREE}): Measurement(1, 1, "measurement_37"),
                    frozenset({EDP_TWO, EDP_THREE}):
                      Measurement(1, 1, "measurement_38"),
                },
                k_reach={
                    frozenset({EDP_ONE}): {
                        1: Measurement(1, 0, "measurement_39"),
                        2: Measurement(1, 0, "measurement_40"),
                    },
                    frozenset({EDP_TWO}): {
                        1: Measurement(1, 1, "measurement_41"),
                        2: Measurement(1, 1, "measurement_42"),
                    },
                    frozenset({EDP_THREE}): {
                        1: Measurement(1, 1, "measurement_43"),
                        2: Measurement(1, 1, "measurement_44"),
                    },
                    frozenset({EDP_TWO, EDP_THREE}): {
                        1: Measurement(1, 1, "measurement_45"),
                        2: Measurement(1, 1, "measurement_46"),
                    },
                },
                impression={
                    frozenset({EDP_ONE}): Measurement(1, 0, "measurement_47"),
                    frozenset({EDP_TWO}): Measurement(1, 1, "measurement_48"),
                    frozenset({EDP_THREE}):
                      Measurement(1, 1, "measurement_49"),
                    frozenset({EDP_TWO, EDP_THREE}):
                      Measurement(1, 1, "measurement_50"),
                }),
            weekly_non_cumulative_measurements={},
        ),
        "custom": MetricReport(
            weekly_cumulative_reaches={
                frozenset({EDP_ONE}): [
                    Measurement(1, 0, "measurement_51"),
                    Measurement(1, 0, "measurement_52")
                ],
                frozenset({EDP_TWO}): [
                    Measurement(1, 1, "measurement_53"),
                    Measurement(1, 1, "measurement_54")
                ],
                frozenset({EDP_THREE}): [
                    Measurement(1, 1, "measurement_55"),
                    Measurement(1, 1, "measurement_56")
                ],
                frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                    Measurement(1, 1, "measurement_57"),
                    Measurement(1, 1, "measurement_58")
                ],
            },
            whole_campaign_measurements=build_measurement_set(
                reach={
                    frozenset({EDP_ONE}): Measurement(1, 0, "measurement_59"),
                    frozenset({EDP_TWO}): Measurement(1, 1, "measurement_60"),
                    frozenset({EDP_THREE}): Measurement(1, 1, "measurement_61"),
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}):
                      Measurement(1, 1, "measurement_62"),
                },
                k_reach={
                    frozenset({EDP_ONE}): {
                        1: Measurement(1, 0, "measurement_63"),
                        2: Measurement(1, 0, "measurement_64"),
                    },
                    frozenset({EDP_TWO}): {
                        1: Measurement(1, 1, "measurement_65"),
                        2: Measurement(1, 1, "measurement_66"),
                    },
                    frozenset({EDP_THREE}): {
                        1: Measurement(1, 1, "measurement_67"),
                        2: Measurement(1, 1, "measurement_68"),
                    },
                    frozenset({EDP_TWO, EDP_THREE}): {
                        1: Measurement(1, 1, "measurement_69"),
                        2: Measurement(1, 1, "measurement_70"),
                    },
                },
                impression={
                    frozenset({EDP_ONE}): Measurement(1, 0, "measurement_71"),
                    frozenset({EDP_TWO}): Measurement(1, 1, "measurement_72"),
                    frozenset({EDP_THREE}):
                      Measurement(1, 1, "measurement_73"),
                    frozenset({EDP_ONE, EDP_THREE}):
                      Measurement(1, 1, "measurement_74"),
                }),
            weekly_non_cumulative_measurements={},
        )
    },
    metric_subsets_by_parent={"ami": ["mrc", "custom"]},
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
        weekly_cumulative_reaches={
            frozenset({EDP_ONE}): [Measurement(1, 1, "measurement_01")],
            frozenset({EDP_TWO}): [Measurement(1, 1, "measurement_02")],
            frozenset({EDP_THREE}): [Measurement(1, 1, "measurement_03")],
            frozenset({EDP_ONE, EDP_TWO}): [
                Measurement(1, 1, "measurement_04")],
            frozenset({EDP_TWO, EDP_THREE}): [
                Measurement(1, 1, "measurement_05")],
            frozenset({EDP_ONE, EDP_THREE}): [
                Measurement(1, 1, "measurement_06")],
            frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                Measurement(1, 1, "measurement_07")],
        },
        whole_campaign_measurements=build_measurement_set(
            reach={},
            k_reach={
                frozenset({EDP_ONE}): {1: Measurement(1, 1, "measurement_08")},
            },
            impression={
                frozenset({EDP_ONE}): Measurement(1, 1, "measurement_09")
            }),
        weekly_non_cumulative_measurements={},
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
        # AMI constraints.
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
            [name_to_index["measurement_09"],
             name_to_index["measurement_10"],
             name_to_index["measurement_11"],
             name_to_index["measurement_12"]]
        ],
        # MRC constraints.
        name_to_index["measurement_33"]: [
            [name_to_index["measurement_27"],
             name_to_index["measurement_29"],
             name_to_index["measurement_31"]]
        ],
        name_to_index["measurement_34"]: [
            [name_to_index["measurement_28"],
             name_to_index["measurement_30"],
             name_to_index["measurement_32"]]
        ],
        name_to_index["measurement_38"]: [
            [name_to_index["measurement_36"],
             name_to_index["measurement_37"]]
        ],
        # CUSTOM constraints.
        name_to_index["measurement_57"]: [
            [name_to_index["measurement_51"],
             name_to_index["measurement_53"],
             name_to_index["measurement_55"]]
        ],
        name_to_index["measurement_58"]: [
            [name_to_index["measurement_52"],
             name_to_index["measurement_54"],
             name_to_index["measurement_56"]]
        ],
        name_to_index["measurement_62"]: [
            [name_to_index["measurement_59"],
             name_to_index["measurement_60"],
             name_to_index["measurement_61"]]
        ],
    }

    spec = SetMeasurementsSpec()
    report._add_cover_relations_to_spec(spec)
    self.assertEqual(len(spec._subsets_by_set), 0)
    self.assertEqual(len(spec._equal_sets), 0)
    self.assertEqual(len(spec._weighted_sum_upperbound_sets), 0)
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
        # AMI constraints.
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
        # MRC constraints.
        name_to_index["measurement_33"]: [name_to_index["measurement_27"],
                                          name_to_index["measurement_29"],
                                          name_to_index["measurement_31"]],
        name_to_index["measurement_34"]: [name_to_index["measurement_28"],
                                          name_to_index["measurement_30"],
                                          name_to_index["measurement_32"]],
        name_to_index["measurement_38"]: [name_to_index["measurement_36"],
                                          name_to_index["measurement_37"]],
        # CUSTOM constraints.
        name_to_index["measurement_57"]: [name_to_index["measurement_51"],
                                          name_to_index["measurement_53"],
                                          name_to_index["measurement_55"]],
        name_to_index["measurement_58"]: [name_to_index["measurement_52"],
                                          name_to_index["measurement_54"],
                                          name_to_index["measurement_56"]],
        name_to_index["measurement_62"]: [name_to_index["measurement_59"],
                                          name_to_index["measurement_60"],
                                          name_to_index["measurement_61"]],
    }

    spec = SetMeasurementsSpec()
    report._add_subset_relations_to_spec(spec)

    self.assertEqual(len(spec._equal_sets), 0)
    self.assertEqual(len(spec._weighted_sum_upperbound_sets), 0)
    self.assertEqual(len(spec._covers_by_set), 0)
    self.assertEqual(expected_subsets_by_set.keys(),
                     spec._subsets_by_set.keys())
    for key in spec._subsets_by_set.keys():
      self.assertEqual(sorted(expected_subsets_by_set[key]),
                       sorted(spec._subsets_by_set[key]))

  def test_add_whole_campaign_impression_relationships(self):
    report = SAMPLE_REPORT
    name_to_index = report._measurement_name_to_index

    expected_subsets_by_set = {
        # AMI constraints.
        name_to_index["measurement_22"]: [name_to_index["measurement_09"]],
        name_to_index["measurement_23"]: [name_to_index["measurement_10"]],
        name_to_index["measurement_24"]: [name_to_index["measurement_11"]],
        name_to_index["measurement_25"]: [name_to_index["measurement_12"]],
        name_to_index["measurement_26"]: [name_to_index["measurement_13"]],
        # MRC constraints.
        name_to_index["measurement_47"]: [name_to_index["measurement_35"]],
        name_to_index["measurement_48"]: [name_to_index["measurement_36"]],
        name_to_index["measurement_49"]: [name_to_index["measurement_37"]],
        name_to_index["measurement_50"]: [name_to_index["measurement_38"]],
        # CUSTOM constraints.
        name_to_index["measurement_71"]: [name_to_index["measurement_59"]],
        name_to_index["measurement_72"]: [name_to_index["measurement_60"]],
        name_to_index["measurement_73"]: [name_to_index["measurement_61"]],
    }

    spec = SetMeasurementsSpec()
    report._add_reach_impression_relations_to_spec(spec)

    self.assertEqual(len(spec._covers_by_set), 0)
    self.assertEqual(len(spec._equal_sets), 0)
    self.assertEqual(len(spec._weighted_sum_upperbound_sets), 0)
    self.assertEqual(expected_subsets_by_set.keys(),
                     spec._subsets_by_set.keys())
    for key in spec._subsets_by_set.keys():
      self.assertEqual(sorted(expected_subsets_by_set[key]),
                       sorted(spec._subsets_by_set[key]))

  def test_add_cumulative_subset_relationships(self):
    report = SAMPLE_REPORT
    name_to_index = report._measurement_name_to_index

    expected_subsets_by_set = {
        # AMI constraints.
        name_to_index["measurement_02"]: [name_to_index["measurement_01"]],
        name_to_index["measurement_04"]: [name_to_index["measurement_03"]],
        name_to_index["measurement_06"]: [name_to_index["measurement_05"]],
        name_to_index["measurement_08"]: [name_to_index["measurement_07"]],
        # MRC constraints.
        name_to_index["measurement_28"]: [name_to_index["measurement_27"]],
        name_to_index["measurement_30"]: [name_to_index["measurement_29"]],
        name_to_index["measurement_32"]: [name_to_index["measurement_31"]],
        name_to_index["measurement_34"]: [name_to_index["measurement_33"]],
        # CUSTOM constraints.
        name_to_index["measurement_52"]: [name_to_index["measurement_51"]],
        name_to_index["measurement_54"]: [name_to_index["measurement_53"]],
        name_to_index["measurement_56"]: [name_to_index["measurement_55"]],
        name_to_index["measurement_58"]: [name_to_index["measurement_57"]],
    }

    spec = SetMeasurementsSpec()
    report._add_cumulative_relations_to_spec(spec)

    self.assertEqual(len(spec._covers_by_set), 0)
    self.assertEqual(len(spec._equal_sets), 0)
    self.assertEqual(len(spec._weighted_sum_upperbound_sets), 0)
    self.assertEqual(expected_subsets_by_set.keys(),
                     spec._subsets_by_set.keys())
    for key in spec._subsets_by_set.keys():
      self.assertEqual(sorted(expected_subsets_by_set[key]),
                       sorted(spec._subsets_by_set[key]))

  def test_add_overlap_relationships(self):
    report = SAMPLE_REPORT
    name_to_index = report._measurement_name_to_index
    expected_ordered_sets = [
        [
            [
                name_to_index["measurement_08"],
                name_to_index["measurement_01"],
                name_to_index["measurement_03"],
                name_to_index["measurement_05"]
            ],
            [
                name_to_index["measurement_07"],
                name_to_index["measurement_02"],
                name_to_index["measurement_04"],
                name_to_index["measurement_06"]
            ]
        ],
        [
            [
                name_to_index["measurement_34"],
                name_to_index["measurement_27"],
                name_to_index["measurement_29"],
                name_to_index["measurement_31"]
            ],
            [
                name_to_index["measurement_33"],
                name_to_index["measurement_28"],
                name_to_index["measurement_30"],
                name_to_index["measurement_32"]
            ]
        ],
        [
            [
                name_to_index["measurement_58"],
                name_to_index["measurement_51"],
                name_to_index["measurement_53"],
                name_to_index["measurement_55"]
            ],
            [
                name_to_index["measurement_57"],
                name_to_index["measurement_52"],
                name_to_index["measurement_54"],
                name_to_index["measurement_56"]
            ]
        ]
    ]

    spec = SetMeasurementsSpec()
    report._add_overlap_relations_to_spec(spec)

    self.assertEqual(len(spec._covers_by_set), 0)
    self.assertEqual(len(spec._equal_sets), 0)
    self.assertEqual(len(spec._weighted_sum_upperbound_sets), 0)
    self.assertEqual(len(spec._subsets_by_set), 0)
    self.assertEqual(len(expected_ordered_sets), len(spec._ordered_sets))
    self.assertEqual(
        get_sorted_list(expected_ordered_sets),
        ordered_sets_to_sorted_list(spec._ordered_sets)
    )

  def test_add_metric_relationships(self):
    report = SAMPLE_REPORT
    name_to_index = report._measurement_name_to_index

    expected_subsets_by_set = {
        # AMI cumulative >= MRC, CUSTOM cumulative.
        name_to_index["measurement_01"]: [name_to_index["measurement_27"],
                                          name_to_index["measurement_51"]],
        name_to_index["measurement_02"]: [name_to_index["measurement_28"],
                                          name_to_index["measurement_52"]],
        name_to_index["measurement_03"]: [name_to_index["measurement_29"],
                                          name_to_index["measurement_53"]],
        name_to_index["measurement_04"]: [name_to_index["measurement_30"],
                                          name_to_index["measurement_54"]],
        name_to_index["measurement_05"]: [name_to_index["measurement_31"],
                                          name_to_index["measurement_55"]],
        name_to_index["measurement_06"]: [name_to_index["measurement_32"],
                                          name_to_index["measurement_56"]],
        name_to_index["measurement_07"]: [name_to_index["measurement_33"],
                                          name_to_index["measurement_57"]],
        name_to_index["measurement_08"]: [name_to_index["measurement_34"],
                                          name_to_index["measurement_58"]],
        # AMI total >= MRC, CUSTOM total.
        name_to_index["measurement_09"]: [name_to_index["measurement_35"],
                                          name_to_index["measurement_59"]],
        name_to_index["measurement_10"]: [name_to_index["measurement_36"],
                                          name_to_index["measurement_60"]],
        name_to_index["measurement_11"]: [name_to_index["measurement_37"],
                                          name_to_index["measurement_61"]],
        name_to_index["measurement_13"]: [name_to_index["measurement_62"]],
        # AMI impression >= MRC, CUSTOM impression.
        name_to_index["measurement_22"]: [name_to_index["measurement_47"],
                                          name_to_index["measurement_71"]],
        name_to_index["measurement_23"]: [name_to_index["measurement_48"],
                                          name_to_index["measurement_72"]],
        name_to_index["measurement_24"]: [name_to_index["measurement_49"],
                                          name_to_index["measurement_73"]],
    }

    expected_ordered_sets = [
        [
            [
                name_to_index["measurement_33"],
                name_to_index["measurement_01"],
                name_to_index["measurement_03"],
                name_to_index["measurement_05"]
            ],
            [
                name_to_index["measurement_07"],
                name_to_index["measurement_27"],
                name_to_index["measurement_29"],
                name_to_index["measurement_31"]
            ]
        ],
        [
            [
                name_to_index["measurement_34"],
                name_to_index["measurement_02"],
                name_to_index["measurement_04"],
                name_to_index["measurement_06"]
            ],
            [
                name_to_index["measurement_08"],
                name_to_index["measurement_28"],
                name_to_index["measurement_30"],
                name_to_index["measurement_32"]
            ]
        ],
        [
            [
                name_to_index["measurement_57"],
                name_to_index["measurement_01"],
                name_to_index["measurement_03"],
                name_to_index["measurement_05"]
            ],
            [
                name_to_index["measurement_07"],
                name_to_index["measurement_51"],
                name_to_index["measurement_53"],
                name_to_index["measurement_55"]
            ]
        ],
        [
            [
                name_to_index["measurement_58"],
                name_to_index["measurement_02"],
                name_to_index["measurement_04"],
                name_to_index["measurement_06"]
            ],
            [
                name_to_index["measurement_08"],
                name_to_index["measurement_52"],
                name_to_index["measurement_54"],
                name_to_index["measurement_56"]
            ]
        ],
        [
            [
                name_to_index["measurement_62"],
                name_to_index["measurement_09"],
                name_to_index["measurement_10"],
                name_to_index["measurement_11"]
            ],
            [
                name_to_index["measurement_13"],
                name_to_index["measurement_59"],
                name_to_index["measurement_60"],
                name_to_index["measurement_61"]
            ]
        ]
    ]

    spec = SetMeasurementsSpec()
    report._add_metric_relations_to_spec(spec)

    self.assertEqual(len(spec._covers_by_set), 0)
    self.assertEqual(len(spec._equal_sets), 0)
    self.assertEqual(len(spec._weighted_sum_upperbound_sets), 0)
    self.assertEqual(expected_subsets_by_set.keys(),
                     spec._subsets_by_set.keys())
    for key in spec._subsets_by_set.keys():
      self.assertEqual(sorted(expected_subsets_by_set[key]),
                       sorted(spec._subsets_by_set[key]))
    self.assertEqual(len(expected_ordered_sets), len(spec._ordered_sets))
    self.assertEqual(
        get_sorted_list(expected_ordered_sets),
        ordered_sets_to_sorted_list(spec._ordered_sets)
    )

  def test_add_cumulative_whole_campaign_relationships(self):
    report = SAMPLE_REPORT
    name_to_index = report._measurement_name_to_index

    expected_equal_sets = [
        # AMI constraints.
        [name_to_index["measurement_02"], [name_to_index["measurement_09"]]],
        [name_to_index["measurement_04"], [name_to_index["measurement_10"]]],
        [name_to_index["measurement_06"], [name_to_index["measurement_11"]]],
        [name_to_index["measurement_08"], [name_to_index["measurement_13"]]],
        # MRC constraints.
        [name_to_index["measurement_28"], [name_to_index["measurement_35"]]],
        [name_to_index["measurement_30"], [name_to_index["measurement_36"]]],
        [name_to_index["measurement_32"], [name_to_index["measurement_37"]]],
        # CUSTOM constraints.
        [name_to_index["measurement_52"], [name_to_index["measurement_59"]]],
        [name_to_index["measurement_54"], [name_to_index["measurement_60"]]],
        [name_to_index["measurement_56"], [name_to_index["measurement_61"]]],
        [name_to_index["measurement_58"], [name_to_index["measurement_62"]]],
    ]

    spec = SetMeasurementsSpec()
    report._add_cumulative_whole_campaign_relations_to_spec(spec)

    self.assertEqual(len(spec._covers_by_set), 0)
    self.assertEqual(len(spec._subsets_by_set), 0)
    self.assertEqual(len(spec._weighted_sum_upperbound_sets), 0)
    self.assertCountEqual(spec._equal_sets, expected_equal_sets)

  def test_add_k_reach_whole_campaign_relations(self):
    report = SAMPLE_REPORT
    name_to_index = report._measurement_name_to_index

    expected_equal_sets = [
        # AMI constraints.
        [name_to_index["measurement_09"],
         [name_to_index["measurement_14"], name_to_index["measurement_15"]]],
        [name_to_index["measurement_10"],
         [name_to_index["measurement_16"], name_to_index["measurement_17"]]],
        [name_to_index["measurement_11"],
         [name_to_index["measurement_18"], name_to_index["measurement_19"]]],
        [name_to_index["measurement_13"],
         [name_to_index["measurement_20"], name_to_index["measurement_21"]]],
        # MRC constraints.
        [name_to_index["measurement_35"],
         [name_to_index["measurement_39"], name_to_index["measurement_40"]]],
        [name_to_index["measurement_36"],
         [name_to_index["measurement_41"], name_to_index["measurement_42"]]],
        [name_to_index["measurement_37"],
         [name_to_index["measurement_43"], name_to_index["measurement_44"]]],
        [name_to_index["measurement_38"],
         [name_to_index["measurement_45"], name_to_index["measurement_46"]]],
        # CUSTOM constraints.
        [name_to_index["measurement_59"],
         [name_to_index["measurement_63"], name_to_index["measurement_64"]]],
        [name_to_index["measurement_60"],
         [name_to_index["measurement_65"], name_to_index["measurement_66"]]],
        [name_to_index["measurement_61"],
         [name_to_index["measurement_67"], name_to_index["measurement_68"]]],
    ]

    spec = SetMeasurementsSpec()
    report._add_k_reach_and_reach_relations_to_spec(spec)

    self.assertEqual(len(spec._covers_by_set), 0)
    self.assertEqual(len(spec._subsets_by_set), 0)
    self.assertEqual(len(spec._weighted_sum_upperbound_sets), 0)
    self.assertCountEqual(spec._equal_sets, expected_equal_sets)

  def test_add_impression_relationships(self):
    report = SAMPLE_REPORT
    name_to_index = report._measurement_name_to_index

    expected_equal_sets = [
        # AMI constraints.
        [
            name_to_index["measurement_25"],
            sorted([name_to_index["measurement_22"],
                    name_to_index["measurement_23"]])
        ],
        [
            name_to_index["measurement_26"],
            sorted([name_to_index["measurement_22"],
                    name_to_index["measurement_23"],
                    name_to_index["measurement_24"]])
        ],
        # MRC constraints.
        [
            name_to_index["measurement_50"],
            sorted([name_to_index["measurement_48"],
                    name_to_index["measurement_49"]])
        ],
        # CUSTOM constraints.
        [
            name_to_index["measurement_74"],
            sorted([name_to_index["measurement_71"],
                    name_to_index["measurement_73"]])
        ],
    ]

    spec = SetMeasurementsSpec()
    report._add_impression_relations_to_spec(spec)

    self.assertEqual(len(spec._covers_by_set), 0)
    self.assertEqual(len(spec._subsets_by_set), 0)
    self.assertEqual(len(spec._weighted_sum_upperbound_sets), 0)
    self.assertCountEqual(spec._equal_sets, expected_equal_sets)

  def test_add_k_reach_impression_relationships(self):
    report = SAMPLE_REPORT
    name_to_index = report._measurement_name_to_index

    expected_weighted_sum_upperbound_sets = {
        # AMI constraints.
        name_to_index["measurement_22"]: [[name_to_index["measurement_14"], 1],
                                          [name_to_index["measurement_15"], 2]],
        name_to_index["measurement_23"]: [[name_to_index["measurement_16"], 1],
                                          [name_to_index["measurement_17"], 2]],
        name_to_index["measurement_24"]: [[name_to_index["measurement_18"], 1],
                                          [name_to_index["measurement_19"], 2]],
        name_to_index["measurement_26"]: [[name_to_index["measurement_20"], 1],
                                          [name_to_index["measurement_21"], 2]],
        # MRC constraints.
        name_to_index["measurement_47"]: [[name_to_index["measurement_39"], 1],
                                          [name_to_index["measurement_40"], 2]],
        name_to_index["measurement_48"]: [[name_to_index["measurement_41"], 1],
                                          [name_to_index["measurement_42"], 2]],
        name_to_index["measurement_49"]: [[name_to_index["measurement_43"], 1],
                                          [name_to_index["measurement_44"], 2]],
        name_to_index["measurement_50"]: [[name_to_index["measurement_45"], 1],
                                          [name_to_index["measurement_46"], 2]],
        # CUSTOM constraints.
        name_to_index["measurement_71"]: [[name_to_index["measurement_63"], 1],
                                          [name_to_index["measurement_64"], 2]],
        name_to_index["measurement_72"]: [[name_to_index["measurement_65"], 1],
                                          [name_to_index["measurement_66"], 2]],
        name_to_index["measurement_73"]: [[name_to_index["measurement_67"], 1],
                                          [name_to_index["measurement_68"], 2]],
    }

    spec = SetMeasurementsSpec()
    report._add_k_reach_impression_relations_to_spec(spec)

    self.assertEqual(len(spec._covers_by_set), 0)
    self.assertEqual(len(spec._subsets_by_set), 0)
    self.assertEqual(len(spec._equal_sets), 0)
    self.assertCountEqual(spec._weighted_sum_upperbound_sets.keys(),
                          expected_weighted_sum_upperbound_sets.keys())
    for key in expected_weighted_sum_upperbound_sets.keys():
      self.assertCountEqual(spec._weighted_sum_upperbound_sets[key],
                            expected_weighted_sum_upperbound_sets[key])

  def test_zero_variance_edp_with_invalid_cumulative_reach_is_not_consistent(
      self):
    # The cumulative reaches are decreasing.
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(55, 0, "measurement_01"),
                        Measurement(48.0, 0, "measurement_02")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(48, 0, "measurement_03"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(20, 0, "measurement_04"),
                            2: Measurement(28, 0, "measurement_05"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(100, 0, "measurement_06"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertFalse(
        report._are_edp_measurements_consistent(frozenset({EDP_ONE}))
    )

  def test_zero_variance_edp_with_mismatch_cumulative_and_total_reach_is_not_consistent(
      self):
    # The total reach is not equal to the last cumulative reach.
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(25, 0, "measurement_01"),
                        Measurement(35.0, 0, "measurement_02")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(48, 0, "measurement_03"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(20, 0, "measurement_04"),
                            2: Measurement(28, 0, "measurement_05"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(100, 0, "measurement_06"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertFalse(
        report._are_edp_measurements_consistent(frozenset({EDP_ONE}))
    )

  def test_zero_variance_edp_with_mismatch_total_reach_and_k_reach_is_not_consistent(
      self):
    # The total reach is not equal to the sum of k reaches.
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(25, 0, "measurement_01"),
                        Measurement(48.0, 0, "measurement_02")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(48, 0, "measurement_03"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(20, 0, "measurement_04"),
                            2: Measurement(20, 0, "measurement_05"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(100, 0, "measurement_06"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertFalse(
        report._are_edp_measurements_consistent(frozenset({EDP_ONE}))
    )

  def test_zero_variance_edp_with_impression_count_less_than_weighted_sum_of_k_reaches_is_not_consistent(
      self):
    # The impression count is much less than the weighted sum of k reaches.
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(25, 0, "measurement_01"),
                        Measurement(48.0, 0, "measurement_02")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(48, 0, "measurement_03"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(20, 0, "measurement_04"),
                            2: Measurement(28, 0, "measurement_05"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(50, 0, "measurement_06"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertFalse(
        report._are_edp_measurements_consistent(frozenset({EDP_ONE}))
    )

  def test_zero_variance_edp_with_inconsistent_metric_relation_is_not_consistent(
      self):
    # The first mrc cumulative week has higher reach than that of ami.
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(35, 0, "measurement_01"),
                        Measurement(48.0, 0, "measurement_02")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(48, 0, "measurement_03"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(20, 0, "measurement_04"),
                            2: Measurement(28, 0, "measurement_05"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(100, 0, "measurement_06"),
                    }),
                weekly_non_cumulative_measurements={},
            ),
            "mrc": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(40, 0, "measurement_07"),
                        Measurement(40.0, 0, "measurement_08")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(40, 0, "measurement_09"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(20, 0, "measurement_10"),
                            2: Measurement(20, 0, "measurement_11"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(80, 0, "measurement_12"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={"ami": ["mrc"]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertFalse(
        report._are_edp_measurements_consistent(frozenset({EDP_ONE}))
    )

  def test_union_consistent_check_throws_exception_when_population_is_zero(
      self):
    with self.assertRaises(ValueError) as ex:
      is_union_reach_consistent(
          Measurement(4.00, 1, "measurement_03"),
          [
              Measurement(0.00, 1, "measurement_01"),
              Measurement(3.30, 1, "measurement_02")
          ],
          0
      )

    self.assertTrue(
        str(ex.exception).startswith(
            "The population size must be greater than 0"
        )
    )

  def test_union_consistent_check_throws_exception_when_there_are_less_than_two_components(
      self):
    with self.assertRaises(ValueError) as ex:
      is_union_reach_consistent(
          Measurement(4.00, 1, "measurement_03"),
          [
              Measurement(0.00, 1, "measurement_01")
          ],
          10000000
      )

    self.assertTrue(
        str(ex.exception).startswith(
            "The length of individual reaches must be at least 2"
        )
    )

  def test_union_consistent_check_passes_when_expected_union_is_within_confidence_interval(
      self):
    # The difference between the expected union and the measured one is
    # abs(12453482 - 12460000), which is 6518. The standard deviation upperbound
    # is 1838.5. The test passes as 6518 < 7*1838.5.
    self.assertTrue(
        is_union_reach_consistent(
            union_measurement=Measurement(12460000, 1300, "measurement_01"),
            component_measurements=[
                Measurement(10000000, 0, "measurement_02"),
                Measurement(2998700, 1300, "measurement_03"),
            ],
            population_size=55000000
        )
    )

  def test_union_consistent_check_fails_when_expected_union_is_outside_confidence_interval(
      self):
    # The difference between the expected union and the measured one is
    # abs(12453482 - 12502000), which is 48518. The standard deviation upperbound
    # is 1838.5. The test passes as 48518 > 7*1838.5.
    self.assertFalse(
        is_union_reach_consistent(
            union_measurement=Measurement(12502000, 1300, "measurement_01"),
            component_measurements=[
                Measurement(10000000, 0, "measurement_02"),
                Measurement(2998700, 1300, "measurement_03"),
            ],
            population_size=55000000
        )
    )

  def test_can_correct_time_series(self):
    ami = "ami"
    report = Report(
        metric_reports={
            ami: MetricReport(
                weekly_cumulative_reaches={
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
                whole_campaign_measurements={},
                weekly_non_cumulative_measurements={},
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
    # 3. All reach values are non-negative.
    corrected, report_post_processor_result = report.get_corrected_report()

    expected = Report(
        metric_reports={
            ami: MetricReport(
                weekly_cumulative_reaches={
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
                whole_campaign_measurements={},
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertTrue(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))
    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_can_correct_overlap_constraints(self):
    # The overlap for the cumulative and whole campaign measurements are:
    # ami: ([0, -4], -9) and mrc: ([1, 3], 1).
    # Overlap violations:
    # Within ami: 0 <= -4
    # Between ami/mrc: 0 >= 1, -4 >= 3, -9 >= 1.
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(0.00, 1, "measurement_01"),
                        Measurement(33.0, 1, "measurement_02"),
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(0.00, 1, "measurement_03"),
                        Measurement(23.0, 1, "measurement_04"),
                    ],
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(0.00, 1, "measurement_05"),
                        Measurement(60.0, 1, "measurement_06"),
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}):
                          Measurement(30.0, 1, "measurement_07"),
                        frozenset({EDP_TWO}):
                          Measurement(30.0, 1, "measurement_08"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(69.0, 1, "measurement_09"),
                    },
                    k_reach={},
                    impression={}),
                weekly_non_cumulative_measurements={},
            ),
            "mrc": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(1.00, 1, "measurement_10"),
                        Measurement(33.0, 1, "measurement_11"),
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(0.00, 1, "measurement_12"),
                        Measurement(23.0, 1, "measurement_13"),
                    ],
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(0.00, 1, "measurement_14"),
                        Measurement(53.0, 1, "measurement_15"),
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}):
                          Measurement(40.0, 1, "measurement_16"),
                        frozenset({EDP_TWO}):
                          Measurement(30.0, 1, "measurement_17"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(69.0, 1, "measurement_18"),
                    },
                    k_reach={},
                    impression={}),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={"ami": ["mrc"]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    corrected, _ = report.get_corrected_report()
    self.assertFalse(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))

  def test_can_correct_time_series_for_three_edps(self):
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
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
                whole_campaign_measurements={},
                weekly_non_cumulative_measurements={},
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
    # 4. All reach values are non-negative.
    corrected, report_post_processor_result = report.get_corrected_report()

    expected = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
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
                whole_campaign_measurements={},
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertFalse(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))
    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_correct_report_with_both_time_series_and_whole_campaign_measurements_three_edps(
      self):
    ami = "ami"

    report = Report(
        metric_reports={
            ami: MetricReport(
                weekly_cumulative_reaches={
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
                whole_campaign_measurements=build_measurement_set(
                    reach={
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
                    k_reach={},
                    impression={}),
                weekly_non_cumulative_measurements={},
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
    # 4. Last time series reach is equal to whole campaign reach,
    # e.g. cumulative_reach[edp1][#num_periods - 1] = whole_campaign_reach[edp1].
    # 5. All reach values are non-negative.
    corrected, report_post_processor_result = report.get_corrected_report()

    expected = Report(
        metric_reports={
            ami: MetricReport(
                weekly_cumulative_reaches={
                    # 1 way comb
                    frozenset({EDP_ONE}): [
                        Measurement(0.10, 1.00, "measurement_01"),
                        Measurement(3.65, 1.00, "measurement_02"),
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(0.00, 1.00, "measurement_04"),
                        Measurement(2.9333, 1.00, "measurement_05"),
                    ],
                    frozenset({EDP_THREE}): [
                        Measurement(0.95, 1.00, "measurement_07"),
                        Measurement(4.4333, 1.00, "measurement_08"),
                    ],
                    # 2 way combs
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(0.10, 1.00, "measurement_10"),
                        Measurement(6.0999, 1.00, "measurement_11"),
                    ],
                    frozenset({EDP_TWO, EDP_THREE}): [
                        Measurement(0.95, 1.00, "measurement_13"),
                        Measurement(7.3666, 1.00, "measurement_14"),
                    ],
                    frozenset({EDP_ONE, EDP_THREE}): [
                        Measurement(1.05, 1.00, "measurement_16"),
                        Measurement(7.95, 1.00, "measurement_17"),
                    ],
                    # 3 way comb
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                        Measurement(1.05, 1.00, "measurement_19"),
                        Measurement(9.95, 1.00, "measurement_20"),
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        # 1 way comb
                        frozenset({EDP_ONE}):
                          Measurement(3.65, 1.00, "measurement_03"),
                        frozenset({EDP_TWO}):
                          Measurement(2.9333, 1.00, "measurement_06"),
                        frozenset({EDP_THREE}):
                          Measurement(4.4333, 1.00, "measurement_09"),
                        # 2 way combs
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(6.0999, 1.00, "measurement_12"),
                        frozenset({EDP_TWO, EDP_THREE}):
                          Measurement(7.3666, 1.00, "measurement_15"),
                        frozenset({EDP_ONE, EDP_THREE}):
                          Measurement(7.95, 1.00, "measurement_18"),
                        # 3 way comb
                        frozenset({EDP_ONE, EDP_TWO, EDP_THREE}):
                          Measurement(9.95, 1.00, "measurement_21"),
                    },
                    k_reach={},
                    impression={}),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertFalse(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))
    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_correct_report_with_whole_campaign_has_more_edp_combinations(self):
    ami = "ami"

    report = Report(
        metric_reports={
            ami: MetricReport(
                weekly_cumulative_reaches={
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
                whole_campaign_measurements=build_measurement_set(
                    reach={
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
                    k_reach={},
                    impression={}),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    # The corrected report should be consistent between time series reaches and
    # whole campaign reach: the last time series reach is equal to whole
    # campaign reach, e.g. cumulative_reach[edp1][#num_period - 1] =
    # whole_campaign_reach[edp1]. All reach values must be non-negative.
    corrected, report_post_processor_result = report.get_corrected_report()

    expected = Report(
        metric_reports={
            ami: MetricReport(
                weekly_cumulative_reaches={
                    # 1 way comb
                    frozenset({EDP_ONE}): [
                        Measurement(0.0250, 1.00, "measurement_01"),
                        Measurement(3.7966, 1.00, "measurement_02"),
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(0.0249, 1.00, "measurement_04"),
                        Measurement(3.1633, 1.00, "measurement_05"),
                    ],
                    frozenset({EDP_THREE}): [
                        Measurement(1.0245, 1.00, "measurement_07"),
                        Measurement(4.8099, 1.00, "measurement_08"),
                    ],
                    # 3 way comb
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                        Measurement(1.075, 1.00, "measurement_19"),
                        Measurement(9.9499, 1.00, "measurement_20"),
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        # 1 way comb
                        frozenset({EDP_ONE}):
                          Measurement(3.7966, 1.00, "measurement_03"),
                        frozenset({EDP_TWO}):
                          Measurement(3.1633, 1.00, "measurement_06"),
                        frozenset({EDP_THREE}):
                          Measurement(4.8099, 1.00, "measurement_09"),
                        # 2 way combs
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(6.8999, 1.00, "measurement_12"),
                        frozenset({EDP_TWO, EDP_THREE}):
                          Measurement(7.9733, 1.00, "measurement_15"),
                        frozenset({EDP_ONE, EDP_THREE}):
                          Measurement(8.6066, 1.00, "measurement_18"),
                        # 3 way comb
                        frozenset({EDP_ONE, EDP_TWO, EDP_THREE}):
                          Measurement(9.9499, 1.00, "measurement_21"),
                    },
                    k_reach={},
                    impression={}),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertTrue(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))
    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_allows_incorrect_time_series(self):
    ami = "ami"
    report = Report(
        metric_reports={
            ami: MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_TWO}): [
                        Measurement(0.00, 1, "measurement_01"),
                        Measurement(3.30, 1, "measurement_02"),
                        Measurement(4.00, 1, "measurement_03"),
                    ],
                    frozenset({EDP_ONE}): [
                        Measurement(0.00, 1, "measurement_04"),
                        Measurement(3.30, 1, "measurement_05"),
                        Measurement(1.00, 1, "measurement_06"),
                    ],
                },
                whole_campaign_measurements={},
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations=set(
            frozenset({EDP_ONE})),
    )

    # The corrected report should be consistent: all the time series reaches are
    # monotonic increasing, e.g. reach[edp1][i] <= reach[edp1][i+1], except for
    # the one in the exception list, e.g. edp1. All reach values must be
    # non-negative.
    corrected, report_post_processor_result = report.get_corrected_report()

    expected = Report(
        metric_reports={
            ami: MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_TWO}): [
                        Measurement(0.00, 1, "measurement_01"),
                        Measurement(3.30, 1, "measurement_02"),
                        Measurement(4.00, 1, "measurement_03"),
                    ],
                    frozenset({EDP_ONE}): [
                        Measurement(0.00, 1, "measurement_04"),
                        Measurement(3.30, 1, "measurement_05"),
                        Measurement(1.00, 1, "measurement_06"),
                    ],
                },
                whole_campaign_measurements={},
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations=set(
            frozenset({EDP_ONE})),
    )

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertTrue(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))
    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_can_correct_related_metrics(self):
    ami = "ami"
    mrc = "mrc"
    report = Report(
        metric_reports={
            ami: MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(51, 1, "measurement_01")],
                    frozenset({EDP_ONE}): [
                        Measurement(50, 1, "measurement_02")],
                },
                whole_campaign_measurements={},
                weekly_non_cumulative_measurements={},
            ),
            mrc: MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(52, 1, "measurement_03")],
                    frozenset({EDP_ONE}): [
                        Measurement(51, 1, "measurement_04")],
                },
                whole_campaign_measurements={},
                weekly_non_cumulative_measurements={},
            ),
        },
        # AMI is a parent of MRC
        metric_subsets_by_parent={ami: [mrc]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    # The corrected report should be consistent for metric relations: MRC
    # measurements are less than or equal to the AMI measurements, e.g.
    # mrc_reach[edp1][0] <= ami_reach[edp1][0]. All reach values must be
    # non-negative.
    corrected, report_post_processor_result = report.get_corrected_report()

    expected = Report(
        metric_reports={
            ami: MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(51.5, 1, "measurement_01")],
                    frozenset({EDP_ONE}): [
                        Measurement(50.5, 1, "measurement_02")],
                },
                whole_campaign_measurements={},
                weekly_non_cumulative_measurements={},
            ),
            mrc: MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(51.5, 1, "measurement_03")],
                    frozenset({EDP_ONE}): [
                        Measurement(50.5, 1, "measurement_04")],
                },
                whole_campaign_measurements={},
                weekly_non_cumulative_measurements={},
            ),
        },
        # AMI is a parent of MRC
        metric_subsets_by_parent={ami: [mrc]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertTrue(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))
    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_correct_report_with_one_consistent_zero_variance_edp_return_consistent_status(
      self):
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(35, 0, "measurement_01"),
                        Measurement(48.0, 0, "measurement_02")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(48, 0, "measurement_03"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(20, 0, "measurement_04"),
                            2: Measurement(28, 0, "measurement_05"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(100, 0, "measurement_06"),
                    }),
                weekly_non_cumulative_measurements={},
            ),
            "mrc": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(30, 0, "measurement_07"),
                        Measurement(40.0, 0, "measurement_08")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(40, 0, "measurement_09"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(20, 0, "measurement_10"),
                            2: Measurement(20, 0, "measurement_11"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(80, 0, "measurement_12"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={"ami": ["mrc"]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    corrected, report_post_processor_result = report.get_corrected_report()

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertEqual(
        report_post_processor_result
        .pre_correction_quality
        .zero_variance_measurements_status,
        ReportQuality.ZeroVarianceMeasurementsStatus.CONSISTENT)
    self.assertEqual(
        report_post_processor_result
        .post_correction_quality
        .zero_variance_measurements_status,
        ReportQuality.ZeroVarianceMeasurementsStatus.CONSISTENT)
    # No union check status as population size is not specified.
    self.assertEqual(
        report_post_processor_result.pre_correction_quality.union_status,
        ReportQuality
        .IndependenceCheckStatus
        .INDEPENDENCE_CHECK_STATUS_UNSPECIFIED
    )
    self.assertEqual(
        report_post_processor_result.post_correction_quality.union_status,
        ReportQuality
        .IndependenceCheckStatus
        .INDEPENDENCE_CHECK_STATUS_UNSPECIFIED
    )
    self.assertTrue(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))
    self._assertReportsAlmostEqual(report, corrected, corrected.to_array())

  def test_correct_report_with_many_consistent_zero_variance_edp_edps_return_consistent_status(
      self):
    # There are two unnoised edps with consistent measurements.
    # The union differences are around 47000, and fall outside the confidence
    # interval [-7sigma, 7sigma] where sigma = sqrt(1300^2 + 1300^2) = 1838.
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(10000000, 0, "measurement_02")
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(3000000, 0, "measurement_03")],
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(12502000, 1300, "measurement_01")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(10000000, 0,
                                                          "measurement_04"),
                        frozenset({EDP_TWO}): Measurement(3000000, 0,
                                                          "measurement_05"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(12500500, 1300, "measurement_06"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(3000000, 0, "measurement_07"),
                            2: Measurement(7000000, 0, "measurement_08"),
                        },
                        frozenset({EDP_TWO}): {
                            1: Measurement(1000000, 0, "measurement_09"),
                            2: Measurement(2000000, 0, "measurement_10"),
                        },
                        frozenset({EDP_ONE, EDP_TWO}): {
                            1: Measurement(4002000, 300, "measurement_11"),
                            2: Measurement(8498500, 300, "measurement_12"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(20000000, 0,
                                                          "measurement_13"),
                        frozenset({EDP_TWO}): Measurement(6000000, 0,
                                                          "measurement_14"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(26002000, 2800, "measurement_15"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
        population_size=55000000,
    )

    corrected, report_post_processor_result = report.get_corrected_report()

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertEqual(
        report_post_processor_result
        .pre_correction_quality
        .zero_variance_measurements_status,
        ReportQuality.ZeroVarianceMeasurementsStatus.CONSISTENT)
    self.assertEqual(
        report_post_processor_result
        .post_correction_quality
        .zero_variance_measurements_status,
        ReportQuality.ZeroVarianceMeasurementsStatus.CONSISTENT)
    self.assertEqual(
        report_post_processor_result.pre_correction_quality.union_status,
        ReportQuality.IndependenceCheckStatus.OUTSIDE_CONFIDENCE_RANGE
    )
    self.assertEqual(
        report_post_processor_result.post_correction_quality.union_status,
        ReportQuality.IndependenceCheckStatus.OUTSIDE_CONFIDENCE_RANGE
    )
    self.assertTrue(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))

  def test_correct_report_with_not_consistent_zero_variance_edp_return_not_consistent_status(
      self):
    # Report has inconsistent unnoised data.
    # measurement 7 should be less than measurement 1.
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(35, 0, "measurement_01"),
                        Measurement(48.0, 0, "measurement_02")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(48, 0, "measurement_03"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(20, 0, "measurement_04"),
                            2: Measurement(28, 0, "measurement_05"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(100, 0, "measurement_06"),
                    }),
                weekly_non_cumulative_measurements={},
            ),
            "mrc": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(40, 0, "measurement_07"),
                        Measurement(40.0, 0, "measurement_08")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(40, 0, "measurement_09"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(20, 0, "measurement_10"),
                            2: Measurement(20, 0, "measurement_11"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(80, 0, "measurement_12"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={"ami": ["mrc"]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    corrected, report_post_processor_result = report.get_corrected_report()

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_NOT_FOUND)
    self.assertEqual(
        report_post_processor_result.status.primal_equality_residual,
        float('inf')
    )
    self.assertEqual(
        report_post_processor_result.status.primal_inequality_residual,
        float('inf')
    )
    self.assertEqual(
        report_post_processor_result
        .pre_correction_quality
        .zero_variance_measurements_status,
        ReportQuality.ZeroVarianceMeasurementsStatus.INCONSISTENT)
    self.assertEqual(
        report_post_processor_result
        .post_correction_quality
        .zero_variance_measurements_status,
        ReportQuality
        .ZeroVarianceMeasurementsStatus
        .ZERO_VARIANCE_MEASUREMENTS_STATUS_UNSPECIFIED)
    self.assertEqual(
        report_post_processor_result.pre_correction_quality.union_status,
        ReportQuality
        .IndependenceCheckStatus
        .INDEPENDENCE_CHECK_STATUS_UNSPECIFIED
    )
    self.assertEqual(
        report_post_processor_result.post_correction_quality.union_status,
        ReportQuality
        .IndependenceCheckStatus
        .INDEPENDENCE_CHECK_STATUS_UNSPECIFIED
    )
    self.assertIsNone(corrected,
                      "Corrected report is None as QP solver fails.")

  def test_correct_report_with_valid_union_statistics(self):
    # The union differences are less than 5000, and fall within the confidence
    # interval [-7sigma, 7sigma] where sigma = sqrt(1300^2 + 1300^2) = 1838.
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(10000000, 0, "measurement_02")
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(2998700, 1300, "measurement_03")],
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(12460000, 1300, "measurement_01")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(10000000, 0,
                                                          "measurement_04"),
                        frozenset({EDP_TWO}): Measurement(3001000, 1300,
                                                          "measurement_05"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(12458000, 1300, "measurement_06"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(3000000, 0, "measurement_07"),
                            2: Measurement(7000000, 0, "measurement_08"),
                        },
                        frozenset({EDP_TWO}): {
                            1: Measurement(1000300, 300, "measurement_09"),
                            2: Measurement(2000700, 300, "measurement_10"),
                        },
                        frozenset({EDP_ONE, EDP_TWO}): {
                            1: Measurement(4002000, 300, "measurement_11"),
                            2: Measurement(8456000, 300, "measurement_12"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(20000000, 0,
                                                          "measurement_13"),
                        frozenset({EDP_TWO}): Measurement(5995000, 2800,
                                                          "measurement_14"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(26002000, 2800, "measurement_15"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
        population_size=55000000,
    )

    corrected, report_post_processor_result = report.get_corrected_report()

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertEqual(
        report_post_processor_result
        .pre_correction_quality
        .zero_variance_measurements_status,
        ReportQuality.ZeroVarianceMeasurementsStatus.CONSISTENT)
    self.assertEqual(
        report_post_processor_result
        .post_correction_quality
        .zero_variance_measurements_status,
        ReportQuality.ZeroVarianceMeasurementsStatus.CONSISTENT)
    self.assertEqual(
        report_post_processor_result.pre_correction_quality.union_status,
        ReportQuality.IndependenceCheckStatus.WITHIN_CONFIDENCE_RANGE
    )
    self.assertEqual(
        report_post_processor_result.post_correction_quality.union_status,
        ReportQuality.IndependenceCheckStatus.WITHIN_CONFIDENCE_RANGE
    )
    self.assertTrue(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))

  def test_correct_report_with_invalid_union_statistics(self):
    # The union differences are around 47000, and fall outside the confidence
    # interval [-7sigma, 7sigma] where sigma = sqrt(1300^2 + 1300^2) = 1838.
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(10000000, 0, "measurement_02")
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(2998700, 1300, "measurement_03")],
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(12502000, 1300, "measurement_01")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(10000000, 0,
                                                          "measurement_04"),
                        frozenset({EDP_TWO}): Measurement(3001000, 1300,
                                                          "measurement_05"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(12500500, 1300, "measurement_06"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(3000000, 0, "measurement_07"),
                            2: Measurement(7000000, 0, "measurement_08"),
                        },
                        frozenset({EDP_TWO}): {
                            1: Measurement(1000300, 300, "measurement_09"),
                            2: Measurement(2000700, 300, "measurement_10"),
                        },
                        frozenset({EDP_ONE, EDP_TWO}): {
                            1: Measurement(4002000, 300, "measurement_11"),
                            2: Measurement(8498500, 300, "measurement_12"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(20000000, 0,
                                                          "measurement_13"),
                        frozenset({EDP_TWO}): Measurement(5995000, 2800,
                                                          "measurement_14"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(26002000, 2800, "measurement_15"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
        population_size=55000000,
    )

    corrected, report_post_processor_result = report.get_corrected_report()

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertEqual(
        report_post_processor_result
        .pre_correction_quality
        .zero_variance_measurements_status,
        ReportQuality.ZeroVarianceMeasurementsStatus.CONSISTENT)
    self.assertEqual(
        report_post_processor_result
        .post_correction_quality
        .zero_variance_measurements_status,
        ReportQuality.ZeroVarianceMeasurementsStatus.CONSISTENT)
    self.assertEqual(
        report_post_processor_result.pre_correction_quality.union_status,
        ReportQuality.IndependenceCheckStatus.OUTSIDE_CONFIDENCE_RANGE
    )
    self.assertEqual(
        report_post_processor_result.post_correction_quality.union_status,
        ReportQuality.IndependenceCheckStatus.OUTSIDE_CONFIDENCE_RANGE
    )
    self.assertTrue(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))

  def test_get_corrected_report_multiple_filter_single_edp(self):
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(36.599, 1, "measurement_01"),
                        Measurement(48.0, 1, "measurement_02")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(40, 1, "measurement_03"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(20.0, 1, "measurement_04"),
                            2: Measurement(0, 1, "measurement_05"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(25, 1, "measurement_06"),
                    }),
                weekly_non_cumulative_measurements={},
            ),
            "mrc": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(20.0, 1, "measurement_07"),
                        Measurement(58.0, 1, "measurement_08")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(40, 1, "measurement_09"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(20.0, 1, "measurement_10"),
                            2: Measurement(0, 1, "measurement_11"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(35, 1, "measurement_12"),
                    }),
                weekly_non_cumulative_measurements={},
            ),
            "custom": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(45.0, 1, "measurement_13"),
                        Measurement(38.0, 1, "measurement_14")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(40, 1, "measurement_15"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(20.0, 1, "measurement_16"),
                            2: Measurement(0, 1, "measurement_17"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(30, 1, "measurement_18"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={"ami": ["mrc", "custom"]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    corrected, report_post_processor_result = report.get_corrected_report()

    # The corrected report should be consistent:
    # 1. Within the same metric report:
    # a) Time series measurements form a non-decreasing sequences.
    # b) The last time series reach is equal to the whole campaign reach.
    # c) The whole campaign reach is equal to the sum of the k reaches.
    # d) The impression is greater than or equal to the weighted sum of the k
    # reaches (where the weights are the corresponding frequency).
    # e) The impression of the union set is equal to the sum of the impression
    # of the individual sets (e.g. impression(edp1 U edp2) = impression(edp1) +
    # impression(edp2)).
    # f) The reach of the union set is less than or equal to the sum of the
    # reach of the subsets it covers (e.g. r(edp1 U edp2) <= r(edp1) + r(edp2)).
    # 2. Excepted for k reaches, mrc/custom measurements <= ami measurements.
    # (e.g. mrc_whole_campaign_reach(edp1) <= ami_whole_campaign_reach(edp1)).
    expected = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(35.844, 1, "measurement_01"),
                        Measurement(35.844, 1, "measurement_02")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(35.844, 1,
                                                          "measurement_03"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(32.510, 1, "measurement_04"),
                            2: Measurement(3.333, 1, "measurement_05"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(39.177, 1,
                                                          "measurement_06"),
                    }),
                weekly_non_cumulative_measurements={},
            ),
            "mrc": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(19.999, 1, "measurement_07"),
                        Measurement(35.844, 1, "measurement_08")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(35.844, 1,
                                                          "measurement_09"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(32.510, 1, "measurement_10"),
                            2: Measurement(3.333, 1, "measurement_11"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(39.177, 1,
                                                          "measurement_12"),
                    }),
                weekly_non_cumulative_measurements={},
            ),
            "custom": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(34.599, 1, "measurement_13"),
                        Measurement(34.599, 1, "measurement_14")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(34.599, 1,
                                                          "measurement_15"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(31.266, 1, "measurement_16"),
                            2: Measurement(3.333, 1, "measurement_17"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(37.933, 1,
                                                          "measurement_18"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={"ami": ["mrc", "custom"]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertTrue(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))
    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_get_corrected_report_single_metric_multiple_edps(self):
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(50, 1, "measurement_01")
                    ],
                    frozenset({EDP_ONE}): [
                        Measurement(48, 0, "measurement_02")
                    ],
                    frozenset({EDP_TWO}): [Measurement(1, 1, "measurement_03")],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(1, 1, "measurement_04"),
                        frozenset({EDP_TWO}): Measurement(1, 1, "measurement_05"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(1, 1, "measurement_06"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(1, 1, "measurement_07"),
                            2: Measurement(1, 1, "measurement_08"),
                        },
                        frozenset({EDP_TWO}): {
                            1: Measurement(1, 1, "measurement_09"),
                            2: Measurement(1, 1, "measurement_10"),
                        },
                        frozenset({EDP_ONE, EDP_TWO}): {
                            1: Measurement(1, 1, "measurement_11"),
                            2: Measurement(1, 1, "measurement_12"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(1, 1, "measurement_13"),
                        frozenset({EDP_TWO}): Measurement(1, 1, "measurement_14"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(1, 1, "measurement_15"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    corrected, report_post_processor_result = report.get_corrected_report()

    # The corrected report should be consistent:
    # a) Time series measurements form a non-decreasing sequences.
    # b) The last time series reach is equal to the whole campaign reach.
    # c) The whole campaign reach is equal to the sum of the k reaches.
    # d) The impression is greater than or equal to the weighted sum of the k
    # reaches (where the weights are the corresponding frequency).
    # e) The impression of the union set is equal to the sum of the impression
    # of the individual sets (e.g. impression(edp1 U edp2) = impression(edp1) +
    # impression(edp2)).
    # f) The reach of the union set is less than or equal to the sum of the
    # reach of the subsets it covers (e.g. r(edp1 U edp2) <= r(edp1) + r(edp2)).
    expected = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(48.0, 1, "measurement_02")
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(0.7142, 1, "measurement_03")
                    ],
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(48.0, 1, "measurement_01")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_TWO}):
                          Measurement(0.7142, 1, "measurement_05"),
                        frozenset({EDP_ONE}): Measurement(48, 1, "measurement_04"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(48.0, 1, "measurement_06"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(48.0, 1, "measurement_07"),
                            2: Measurement(0, 1, "measurement_08"),
                        },
                        frozenset({EDP_TWO}): {
                            1: Measurement(0.7142, 1, "measurement_09"),
                            2: Measurement(0, 1, "measurement_10"),
                        },
                        frozenset({EDP_ONE, EDP_TWO}): {
                            1: Measurement(47.29, 1, "measurement_11"),
                            2: Measurement(0.7142, 1, "measurement_12"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(48, 1, "measurement_13"),
                        frozenset({EDP_TWO}):
                          Measurement(0.7142, 1, "measurement_14"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(48.7184, 1, "measurement_15"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertTrue(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))
    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_report_processor_logs_measurements_with_large_correction(self):
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(48, 1, "measurement_02")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(2, 1, "measurement_04"),
                    },
                    k_reach={},
                    impression={}),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    corrected, report_post_processor_result = report.get_corrected_report()

    expected = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(25, 1, "measurement_02")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(25, 1, "measurement_04"),
                    },
                    k_reach={},
                    impression={}),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )
    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

    self.assertCountEqual(
        report_post_processor_result.large_corrections,
        [
            LargeCorrection(
                metric_title="measurement_04",
                original_value=2,
                corrected_value=25,
                sigma=1.0
            ),
            LargeCorrection(
                metric_title="measurement_02",
                original_value=48,
                corrected_value=25,
                sigma=1.0
            )
        ]
    )

  def test_small_correction_for_unnoised_edp_does_not_log_large_correction(
      self
  ):
    spec = SetMeasurementsSpec()
    spec.add_subset_relation(1, 2)
    spec.add_measurement(1, Measurement(48, 0, "measurement_01"))
    spec.add_measurement(2, Measurement(48, 0, "measurement_02"))

    solver = Solver(spec)

    def side_effect():
        return Solution(
            x=[48.0033325, 48.00666747],
            found=True,
            problem=solver._problem()), ReportPostProcessorStatus(
                status_code=StatusCode.SOLUTION_FOUND_WITH_OSQP)

    mock_solve = MagicMock(side_effect=side_effect)
    solver._solve = mock_solve
    
    report = Report(
        metric_reports={
            "ami":
            MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}):
                    [Measurement(48, 0, "measurement_01")],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}):
                        Measurement(48.01, 0, "measurement_02"),
                    },
                    k_reach={},
                    impression={}),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    expected = Report(
        metric_reports={
            "ami":
            MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}):
                    [Measurement(48.0033325, 0, "measurement_02")],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}):
                        Measurement(48.00666747, 0, "measurement_04"),
                    },
                    k_reach={},
                    impression={}),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    corrected, report_post_processor_result = report.get_corrected_report()

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_OSQP)
    self._assertReportsAlmostEqual(expected, corrected,
                                   corrected.to_array())
    self.assertEqual(len(report_post_processor_result.large_corrections),
                     0)

  def test_get_corrected_reach_only_report_single_metric_multiple_edps(self):
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(50, 1, "measurement_01")
                    ],
                    frozenset({EDP_ONE}): [
                        Measurement(48, 0, "measurement_02")
                    ],
                    frozenset({EDP_TWO}): [Measurement(1, 1, "measurement_03")],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(1, 1, "measurement_04"),
                        frozenset({EDP_TWO}): Measurement(1, 1, "measurement_05"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(1, 1, "measurement_06"),
                    },
                    k_reach={},
                    impression={
                        frozenset({EDP_ONE}): Measurement(1, 1, "measurement_13"),
                        frozenset({EDP_TWO}): Measurement(1, 1, "measurement_14"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(1, 1, "measurement_15"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    corrected, report_post_processor_result = report.get_corrected_report()

    # The corrected report should be consistent:
    # a) Time series measurements form a non-decreasing sequences.
    # b) The last time series reach is equal to the whole campaign reach.
    # c) The whole campaign reach is equal to the sum of the k reaches.
    # d) The impression is greater than or equal to the whole campaign reach.
    # e) The impression of the union set is equal to the sum of the impression
    # of the individual sets (e.g. impression(edp1 U edp2) = impression(edp1) +
    # impression(edp2)).
    # f) The reach of the union set is less than or equal to the sum of the
    # reach of the subsets it covers (e.g. r(edp1 U edp2) <= r(edp1) + r(edp2)).
    expected = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(48.0, 1, "measurement_02")
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(0.0, 1, "measurement_03")
                    ],
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(48.0, 1, "measurement_01")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_TWO}):
                          Measurement(0.0, 1, "measurement_05"),
                        frozenset({EDP_ONE}): Measurement(48, 1, "measurement_04"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(48.0, 1, "measurement_06"),
                    },
                    k_reach={},
                    impression={
                        frozenset({EDP_ONE}): Measurement(48, 1, "measurement_13"),
                        frozenset({EDP_TWO}):
                          Measurement(0.0, 1, "measurement_14"),
                        frozenset({EDP_ONE, EDP_TWO}):
                          Measurement(48.0, 1, "measurement_15"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertTrue(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))
    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_get_corrected_report_when_cumulative_reaches_are_consistent(self):
    # AMI reaches are consistent.
    ami_time_series = [
        2931765,
        3049283,
        3081004,
    ]
    # MRC reaches are consistent.
    mrc_time_series = [
        2043370,
        2130897,
        2181590,
    ]

    # Total reach is the sum of k reaches, and is different from the
    # corresponding last cumulative reach.
    AMI_TOTAL_REACH = 3072651
    MRC_TOTAL_REACH = 2169768

    ami_k_reach = [2020883, 491853, 328201, 200862, 30852]
    mrc_k_reach = [1580273, 389637, 138795, 27131, 33932]

    ami_impression = 5262888
    mrc_impression = 3343141

    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(ami_time_series[i], 10000,
                                    "measurement_" + str(i))
                        for i in range(0, len(ami_time_series))
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(AMI_TOTAL_REACH, 10000,
                                                          "measurement_3")
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(ami_k_reach[0], 10000, "measurement_4"),
                            2: Measurement(ami_k_reach[1], 10000, "measurement_5"),
                            3: Measurement(ami_k_reach[2], 10000, "measurement_6"),
                            4: Measurement(ami_k_reach[3], 10000, "measurement_7"),
                            5: Measurement(ami_k_reach[4], 10000, "measurement_8"),
                        }
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(ami_impression, 10000,
                                                          "measurement_9")
                    }),
                weekly_non_cumulative_measurements={},
            ),
            "mrc": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(mrc_time_series[i], 10000,
                                    "measurement_" + str(10 + i))
                        for i in range(0, len(mrc_time_series))
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(MRC_TOTAL_REACH, 10000,
                                                          "measurement_13")
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(mrc_k_reach[0], 10000, "measurement_14"),
                            2: Measurement(mrc_k_reach[1], 10000, "measurement_15"),
                            3: Measurement(mrc_k_reach[2], 10000, "measurement_16"),
                            4: Measurement(mrc_k_reach[3], 10000, "measurement_17"),
                            5: Measurement(mrc_k_reach[4], 10000, "measurement_18"),
                        }
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(mrc_impression, 10000,
                                                          "measurement_19")
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={"ami": ["mrc"]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    corrected, report_post_processor_result = report.get_corrected_report()

    # The corrected report should be consistent:
    # a) Time series measurements form a non-decreasing sequences.
    # b) The last time series reach is equal to the whole campaign reach.
    # c) The whole campaign reach is equal to the sum of the k reaches.
    # d) The impression is greater than or equal to the weighted sum of the k
    # reaches (where the weights are the corresponding frequency).
    # e) The impression of the union set is equal to the sum of the impression
    # of the individual sets (e.g. impression(edp1 U edp2) = impression(edp1) +
    # impression(edp2)).
    # f) The reach of the union set is less than or equal to the sum of the
    # reach of the subsets it covers (e.g. r(edp1 U edp2) <= r(edp1) + r(edp2)).
    expected = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(2931764.71, 10000, "measurement_0"),
                        Measurement(3049282.70, 10000, "measurement_1"),
                        Measurement(3076447.51, 10000, "measurement_2"),
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(3076447.51, 10000,
                                                          "measurement_3")
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(2021642.16, 10000, "measurement_4"),
                            2: Measurement(492612.31, 10000, "measurement_5"),
                            3: Measurement(328960.33, 10000, "measurement_6"),
                            4: Measurement(201621.34, 10000, "measurement_7"),
                            5: Measurement(31611.36, 10000, "measurement_8"),
                        }
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(5262887.47, 10000,
                                                          "measurement_9")
                    }),
                weekly_non_cumulative_measurements={},
            ),
            "mrc": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(2043369.80, 10000, "measurement_10"),
                        Measurement(2130896.79, 10000, "measurement_11"),
                        Measurement(2175141.42, 10000, "measurement_12"),
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(2175141.42, 10000,
                                                          "measurement_13")
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(1581347.57, 10000, "measurement_14"),
                            2: Measurement(390711.69, 10000, "measurement_15"),
                            3: Measurement(139869.71, 10000, "measurement_16"),
                            4: Measurement(28205.72, 10000, "measurement_17"),
                            5: Measurement(35006.72, 10000, "measurement_18"),
                        }
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(3343140.67, 10000,
                                                          "measurement_19")
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={"ami": ["mrc"]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertTrue(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))
    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_get_corrected_report_without_cumulative_reaches(self):
    # AMI reaches are consistent.
    ami_time_series = [
        2931765,
        3049283,
        3081004,
    ]
    # MRC reaches are consistent.
    mrc_time_series = [
        2043370,
        2130897,
        2181590,
    ]

    # Total reach is the sum of k reaches, and is different from the
    # corresponding last cumulative reach.
    AMI_TOTAL_REACH = 3072651
    MRC_TOTAL_REACH = 2169768

    ami_k_reach = [2020883, 491853, 328201, 200862, 30852]
    mrc_k_reach = [1580273, 389637, 138795, 27131, 33932]

    ami_impression = 5262888
    mrc_impression = 3343141

    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={},
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(AMI_TOTAL_REACH, 10000,
                                                          "measurement_3")
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(ami_k_reach[0], 10000, "measurement_4"),
                            2: Measurement(ami_k_reach[1], 10000, "measurement_5"),
                            3: Measurement(ami_k_reach[2], 10000, "measurement_6"),
                            4: Measurement(ami_k_reach[3], 10000, "measurement_7"),
                            5: Measurement(ami_k_reach[4], 10000, "measurement_8"),
                        }
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(ami_impression, 10000,
                                                          "measurement_9")
                    }),
                weekly_non_cumulative_measurements={},
            ),
            "mrc": MetricReport(
                weekly_cumulative_reaches={},
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(MRC_TOTAL_REACH, 10000,
                                                          "measurement_13")
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(mrc_k_reach[0], 10000, "measurement_14"),
                            2: Measurement(mrc_k_reach[1], 10000, "measurement_15"),
                            3: Measurement(mrc_k_reach[2], 10000, "measurement_16"),
                            4: Measurement(mrc_k_reach[3], 10000, "measurement_17"),
                            5: Measurement(mrc_k_reach[4], 10000, "measurement_18"),
                        }
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(mrc_impression, 10000,
                                                          "measurement_19")
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={"ami": ["mrc"]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    corrected, report_post_processor_result = report.get_corrected_report()

    # The corrected report should be consistent:
    # a) The whole campaign reach is equal to the sum of the k reaches.
    # b) The impression is greater than or equal to the weighted sum of the k
    # reaches (where the weights are the corresponding frequency).
    # c) The impression of mrc is less than or equal to that of ami.
    expected = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={},
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(3072650.69, 10000,
                                                          "measurement_3")
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(2020882.79, 10000, "measurement_4"),
                            2: Measurement(491852.95, 10000, "measurement_5"),
                            3: Measurement(328200.96, 10000, "measurement_6"),
                            4: Measurement(200861.98, 10000, "measurement_7"),
                            5: Measurement(30851.99, 10000, "measurement_8"),
                        }
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(5262887.47, 10000,
                                                          "measurement_9")
                    }),
                weekly_non_cumulative_measurements={},
            ),
            "mrc": MetricReport(
                weekly_cumulative_reaches={},
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(2169767.78, 10000,
                                                          "measurement_13")
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(1580272.84, 10000, "measurement_14"),
                            2: Measurement(389636.96, 10000, "measurement_15"),
                            3: Measurement(138794.98, 10000, "measurement_16"),
                            4: Measurement(27130.99, 10000, "measurement_17"),
                            5: Measurement(33931.99, 10000, "measurement_18"),
                        }
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(3343140.67, 10000,
                                                          "measurement_19")
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={"ami": ["mrc"]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())

  def test_get_corrected_report_without_reaches(self):
    report = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={},
                whole_campaign_measurements=build_measurement_set(
                    reach={},
                    k_reach={},
                    impression={
                        frozenset({EDP_ONE}): Measurement(5262888, 10000,
                                                          "measurement_9")
                    }),
                weekly_non_cumulative_measurements={},
            ),
            "mrc": MetricReport(
                weekly_cumulative_reaches={},
                whole_campaign_measurements=build_measurement_set(
                    reach={},
                    k_reach={},
                    impression={
                        frozenset({EDP_ONE}): Measurement(6343141, 10000,
                                                          "measurement_19")
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={"ami": ["mrc"]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    corrected, report_post_processor_result = report.get_corrected_report()

    # The corrected report should be consistent: c) The impression of mrc is
    # less than or equal to that of ami.
    expected = Report(
        metric_reports={
            "ami": MetricReport(
                weekly_cumulative_reaches={},
                whole_campaign_measurements=build_measurement_set(
                    reach={},
                    k_reach={},
                    impression={
                        frozenset({EDP_ONE}): Measurement(5803013.91, 10000,
                                                          "measurement_9")
                    }),
                weekly_non_cumulative_measurements={},
            ),
            "mrc": MetricReport(
                weekly_cumulative_reaches={},
                whole_campaign_measurements=build_measurement_set(
                    reach={},
                    k_reach={},
                    impression={
                        frozenset({EDP_ONE}): Measurement(5803013.91, 10000,
                                                          "measurement_19")
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={"ami": ["mrc"]},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self._assertReportsAlmostEqual(expected, corrected, corrected.to_array())


  def test_get_corrected_report_without_ami_multiple_filter_single_edp(self):
    report = Report(
        metric_reports={
            "mrc": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(20.0, 1, "measurement_07"),
                        Measurement(58.0, 1, "measurement_08")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(40, 1, "measurement_09"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(20.0, 1, "measurement_10"),
                            2: Measurement(0, 1, "measurement_11"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(35, 1, "measurement_12"),
                    }),
                weekly_non_cumulative_measurements={},
            ),
            "custom": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(45.0, 1, "measurement_13"),
                        Measurement(38.0, 1, "measurement_14")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(40, 1, "measurement_15"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(20.0, 1, "measurement_16"),
                            2: Measurement(0, 1, "measurement_17"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(30, 1, "measurement_18"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    corrected, report_post_processor_result = report.get_corrected_report()

    # The corrected report should be consistent:
    # 1. Within the same metric report:
    # a) Time series measurements form a non-decreasing sequences.
    # b) The last time series reach is equal to the whole campaign reach.
    # c) The whole campaign reach is equal to the sum of the k reaches.
    # d) The impression is greater than or equal to the weighted sum of the k
    # reaches (where the weights are the corresponding frequency).
    # e) The impression of the union set is equal to the sum of the impression
    # of the individual sets (e.g. impression(edp1 U edp2) = impression(edp1) +
    # impression(edp2)).
    # f) The reach of the union set is less than or equal to the sum of the
    # reach of the subsets it covers (e.g. r(edp1 U edp2) <= r(edp1) + r(edp2)).
    expected = Report(
        metric_reports={
            "mrc": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(19.999, 1, "measurement_07"),
                        Measurement(38.249996, 1, "measurement_08")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(38.249996, 1,
                                                          "measurement_09"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(33.249997, 1, "measurement_10"),
                            2: Measurement(4.999999, 1, "measurement_11"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(43.249996, 1,
                                                          "measurement_12"),
                    }),
                weekly_non_cumulative_measurements={},
            ),
            "custom": MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(34.599996, 1, "measurement_13"),
                        Measurement(34.599996, 1, "measurement_14")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}): Measurement(34.599996, 1,
                                                          "measurement_15"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(31.26666, 1, "measurement_16"),
                            2: Measurement(3.33333, 1, "measurement_17"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}): Measurement(37.93333, 1,
                                                          "measurement_18"),
                    }),
                weekly_non_cumulative_measurements={},
            )
        },
        metric_subsets_by_parent={},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    self.assertEqual(report_post_processor_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(
        report_post_processor_result.status.primal_equality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertLess(
        report_post_processor_result.status.primal_inequality_residual,
        NOISE_CORRECTION_TOLERANCE)
    self.assertTrue(
        are_overlap_constraints_consistent(report, NOISE_CORRECTION_TOLERANCE))
    self.assertTrue(
        are_overlap_constraints_consistent(corrected,
                                           NOISE_CORRECTION_TOLERANCE))
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
    self.assertEqual(
        expected.get_number_of_periods(), actual.get_number_of_periods()
    )
    self.assertEqual(
        expected.get_number_of_frequencies(), actual.get_number_of_frequencies()
    )

    self.assertCountEqual(
        expected.get_weekly_cumulative_reach_edp_combinations(),
        actual.get_weekly_cumulative_reach_edp_combinations()
    )
    for edp_combination in expected.get_weekly_cumulative_reach_edp_combinations():
      for period in range(0, expected.get_number_of_periods()):
        self._assertMeasurementAlmostEquals(
            expected.get_weekly_cumulative_reach_measurement(edp_combination, period),
            actual.get_weekly_cumulative_reach_measurement(edp_combination, period),
            msg,
        )

    self.assertCountEqual(
        expected.get_whole_campaign_reach_edp_combinations(),
        actual.get_whole_campaign_reach_edp_combinations()
    )
    for edp_combination in expected.get_whole_campaign_reach_edp_combinations():
      self._assertMeasurementAlmostEquals(
          expected.get_whole_campaign_reach_measurement(edp_combination),
          actual.get_whole_campaign_reach_measurement(edp_combination),
          msg,
      )

    self.assertCountEqual(
        expected.get_whole_campaign_k_reach_edp_combinations(),
        actual.get_whole_campaign_k_reach_edp_combinations()
    )
    for edp_combination in expected.get_whole_campaign_k_reach_edp_combinations():
      for frequency in range(1, expected.get_number_of_frequencies() + 1):
        self._assertMeasurementAlmostEquals(
            expected.get_whole_campaign_k_reach_measurement(edp_combination, frequency),
            actual.get_whole_campaign_k_reach_measurement(edp_combination, frequency),
            msg
        )

    self.assertCountEqual(
        expected.get_whole_campaign_impression_edp_combinations(),
        actual.get_whole_campaign_impression_edp_combinations()
    )
    for edp_combination in expected.get_whole_campaign_impression_edp_combinations():
      self._assertMeasurementAlmostEquals(
          expected.get_whole_campaign_impression_measurement(edp_combination),
          actual.get_whole_campaign_impression_measurement(edp_combination),
          msg
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
