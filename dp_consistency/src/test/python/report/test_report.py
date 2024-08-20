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

from unittest import TestCase

from src.main.python.noiseninja.noised_measurements import Measurement
from src.main.python.report.report import Report, MetricReport

EXPECTED_PRECISION = 3
EDP_ONE = "EDP_ONE"
EDP_TWO = "EDP_TWO"
EDP_THREE = "EDP_THREE"


class TestReport(TestCase):

    def test_get_cover_relationships(self):
        metric_report = MetricReport(
            reach_time_series_by_edp_combination={
                frozenset({EDP_ONE}): [Measurement(1, 0)],
                frozenset({EDP_TWO}): [Measurement(1, 0)],
                frozenset({EDP_THREE}): [Measurement(1, 0)],
                frozenset({EDP_ONE, EDP_TWO}): [Measurement(1, 0)],
                frozenset({EDP_TWO, EDP_THREE}): [Measurement(1, 0)],
                frozenset({EDP_ONE, EDP_THREE}): [Measurement(1, 0)],
                frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [Measurement(1, 0)],
            }
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
        self.assertEqual(metric_report.get_cover_relationships(), expected)

    def test_get_corrected_single_metric_report(self):

        ami = "ami"

        report = Report(
            metric_reports={
                ami: MetricReport(
                    reach_time_series_by_edp_combination={
                        frozenset({EDP_ONE, EDP_TWO}): [Measurement(50, 1)],
                        frozenset({EDP_ONE}): [Measurement(48, 0)],
                        frozenset({EDP_TWO}): [Measurement(1, 1)],
                    }
                )
            },
            metric_subsets_by_parent={},
            cumulative_inconsistency_allowed_edp_combs={},
        )

        corrected = report.get_corrected_report()

        expected = Report(
            metric_reports={
                ami: MetricReport(
                    reach_time_series_by_edp_combination={
                        frozenset({EDP_ONE, EDP_TWO}): [Measurement(49.5, 1)],
                        frozenset({EDP_ONE}): [Measurement(48, 0)],
                        frozenset({EDP_TWO}): [Measurement(1.5, 1)],
                    }
                )
            },
            metric_subsets_by_parent={},
            cumulative_inconsistency_allowed_edp_combs={},
        )

        self.__assertReportsAlmostEqual(expected, corrected, corrected.to_array())

    def test_can_correct_time_series(self):
        ami = "ami"
        report = Report(
            metric_reports={
                ami: MetricReport(
                    reach_time_series_by_edp_combination={
                        frozenset({EDP_ONE, EDP_TWO}): [
                            Measurement(0.00, 1),
                            Measurement(3.30, 1),
                            Measurement(0.00, 1),
                        ],
                        frozenset({EDP_ONE}): [
                            Measurement(0.00, 1),
                            Measurement(3.30, 1),
                            Measurement(0.00, 1),
                        ],
                    }
                )
            },
            metric_subsets_by_parent={},
            cumulative_inconsistency_allowed_edp_combs={},
        )

        corrected = report.get_corrected_report()

        expected = Report(
            metric_reports={
                ami: MetricReport(
                    reach_time_series_by_edp_combination={
                        frozenset({EDP_ONE, EDP_TWO}): [
                            Measurement(0.00, 1),
                            Measurement(1.65, 1),
                            Measurement(1.65, 1),
                        ],
                        frozenset({EDP_ONE}): [
                            Measurement(0.00, 1),
                            Measurement(1.65, 1),
                            Measurement(1.65, 1),
                        ],
                    }
                )
            },
            metric_subsets_by_parent={},
            cumulative_inconsistency_allowed_edp_combs={},
        )

        self.__assertReportsAlmostEqual(expected, corrected, corrected.to_array())

    def test_can_correct_time_series_for_three_edps(self):
        ami = "ami"
        report = Report(
            metric_reports={
                ami: MetricReport(
                    reach_time_series_by_edp_combination={
                        # 1 way comb
                        frozenset({EDP_ONE}): [
                            Measurement(0.00, 1),
                            Measurement(3.30, 1),
                            Measurement(4.00, 1),
                        ],
                        frozenset({EDP_TWO}): [
                            Measurement(0.00, 1),
                            Measurement(2.30, 1),
                            Measurement(3.00, 1),
                        ],
                        frozenset({EDP_THREE}): [
                            Measurement(1.00, 1),
                            Measurement(3.30, 1),
                            Measurement(5.00, 1),
                        ],
                        # 2 way combs
                        frozenset({EDP_ONE, EDP_TWO}): [
                            Measurement(0.00, 1),
                            Measurement(5.30, 1),
                            Measurement(6.90, 1),
                        ],
                        frozenset({EDP_TWO, EDP_THREE}): [
                            Measurement(0.70, 1),
                            Measurement(6.30, 1),
                            Measurement(9.00, 1),
                        ],
                        frozenset({EDP_ONE, EDP_THREE}): [
                            Measurement(1.20, 1),
                            Measurement(7.00, 1),
                            Measurement(8.90, 1),
                        ],
                        # 3 way comb
                        frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                            Measurement(1.10, 1),
                            Measurement(8.0, 1),
                            Measurement(11.90, 1),
                        ],
                    }
                )
            },
            metric_subsets_by_parent={},
            cumulative_inconsistency_allowed_edp_combs={},
        )

        corrected = report.get_corrected_report()

        expected = Report(
            metric_reports={
                ami: MetricReport(
                    reach_time_series_by_edp_combination={
                        # 1 way comb
                        frozenset({EDP_ONE}): [
                            Measurement(0.10, 1.00),
                            Measurement(3.362, 1.00),
                            Measurement(4.00, 1.00),
                        ],
                        frozenset({EDP_TWO}): [
                            Measurement(0.00, 1.00),
                            Measurement(2.512, 1.00),
                            Measurement(3.3333, 1.00),
                        ],
                        frozenset({EDP_THREE}): [
                            Measurement(0.95, 1.00),
                            Measurement(3.5749, 1.00),
                            Measurement(5.3333, 1.00),
                        ],
                        # 2 way combs
                        frozenset({EDP_ONE, EDP_TWO}): [
                            Measurement(0.10, 1.00),
                            Measurement(5.30, 1.00),
                            Measurement(6.90, 1.00),
                        ],
                        frozenset({EDP_TWO, EDP_THREE}): [
                            Measurement(0.95, 1.00),
                            Measurement(6.087, 1.00),
                            Measurement(8.66666, 1.00),
                        ],
                        frozenset({EDP_ONE, EDP_THREE}): [
                            Measurement(1.05, 1.00),
                            Measurement(6.937, 1.00),
                            Measurement(8.90, 1.00),
                        ],
                        # 3 way comb
                        frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                            Measurement(1.05, 1.00),
                            Measurement(8.00, 1.00),
                            Measurement(11.90, 1.00),
                        ],
                    }
                )
            },
            metric_subsets_by_parent={},
            cumulative_inconsistency_allowed_edp_combs={},
        )

        self.__assertReportsAlmostEqual(expected, corrected, corrected.to_array())

    def test_allows_incorrect_time_series(self):
        ami = "ami"
        report = Report(
            metric_reports={
                ami: MetricReport(
                    reach_time_series_by_edp_combination={
                        frozenset({EDP_TWO}): [
                            Measurement(0.00, 1),
                            Measurement(3.30, 1),
                            Measurement(4.00, 1),
                        ],
                        frozenset({EDP_ONE}): [
                            Measurement(0.00, 1),
                            Measurement(3.30, 1),
                            Measurement(1.00, 1),
                        ],
                    }
                )
            },
            metric_subsets_by_parent={},
            cumulative_inconsistency_allowed_edp_combs=set(frozenset({EDP_ONE})),
        )

        corrected = report.get_corrected_report()

        expected = Report(
            metric_reports={
                ami: MetricReport(
                    reach_time_series_by_edp_combination={
                        frozenset({EDP_TWO}): [
                            Measurement(0.00, 1),
                            Measurement(3.30, 1),
                            Measurement(4.00, 1),
                        ],
                        frozenset({EDP_ONE}): [
                            Measurement(0.00, 1),
                            Measurement(3.30, 1),
                            Measurement(1.00, 1),
                        ],
                    }
                )
            },
            metric_subsets_by_parent={},
            cumulative_inconsistency_allowed_edp_combs=set(frozenset({EDP_ONE})),
        )

        self.__assertReportsAlmostEqual(expected, corrected, corrected.to_array())

    def test_can_correct_related_metrics(self):
        ami = "ami"
        mrc = "mrc"
        report = Report(
            metric_reports={
                ami: MetricReport(
                    reach_time_series_by_edp_combination={
                        frozenset({EDP_ONE, EDP_TWO}): [Measurement(51, 1)],
                        frozenset({EDP_ONE}): [Measurement(50, 1)],
                    }
                ),
                mrc: MetricReport(
                    reach_time_series_by_edp_combination={
                        frozenset({EDP_ONE, EDP_TWO}): [Measurement(52, 1)],
                        frozenset({EDP_ONE}): [Measurement(51, 1)],
                    }
                ),
            },
            # AMI is a parent of MRC
            metric_subsets_by_parent={ami: [mrc]},
            cumulative_inconsistency_allowed_edp_combs={},
        )

        corrected = report.get_corrected_report()

        expected = Report(
            metric_reports={
                ami: MetricReport(
                    reach_time_series_by_edp_combination={
                        frozenset({EDP_ONE, EDP_TWO}): [Measurement(51.5, 1)],
                        frozenset({EDP_ONE}): [Measurement(50.5, 1)],
                    }
                ),
                mrc: MetricReport(
                    reach_time_series_by_edp_combination={
                        frozenset({EDP_ONE, EDP_TWO}): [Measurement(51.5, 1)],
                        frozenset({EDP_ONE}): [Measurement(50.5, 1)],
                    }
                ),
            },
            # AMI is a parent of MRC
            metric_subsets_by_parent={ami: [mrc]},
            cumulative_inconsistency_allowed_edp_combs={},
        )

        self.__assertReportsAlmostEqual(expected, corrected, corrected.to_array())

    def __assertMeasurementAlmostEquals(
        self, expected: Measurement, actual: Measurement, msg
    ):
        if expected.sigma == 0:
            self.assertAlmostEqual(expected.value, actual.value, msg=msg)
        else:
            self.assertAlmostEqual(
                expected.value, actual.value, places=EXPECTED_PRECISION, msg=msg
            )

    def __assertMetricReportsAlmostEqual(
        self, expected: MetricReport, actual: MetricReport, msg
    ):
        self.assertEqual(expected.get_num_edp_combs(), actual.get_num_edp_combs())
        self.assertEqual(
            expected.get_number_of_periods(), actual.get_number_of_periods()
        )
        for period in range(0, expected.get_number_of_periods()):
            for edp_comb in expected.get_edp_combs():
                self.__assertMeasurementAlmostEquals(
                    expected.get_edp_comb_measurement(edp_comb, period),
                    actual.get_edp_comb_measurement(edp_comb, period),
                    msg,
                )

    def __assertReportsAlmostEqual(self, expected: Report, actual: Report, msg):
        self.assertEqual(expected.get_metrics(), actual.get_metrics())
        for metric in expected.get_metrics():
            self.__assertMetricReportsAlmostEqual(
                expected.get_metric_report(metric),
                actual.get_metric_report(metric),
                msg,
            )
