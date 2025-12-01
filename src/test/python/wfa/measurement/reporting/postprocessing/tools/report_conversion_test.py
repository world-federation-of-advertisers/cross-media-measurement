# Copyright 2025 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for report_conversion."""

import unittest

from google.protobuf import text_format
from src.main.python.wfa.measurement.reporting.postprocessing.tools.report_conversion import (
    _create_report_summary_for_group,
    report_summaries_from_reporting_set_results,
)
from wfa.measurement.internal.reporting.postprocessing import (
    report_summary_v2_pb2,
)
from wfa.measurement.internal.reporting.v2 import report_result_pb2
from wfa.measurement.internal.reporting.v2 import report_results_service_pb2
from wfa.measurement.internal.reporting.v2 import event_template_field_pb2
from wfa.measurement.internal.reporting.v2 import (
    metric_frequency_spec_pb2,
)

ReportSummaryWindowResult = (report_summary_v2_pb2.ReportSummaryV2.
                             ReportSummarySetResult.ReportSummaryWindowResult)


class ReportConversionTest(unittest.TestCase):

    def setUp(self):
        super().setUp()
        with open(
                'src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_result.textproto',
                'r') as file:
            list_response_textproto = file.read()
        self.reporting_set_results = _parse_reporting_set_results(
            list_response_textproto
        )

        self.edp_combinations = {
            'edp1': ['edp1'],
            'edp2': ['edp2'],
            'edp3': ['edp3'],
            'edp1_edp2': ['edp1', 'edp2'],
            'edp1_edp2_edp3': ['edp1', 'edp2', 'edp3'],
        }

    def test_create_report_summary_for_group_with_empty_results_raises_error(
            self):
        with self.assertRaisesRegex(ValueError, "No results found for group"):
            _create_report_summary_for_group(
                cmms_measurement_consumer_id="test-mc-id",
                external_report_result_id=123,
                group_key=(456, 789),
                results_for_group=[],
                primitive_reporting_sets_by_reporting_set_id={
                    "1": ["EDP1"],
                    "2": ["EDP2"],
                },
            )

    def test_empty_edp_combinations_map_raises_error(self):
        with self.assertRaisesRegex(
                ValueError,
                'Cannot find the data providers for reporting set'):
            report_summaries_from_reporting_set_results(
                self.reporting_set_results, {}
            )

    def test_report_result_missing_edp_combination_key_raises_error(self):
        """Tests that a ValueError is raised for a missing key in edp_combinations."""
        invalid_edp_combinations = self.edp_combinations.copy()
        del invalid_edp_combinations['edp1']
        with self.assertRaisesRegex(
                ValueError,
                'Cannot find the data providers for reporting set edp1'):
            report_summaries_from_reporting_set_results(
                self.reporting_set_results, invalid_edp_combinations
            )

    def test_report_result_missing_cmms_measurement_consumer_id_raises_error(
            self):
        self.reporting_set_results[0].ClearField('cmms_measurement_consumer_id')
        with self.assertRaisesRegex(
                ValueError, 'must have a cmms_measurement_consumer_id'):
            report_summaries_from_reporting_set_results(
                self.reporting_set_results, self.edp_combinations
            )

    def test_report_result_missing_external_report_result_id_raises_error(
            self):
        self.reporting_set_results[0].ClearField('external_report_result_id')
        with self.assertRaisesRegex(ValueError,
                                    'must have an external_report_result_id'):
            report_summaries_from_reporting_set_results(
                self.reporting_set_results, self.edp_combinations
            )

    def test_report_result_missing_reporting_set_result_dimension_raises_error(
        self,
    ):
        self.reporting_set_results[0].ClearField('dimension')
        with self.assertRaisesRegex(ValueError, 'dimension'):
            report_summaries_from_reporting_set_results(
                self.reporting_set_results, self.edp_combinations
            )

    def test_report_result_missing_external_reporting_set_id_raises_error(
            self):
        self.reporting_set_results[0].dimension.ClearField(
            'external_reporting_set_id'
        )
        with self.assertRaisesRegex(ValueError,
                                    'must have an external_reporting_set_id'):
            report_summaries_from_reporting_set_results(
                self.reporting_set_results, self.edp_combinations
            )

    def test_report_result_unspecified_venn_diagram_region_type_raises_error(
            self):
        self.reporting_set_results[0].dimension.venn_diagram_region_type = (
            report_result_pb2.ReportingSetResult.Dimension.VennDiagramRegionType.VENN_DIAGRAM_REGION_TYPE_UNSPECIFIED
        )
        with self.assertRaisesRegex(ValueError,
                                    'must have a venn_diagram_region_type'):
            report_summaries_from_reporting_set_results(
                self.reporting_set_results, self.edp_combinations
            )

    def test_report_result_missing_impression_qualification_filter_raises_error(
            self):
        self.reporting_set_results[0].dimension.ClearField(
            'impression_qualification_filter'
        )
        with self.assertRaisesRegex(
                ValueError, 'must have an impression_qualification_filter'):
            report_summaries_from_reporting_set_results(
                self.reporting_set_results, self.edp_combinations
            )

    def test_report_result_missing_metric_frequency_spec_raises_error(self):
        self.reporting_set_results[0].dimension.ClearField(
            'metric_frequency_spec'
        )
        with self.assertRaisesRegex(ValueError,
                                    'must have a metric_frequency_spec'):
            report_summaries_from_reporting_set_results(
                self.reporting_set_results, self.edp_combinations
            )

    def test_report_result_missing_window_start_raises_error(self):
        self.reporting_set_results[0].reporting_window_results[
            0
        ].key.ClearField('non_cumulative_start')
        with self.assertRaisesRegex(ValueError,
                                    'must have a non-cumulative start date'):
            report_summaries_from_reporting_set_results(
                self.reporting_set_results, self.edp_combinations
            )

    def test_report_result_missing_window_end_raises_error(self):
        self.reporting_set_results[0].reporting_window_results[
            0
        ].key.ClearField('end')
        with self.assertRaisesRegex(ValueError, 'must have an end date'):
            report_summaries_from_reporting_set_results(
                self.reporting_set_results, self.edp_combinations
            )

    def test_report_result_missing_unprocessed_report_result_values_raises_error(
        self,
    ):
        self.reporting_set_results[0].reporting_window_results[
            0
        ].value.ClearField('unprocessed_report_result_values')
        with self.assertRaisesRegex(
            ValueError, 'Missing unprocessed_report_result_values'
        ):
            report_summaries_from_reporting_set_results(
                self.reporting_set_results, self.edp_combinations
            )

    def test_report_result_with_mismatched_population_raises_error(self):
        mismatched_population_report_result_textproto = """
            reporting_set_results {
              cmms_measurement_consumer_id: "abcd"
              external_report_result_id: 123
              external_reporting_set_result_id: 1
              dimension {
                external_reporting_set_id: "edp1"
                venn_diagram_region_type: UNION
                external_impression_qualification_filter_id: "ami"
                metric_frequency_spec { total: true }
                grouping {
                  value_by_path {
                    key: "person.age_group"
                    value { enum_value: "YEARS_18_TO_34" }
                  }
                }
            
              }
              population_size: 10000
              metric_frequency_spec_fingerprint: 1234
              grouping_dimension_fingerprint: 2
              reporting_window_results {
                key { end { year: 2025 month: 10 day: 15 } }
                value { unprocessed_report_result_values { cumulative_results { reach { value: 1 } } } }
              }
            }
            reporting_set_results {
              cmms_measurement_consumer_id: "abcd"
              external_report_result_id: 123
              external_reporting_set_result_id: 2
              dimension {
                external_reporting_set_id: "edp2"
                venn_diagram_region_type: UNION
                external_impression_qualification_filter_id: "ami"
                metric_frequency_spec { total: true }
                grouping {
                  value_by_path {
                    key: "person.age_group"
                    value { enum_value: "YEARS_18_TO_34" }
                  }
                }
              }
              population_size: 20000
              metric_frequency_spec_fingerprint: 1234
              grouping_dimension_fingerprint: 2
              reporting_window_results {
                key { end { year: 2025 month: 10 day: 15 } }
                value { unprocessed_report_result_values { cumulative_results { reach { value: 1 } } } }
              }
            }
        """
        reporting_set_results = _parse_reporting_set_results(
            mismatched_population_report_result_textproto
        )
        with self.assertRaisesRegex(
                ValueError,
                'Inconsistent population sizes found within the same result group.'
        ):
            report_summaries_from_reporting_set_results(
                reporting_set_results, self.edp_combinations
            )

    def test_get_report_summary_v2_from_empty_report_results_raises_error(
            self):
        with self.assertRaisesRegex(ValueError, 'at least one'):
            report_summaries_from_reporting_set_results(
                [], self.edp_combinations
            )

    def test_validate_report_result_non_cumulative_with_total_spec_fails(self):
        ami_report_textproto = """
            reporting_set_results {
              cmms_measurement_consumer_id: "abcd"
              external_report_result_id: 123
              dimension {
                external_reporting_set_id: "edp1"
                venn_diagram_region_type: UNION
                external_impression_qualification_filter_id: "ami"
                metric_frequency_spec { total: true }
                grouping {
                  value_by_path {
                    key: "person.age_group"
                    value { enum_value: "YEARS_18_TO_34" }
                  }
                  value_by_path {
                    key: "person.gender"
                    value { enum_value: "MALE" }
                  }
                }
            
                event_filters {
                  terms {
                    path: "banner_ad.viewable"
                    value { bool_value: true }
                  }
                }
              }
              population_size: 10000
              metric_frequency_spec_fingerprint: 1234
              grouping_dimension_fingerprint: 2
              filter_fingerprint: 5
              reporting_window_results {
                key {
                  non_cumulative_start { year: 2025 month: 10 day: 1 }
                  end { year: 2025 month: 10 day: 15 }
                }
                value {
                  unprocessed_report_result_values {
                    non_cumulative_results {
                      reach {
                        value: 5000
                        univariate_statistics { standard_deviation: 200 }
                      }
                      impression_count { value: 50000 }
                      frequency_histogram {
                        bin_results { key: 1 value { value: 2500 } }
                      }
                    }
                  }
                }
              }
            }
        """
        reporting_set_results = _parse_reporting_set_results(
            ami_report_textproto
        )
        with self.assertRaisesRegex(
                ValueError,
                "Non cumulative results cannot have metric frequency spec of TOTAL.",
        ):
            report_summaries_from_reporting_set_results(
                reporting_set_results, self.edp_combinations
            )

    def test_report_result_with_only_ami_total_measurements(self):
        """Tests conversion for a report with only non-cumulative total measurements."""
        ami_report_textproto = """
            reporting_set_results {
              cmms_measurement_consumer_id: "abcd"
              external_report_result_id: 123
              external_reporting_set_result_id: 1
              dimension {
                external_reporting_set_id: "edp1"
                venn_diagram_region_type: UNION
                external_impression_qualification_filter_id: "ami"
                metric_frequency_spec { total: true }
                grouping {
                  value_by_path {
                    key: "person.age_group"
                    value { enum_value: "YEARS_18_TO_34" }
                  }
                  value_by_path {
                    key: "person.gender"
                    value { enum_value: "MALE" }
                  }
                }
                event_filters {
                  terms {
                    path: "banner_ad.viewable"
                    value { bool_value: true }
                  }
                }
              }
              population_size: 10000
              metric_frequency_spec_fingerprint: 1234
              grouping_dimension_fingerprint: 2
              filter_fingerprint: 5
              reporting_window_results {
                key {
                  non_cumulative_start { year: 2025 month: 10 day: 1 }
                  end { year: 2025 month: 10 day: 15 }
                }
                value {
                  unprocessed_report_result_values {
                    cumulative_results {
                      reach {
                        value: 5000
                        univariate_statistics { standard_deviation: 200 }
                      }
                      impression_count { value: 50000 }
                      frequency_histogram {
                        bin_results { key: 1 value { value: 2500 } }
                      }
                    }
                  }
                }
              }
            }
        """
        expected_ami_report_summary_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: 123
          groupings {
            path: "person.age_group"
            value { enum_value: "YEARS_18_TO_34" }
          }
          groupings {
            path: "person.gender"
            value { enum_value: "MALE" }
          }
          event_filters {
            terms {
              path: "banner_ad.viewable"
              value { bool_value: true }
            }
          }
          population: 10000
          report_summary_set_results {
            external_reporting_set_result_id: 1
            impression_filter: "ami"
            set_operation: "union"
            data_providers: "edp1"
            metric_frequency_spec { total: true }
            whole_campaign_result {
              key {
                non_cumulative_start { year: 2025 month: 10 day: 1 }
                end { year: 2025 month: 10 day: 15 }
              }
              reach {
                value: 5000
                standard_deviation: 200
                metric: "reach_1_cumulative_2025_10_15"
              }
              impression_count {
                value: 50000
                metric: "impression_1_cumulative_2025_10_15"
              }
              frequency {
                bins {
                  key: 1
                  value { value: 2500 }
                }
                metric: "frequency_1_cumulative_2025_10_15"
              }
            }
          }
        """
        reporting_set_results = _parse_reporting_set_results(
            ami_report_textproto
        )
        report_summaries = report_summaries_from_reporting_set_results(
            reporting_set_results, self.edp_combinations
        )
        expected_report_summary = text_format.Parse(
            expected_ami_report_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2(),
        )

        self.assertEqual(len(report_summaries), 1)
        self.assertEqual(report_summaries[0], expected_report_summary)

    def test_report_result_with_only_custom_total_measurements(self):
        """Tests conversion for a report with only non-cumulative total measurements."""
        custom_report_textproto = """
            reporting_set_results {
              cmms_measurement_consumer_id: "abcd"
              external_report_result_id: 123
              external_reporting_set_result_id: 1
              dimension {
                external_reporting_set_id: "edp1"
                venn_diagram_region_type: UNION
                custom: true
                metric_frequency_spec { total: true }
              }
              population_size: 10000
              metric_frequency_spec_fingerprint: 1234
              reporting_window_results {
                key {
                  non_cumulative_start { year: 2025 month: 10 day: 1 }
                  end { year: 2025 month: 10 day: 15 }
                }
                value {
                  unprocessed_report_result_values {
                    cumulative_results {
                      reach {
                        value: 5000
                        univariate_statistics { standard_deviation: 200 }
                      }
                      impression_count { value: 50000 }
                      frequency_histogram {
                        bin_results { key: 1 value { value: 2500 } }
                      }
                    }
                  }
                }
              }
            }
        """
        expected_custom_report_summary_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: 123
          population: 10000
          report_summary_set_results {
            external_reporting_set_result_id: 1
            impression_filter: "custom"
            set_operation: "union"
            data_providers: "edp1"
            metric_frequency_spec { total: true }
            whole_campaign_result {
              key {
                non_cumulative_start { year: 2025 month: 10 day: 1 }
                end { year: 2025 month: 10 day: 15 }
              }
              reach {
                value: 5000
                standard_deviation: 200
                metric: "reach_1_cumulative_2025_10_15"
              }
              impression_count {
                value: 50000
                metric: "impression_1_cumulative_2025_10_15"
              }
              frequency {
                bins {
                  key: 1
                  value { value: 2500 }
                }
                metric: "frequency_1_cumulative_2025_10_15"
              }
            }
          }
        """
        reporting_set_results = _parse_reporting_set_results(
            custom_report_textproto
        )
        report_summaries = report_summaries_from_reporting_set_results(
            reporting_set_results, self.edp_combinations
        )
        expected_report_summary = text_format.Parse(
            expected_custom_report_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2(),
        )

        self.assertEqual(len(report_summaries), 1)
        self.assertEqual(report_summaries[0], expected_report_summary)

    def test_report_result_with_only_non_cumulative_ami_weekly_measurements(
            self):
        """Tests conversion for a report with only non-cumulative weekly measurements."""
        ami_report_textproto = """
            reporting_set_results {
              cmms_measurement_consumer_id: "abcd"
              external_report_result_id: 123
              external_reporting_set_result_id: 1
              dimension {
                external_reporting_set_id: "edp1"
                venn_diagram_region_type: UNION
                external_impression_qualification_filter_id: "ami"
                metric_frequency_spec { weekly: MONDAY }
              }
              population_size: 10000
              metric_frequency_spec_fingerprint: 1234
              reporting_window_results {
                key {
                  non_cumulative_start { year: 2025 month: 10 day: 1 }
                  end { year: 2025 month: 10 day: 8 }
                }
                value {
                  unprocessed_report_result_values {
                    non_cumulative_results {
                      reach { value: 2000 }
                      impression_count { value: 20000 }
                      frequency_histogram {
                        bin_results { key: 1 value { value: 1000 } }
                      }
                    }
                  }
                }
              }
              reporting_window_results {
                key {
                  non_cumulative_start { year: 2025 month: 10 day: 8 }
                  end { year: 2025 month: 10 day: 15 }
                }
                value {
                  unprocessed_report_result_values {
                    non_cumulative_results {
                      reach {
                        value: 3000
                        univariate_statistics { standard_deviation: 150 }
                      }
                      impression_count { value: 30000 }
                      frequency_histogram {
                        bin_results { key: 1 value { value: 1500 } }
                      }
                    }
                  }
                }
              }
            }
        """
        expected_ami_summary_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: 123
          population: 10000
          report_summary_set_results {
            external_reporting_set_result_id: 1
            impression_filter: "ami"
            set_operation: "union"
            data_providers: "edp1"
            metric_frequency_spec { weekly: MONDAY }
            non_cumulative_results {
              key {
                non_cumulative_start { year: 2025 month: 10 day: 1 }
                end { year: 2025 month: 10 day: 8 }
              }
              reach {
                value: 2000
                metric: "reach_1_non_cumulative_2025_10_08"
              }
              impression_count {
                value: 20000
                metric: "impression_1_non_cumulative_2025_10_08"
              }
              frequency {
                bins {
                  key: 1
                  value { value: 1000 }
                }
                metric: "frequency_1_non_cumulative_2025_10_08"
              }
            }
            non_cumulative_results {
              key {
                non_cumulative_start { year: 2025 month: 10 day: 8 }
                end { year: 2025 month: 10 day: 15 }
              }
              reach {
                value: 3000
                standard_deviation: 150
                metric: "reach_1_non_cumulative_2025_10_15"
              }
              impression_count {
                value: 30000
                metric: "impression_1_non_cumulative_2025_10_15"
              }
              frequency {
                bins {
                  key: 1
                  value { value: 1500 }
                }
                metric: "frequency_1_non_cumulative_2025_10_15"
              }
            }
          }
        """
        reporting_set_results = _parse_reporting_set_results(
            ami_report_textproto
        )
        report_summaries = report_summaries_from_reporting_set_results(
            reporting_set_results, self.edp_combinations
        )
        expected_report_summary = text_format.Parse(
            expected_ami_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2())

        self.assertEqual(len(report_summaries), 1)
        self.assertEqual(report_summaries[0], expected_report_summary)

    def test_report_result_with_only_non_cumulative_custom_weekly_measurements(
            self):
        """Tests conversion for a report with only non-cumulative weekly measurements."""
        custom_report_textproto = """
            reporting_set_results {
              cmms_measurement_consumer_id: "abcd"
              external_report_result_id: 123
              external_reporting_set_result_id: 1
              dimension {
                external_reporting_set_id: "edp1"
                venn_diagram_region_type: UNION
                custom: true
                metric_frequency_spec { weekly: MONDAY }
                grouping {
                  value_by_path {
                    key: "person.age_group"
                    value { enum_value: "YEARS_18_TO_34" }
                  }
                  value_by_path {
                    key: "person.gender"
                    value { enum_value: "MALE" }
                  }
                }
                event_filters {
                  terms {
                    path: "banner_ad.viewable"
                    value { bool_value: true }
                  }
                }
              }
                population_size: 10000
                metric_frequency_spec_fingerprint: 1234
                grouping_dimension_fingerprint: 2
                filter_fingerprint: 5
                reporting_window_results {
                  key {
                    non_cumulative_start { year: 2025 month: 10 day: 1 }
                    end { year: 2025 month: 10 day: 8 }
                  }
                  value {
                    unprocessed_report_result_values {
                      non_cumulative_results {
                        reach { value: 2000 }
                        impression_count { value: 20000 }
                        frequency_histogram {
                          bin_results { key: 1 value { value: 1000 } }
                        }
                      }
                    }
                  }
                }
                reporting_window_results {
                  key {
                    non_cumulative_start { year: 2025 month: 10 day: 8 }
                    end { year: 2025 month: 10 day: 15 }
                  }
                  value {
                    unprocessed_report_result_values {
                      non_cumulative_results {
                        reach {
                          value: 3000
                          univariate_statistics { standard_deviation: 150 }
                        }
                        impression_count { value: 30000 }
                        frequency_histogram {
                          bin_results { key: 1 value { value: 1500 } }
                        }
                      }
                    }
                  }
                }
            }
        """
        expected_custom_summary_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: 123
          groupings {
            path: "person.age_group"
            value { enum_value: "YEARS_18_TO_34" }
          }
          groupings {
            path: "person.gender"
            value { enum_value: "MALE" }
          }
          event_filters {
            terms {
              path: "banner_ad.viewable"
              value { bool_value: true }
            }
          }
          population: 10000
          report_summary_set_results {
            external_reporting_set_result_id: 1
            impression_filter: "custom"
            set_operation: "union"
            data_providers: "edp1"
            metric_frequency_spec { weekly: MONDAY }
            non_cumulative_results {
              key {
                non_cumulative_start { year: 2025 month: 10 day: 1 }
                end { year: 2025 month: 10 day: 8 }
              }
              reach {
                value: 2000
                metric: "reach_1_non_cumulative_2025_10_08"
              }
              impression_count {
                value: 20000
                metric: "impression_1_non_cumulative_2025_10_08"
              }
              frequency {
                bins {
                  key: 1
                  value { value: 1000 }
                }
                metric: "frequency_1_non_cumulative_2025_10_08"
              }
            }
            non_cumulative_results {
              key {
                non_cumulative_start { year: 2025 month: 10 day: 8 }
                end { year: 2025 month: 10 day: 15 }
              }
              reach {
                value: 3000
                standard_deviation: 150
                metric: "reach_1_non_cumulative_2025_10_15"
              }
              impression_count {
                value: 30000
                metric: "impression_1_non_cumulative_2025_10_15"
              }
              frequency {
                bins {
                  key: 1
                  value { value: 1500 }
                }
                metric: "frequency_1_non_cumulative_2025_10_15"
              }
            }
         }
        """
        reporting_set_results = _parse_reporting_set_results(
            custom_report_textproto
        )
        report_summaries = report_summaries_from_reporting_set_results(
            reporting_set_results, self.edp_combinations
        )
        expected_report_summary = text_format.Parse(
            expected_custom_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2(),
        )

        self.assertEqual(len(report_summaries), 1)
        self.assertEqual(report_summaries[0], expected_report_summary)

    def test_report_result_with_only_cumulative_ami_measurements(self):
        """Tests conversion for a report with only cumulative weekly measurements."""
        ami_report_textproto = """
            reporting_set_results {
              cmms_measurement_consumer_id: "abcd"
              external_report_result_id: 123
              external_reporting_set_result_id: 1
              dimension {
                external_reporting_set_id: "edp1"
                venn_diagram_region_type: UNION
                external_impression_qualification_filter_id: "ami"
                metric_frequency_spec { weekly: MONDAY }
              }
              population_size: 10000
              metric_frequency_spec_fingerprint: 1234
              reporting_window_results {
                key {
                  non_cumulative_start { year: 2025 month: 10 day: 1 }
                  end { year: 2025 month: 10 day: 8 }
                }
                value {
                  unprocessed_report_result_values {
                    cumulative_results {
                      reach { value: 2000 }
                    }
                  }
                }
              }
              reporting_window_results {
                key {
                  non_cumulative_start { year: 2025 month: 10 day: 8 }
                  end { year: 2025 month: 10 day: 15 }
                }
                value {
                  unprocessed_report_result_values {
                    cumulative_results {
                      reach {
                        value: 3000
                        univariate_statistics { standard_deviation: 150 }
                      }
                    }
                  }
                }
              }
            }
        """
        expected_ami_summary_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: 123
          population: 10000
          report_summary_set_results {
            external_reporting_set_result_id: 1
            impression_filter: "ami"
            set_operation: "union"
            data_providers: "edp1"
            metric_frequency_spec { weekly: MONDAY }
            cumulative_results {
              key {
                non_cumulative_start { year: 2025 month: 10 day: 1 }
                end { year: 2025 month: 10 day: 8 }
              }
              reach {
                value: 2000
                metric: "reach_1_cumulative_2025_10_08"
              }
            }
            cumulative_results {
              key {
                non_cumulative_start { year: 2025 month: 10 day: 8 }
                end { year: 2025 month: 10 day: 15 }
              }
              reach {
                value: 3000
                standard_deviation: 150
                metric: "reach_1_cumulative_2025_10_15"
              }
            }
          }
        """
        reporting_set_results = _parse_reporting_set_results(
            ami_report_textproto
        )
        report_summaries = report_summaries_from_reporting_set_results(
            reporting_set_results, self.edp_combinations
        )
        expected_report_summary = text_format.Parse(
            expected_ami_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2())

        self.assertEqual(len(report_summaries), 1)
        self.assertEqual(report_summaries[0], expected_report_summary)

    def test_report_result_with_only_cumulative_custom_measurements(self):
        """Tests conversion for a report with only cumulative measurements."""
        custom_report_textproto = """
            reporting_set_results {
              cmms_measurement_consumer_id: "abcd"
              external_report_result_id: 123
              external_reporting_set_result_id: 1
              dimension {
                external_reporting_set_id: "edp1"
                venn_diagram_region_type: UNION
                custom: true
                metric_frequency_spec { weekly: MONDAY }
              }
              population_size: 10000
              metric_frequency_spec_fingerprint: 1234
              reporting_window_results {
                key {
                  non_cumulative_start { year: 2025 month: 10 day: 1 }
                  end { year: 2025 month: 10 day: 8 }
                }
                value {
                  unprocessed_report_result_values {
                    cumulative_results {
                      reach { value: 2000 }
                    }
                  }
                }
              }
              reporting_window_results {
                key {
                  non_cumulative_start { year: 2025 month: 10 day: 8 }
                  end { year: 2025 month: 10 day: 15 }
                }
                value {
                  unprocessed_report_result_values {
                    cumulative_results {
                      reach {
                        value: 3000
                        univariate_statistics { standard_deviation: 150 }
                      }
                    }
                  }
                }
              }
            }
        """
        expected_custom_summary_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: 123
          population: 10000
          report_summary_set_results {
            external_reporting_set_result_id: 1
            impression_filter: "custom"
            set_operation: "union"
            data_providers: "edp1"
            metric_frequency_spec { weekly: MONDAY }
            cumulative_results {
              key {
                non_cumulative_start { year: 2025 month: 10 day: 1 }
                end { year: 2025 month: 10 day: 8 }
              }
              reach {
                value: 2000
                metric: "reach_1_cumulative_2025_10_08"
              }
            }
            cumulative_results {
              key {
                non_cumulative_start { year: 2025 month: 10 day: 8 }
                end { year: 2025 month: 10 day: 15 }
              }
              reach {
                value: 3000
                standard_deviation: 150
                metric: "reach_1_cumulative_2025_10_15"
              }
            }
          }
        """
        reporting_set_results = _parse_reporting_set_results(
            custom_report_textproto
        )
        report_summaries = report_summaries_from_reporting_set_results(
            reporting_set_results, self.edp_combinations
        )
        expected_report_summary = text_format.Parse(
            expected_custom_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2(),
        )

        self.assertEqual(len(report_summaries), 1)
        self.assertEqual(report_summaries[0], expected_report_summary)

    def test_get_report_summary_v2_from_a_large_sample_report_result(self):
        """Tests that the full report result is parsed into a summary correctly."""

        report_summaries = report_summaries_from_reporting_set_results(
            self.reporting_set_results, self.edp_combinations
        )

        # Checks that there are two demographic groups in the sample report result.
        # One is (MALE, 18_34, banner_ad.viewable=True), the other is
        # (MALE, 35_54, banner_ad.viewable=True).
        self.assertEqual(len(report_summaries), 2)

        report_summary_18_34 = None
        report_summary_35_54 = None

        grouping_18_34 = event_template_field_pb2.EventTemplateField(
            path='person.age_group', value={'enum_value': 'YEARS_18_TO_34'})
        grouping_35_54 = event_template_field_pb2.EventTemplateField(
            path='person.age_group', value={'enum_value': 'YEARS_35_TO_54'})

        for report_summary in report_summaries:
            if grouping_18_34 in report_summary.groupings:
                report_summary_18_34 = report_summary
            elif grouping_35_54 in report_summary.groupings:
                report_summary_35_54 = report_summary

        self.assertIsNotNone(report_summary_18_34)
        self.assertIsNotNone(report_summary_35_54)

        # Verifies that there are 17 results for the 18-34 age group.
        self.assertEqual(len(report_summary_18_34.report_summary_set_results),
                         17)
        # Verifies that there are 8 results for the 35-54 age group.
        self.assertEqual(len(report_summary_35_54.report_summary_set_results),
                         8)

        # Verifies the grouping predicates for the 18-34 age group.
        expected_groupings_18_34 = [
            event_template_field_pb2.EventTemplateField(
                path='person.age_group',
                value={'enum_value': 'YEARS_18_TO_34'}),
            event_template_field_pb2.EventTemplateField(
                path='person.gender', value={'enum_value': 'MALE'}),
        ]
        self.assertCountEqual(report_summary_18_34.groupings,
                              expected_groupings_18_34)

        # Verifies the grouping predicates for the 35-54 age group.
        expected_groupings_35_54 = [
            event_template_field_pb2.EventTemplateField(
                path='person.age_group',
                value={'enum_value': 'YEARS_35_TO_54'}),
            event_template_field_pb2.EventTemplateField(
                path='person.gender', value={'enum_value': 'MALE'}),
        ]
        self.assertCountEqual(report_summary_35_54.groupings,
                              expected_groupings_35_54)

        # Verifies that the total campaign measurements for the 3 edps are in the
        # report summary.
        expected_reporting_summary_set_result_18_34 = text_format.Parse(
            """
            external_reporting_set_result_id: 25
            impression_filter: "custom"
            set_operation: "union"
            data_providers: "edp1"
            data_providers: "edp2"
            data_providers: "edp3"
            metric_frequency_spec { total: true }
            whole_campaign_result {
              key {
                non_cumulative_start { year: 2025 month: 10 day: 1 }
                end { year: 2025 month: 10 day: 15 }
              }
              reach {
                value: 19030737
                standard_deviation: 10000.0
                metric: "reach_25_cumulative_2025_10_15"
              }
              impression_count {
                value: 35936915
                standard_deviation: 10000.0
                metric: "impression_25_cumulative_2025_10_15"
              }
              frequency {
                bins { key: 1 value { value: 9822316 standard_deviation: 10000.0 } }
                bins { key: 2 value { value: 4911158 standard_deviation: 10000.0 } }
                bins { key: 3 value { value: 2455579 standard_deviation: 10000.0 } }
                bins { key: 4 value { value: 1227790 standard_deviation: 10000.0 } }
                bins { key: 5 value { value: 613894 standard_deviation: 10000.0 } }
                metric: "frequency_25_cumulative_2025_10_15"
              }
            }
            """,
            report_summary_v2_pb2.ReportSummaryV2.ReportSummarySetResult(),
        )

        self.assertIn(expected_reporting_summary_set_result_18_34,
                      report_summary_18_34.report_summary_set_results)

        # Verifies that a specific, expected set result is present in the summary.
        expected_reporting_summary_set_result_35_54 = text_format.Parse(
            """
            external_reporting_set_result_id: 1
            impression_filter: "ami"
            set_operation: "union"
            data_providers: "edp1"
            metric_frequency_spec { weekly: MONDAY }
            cumulative_results {
              key {
                non_cumulative_start { year: 2025 month: 10 day: 1 }
                end { year: 2025 month: 10 day: 8 }
              }
              reach {
                value: 9992500
                standard_deviation: 10000.0
                metric: "reach_1_cumulative_2025_10_08"
              }
            }
            cumulative_results {
              key {
                non_cumulative_start { year: 2025 month: 10 day: 8 }
                end { year: 2025 month: 10 day: 15 }
              }
              reach {
                value: 11998422
                standard_deviation: 10000.0
                metric: "reach_1_cumulative_2025_10_15"
              }
            }
            non_cumulative_results {
              key {
                non_cumulative_start { year: 2025 month: 10 day: 1 }
                end { year: 2025 month: 10 day: 8 }
              }
              reach {
                value: 10008130
                standard_deviation: 10000.0
                metric: "reach_1_non_cumulative_2025_10_08"
              }
              impression_count {
                value: 18379493
                standard_deviation: 10000.0
                metric: "impression_1_non_cumulative_2025_10_08"
              }
              frequency {
                bins { key: 1 value { value: 5165486 standard_deviation: 10000.0 } }
                bins { key: 2 value { value: 2582743 standard_deviation: 10000.0 } }
                bins { key: 3 value { value: 1291372 standard_deviation: 10000.0 } }
                bins { key: 4 value { value: 645686 standard_deviation: 10000.0 } }
                bins { key: 5 value { value: 322843 standard_deviation: 10000.0 } }
                metric: "frequency_1_non_cumulative_2025_10_08"
              }
            }
            non_cumulative_results {
              key {
                non_cumulative_start { year: 2025 month: 10 day: 8 }
                end { year: 2025 month: 10 day: 15 }
              }
              reach {
                value: 2452001
                standard_deviation: 10000.0
                metric: "reach_1_non_cumulative_2025_10_15"
              }
              impression_count {
                value: 4471035
                standard_deviation: 10000.0
                metric: "impression_1_non_cumulative_2025_10_15"
              }
              frequency {
                bins { key: 1 value { value: 1265549 standard_deviation: 10000.0 } }
                bins { key: 2 value { value: 632775 standard_deviation: 10000.0 } }
                bins { key: 3 value { value: 316388 standard_deviation: 10000.0 } }
                bins { key: 4 value { value: 158194 standard_deviation: 10000.0 } }
                bins { key: 5 value { value: 79095 standard_deviation: 10000.0 } }
                metric: "frequency_1_non_cumulative_2025_10_15"
              }
            }
            """,
            report_summary_v2_pb2.ReportSummaryV2.ReportSummarySetResult(),
        )

        self.assertIn(expected_reporting_summary_set_result_35_54,
                      report_summary_35_54.report_summary_set_results)


def _parse_reporting_set_results(
    list_response_textproto,
) -> list[report_result_pb2.ReportingSetResult]:
    response = text_format.Parse(
        list_response_textproto,
        report_results_service_pb2.ListReportingSetResultsResponse(),
    )
    return response.reporting_set_results


if __name__ == '__main__':
    unittest.main()
