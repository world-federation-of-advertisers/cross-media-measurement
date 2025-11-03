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
    get_report_summary_v2_from_report_result, )
from src.main.proto.wfa.measurement.internal.reporting.postprocessing import (
    report_summary_v2_pb2, )
from wfa.measurement.internal.reporting.v2 import report_result_pb2
from wfa.measurement.internal.reporting.v2 import event_template_field_pb2

ReportSummaryWindowResult = (report_summary_v2_pb2.ReportSummaryV2.
                             ReportSummarySetResult.ReportSummaryWindowResult)


class ReportConversionTest(unittest.TestCase):

    def setUp(self):
        super().setUp()
        with open(
                'src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_result.textproto',
                'r') as file:
            report_result_textproto = file.read()
        self.report_result = text_format.Parse(
            report_result_textproto, report_result_pb2.ReportResult())

        self.edp_combinations = {
            'edp1': ['edp1'],
            'edp2': ['edp2'],
            'edp3': ['edp3'],
            'edp1_edp2': ['edp1', 'edp2'],
            'edp1_edp2_edp3': ['edp1', 'edp2', 'edp3'],
        }

    def test_empty_edp_combinations_map_raises_error(self):
        with self.assertRaisesRegex(
                ValueError,
                'Cannot find the data providers for reporting set'):
            get_report_summary_v2_from_report_result(self.report_result, {})

    def test_report_result_missing_edp_combination_key_raises_error(self):
        """Tests that a ValueError is raised for a missing key in edp_combinations."""
        invalid_edp_combinations = self.edp_combinations.copy()
        del invalid_edp_combinations['edp1']
        with self.assertRaisesRegex(
                ValueError,
                'Cannot find the data providers for reporting set edp1'):
            get_report_summary_v2_from_report_result(self.report_result,
                                                     invalid_edp_combinations)

    def test_report_result_missing_cmms_measurement_consumer_id_raises_error(
            self):
        self.report_result.ClearField('cmms_measurement_consumer_id')
        with self.assertRaisesRegex(
                ValueError, 'must have a cmms_measurement_consumer_id'):
            get_report_summary_v2_from_report_result(self.report_result,
                                                     self.edp_combinations)

    def test_report_result_missing_external_report_result_id_raises_error(
            self):
        self.report_result.ClearField('external_report_result_id')
        with self.assertRaisesRegex(ValueError,
                                    'must have an external_report_result_id'):
            get_report_summary_v2_from_report_result(self.report_result,
                                                     self.edp_combinations)

    def test_report_result_missing_report_start_raises_error(self):
        self.report_result.ClearField('report_start')
        with self.assertRaisesRegex(ValueError,
                                    'must have a report_start date'):
            get_report_summary_v2_from_report_result(self.report_result,
                                                     self.edp_combinations)

    def test_report_result_missing_reporting_set_result_key_raises_error(self):
        self.report_result.reporting_set_results[0].ClearField('key')
        with self.assertRaisesRegex(ValueError, 'must have a key'):
            get_report_summary_v2_from_report_result(self.report_result,
                                                     self.edp_combinations)

    def test_report_result_missing_reporting_set_result_value_raises_error(
            self):
        self.report_result.reporting_set_results[0].ClearField('value')
        with self.assertRaisesRegex(ValueError, 'must have a value'):
            get_report_summary_v2_from_report_result(self.report_result,
                                                     self.edp_combinations)

    def test_report_result_missing_external_reporting_set_id_raises_error(
            self):
        self.report_result.reporting_set_results[0].key.ClearField(
            'external_reporting_set_id')
        with self.assertRaisesRegex(ValueError,
                                    'must have an external_reporting_set_id'):
            get_report_summary_v2_from_report_result(self.report_result,
                                                     self.edp_combinations)

    def test_report_result_unspecified_venn_diagram_region_type_raises_error(
            self):
        self.report_result.reporting_set_results[
            0].key.venn_diagram_region_type = (
                report_result_pb2.ReportResult.VennDiagramRegionType.
                VENN_DIAGRAM_REGION_TYPE_UNSPECIFIED)
        with self.assertRaisesRegex(ValueError,
                                    'must have a venn_diagram_region_type'):
            get_report_summary_v2_from_report_result(self.report_result,
                                                     self.edp_combinations)

    def test_report_result_missing_impression_qualification_filter_raises_error(
            self):
        self.report_result.reporting_set_results[0].key.ClearField(
            'impression_qualification_filter')
        with self.assertRaisesRegex(
                ValueError, 'must have an impression_qualification_filter'):
            get_report_summary_v2_from_report_result(self.report_result,
                                                     self.edp_combinations)

    def test_report_result_missing_metric_frequency_spec_raises_error(self):
        self.report_result.reporting_set_results[0].key.ClearField(
            'metric_frequency_spec')
        with self.assertRaisesRegex(ValueError,
                                    'must have a metric_frequency_spec'):
            get_report_summary_v2_from_report_result(self.report_result,
                                                     self.edp_combinations)

    def test_report_result_missing_window_start_raises_error(self):
        self.report_result.reporting_set_results[
            0].value.reporting_window_results[0].key.ClearField(
                'non_cumulative_start')
        with self.assertRaisesRegex(ValueError,
                                    'must have a non-cumulative start date'):
            get_report_summary_v2_from_report_result(self.report_result,
                                                     self.edp_combinations)

    def test_report_result_missing_window_end_raises_error(self):
        self.report_result.reporting_set_results[
            0].value.reporting_window_results[0].key.ClearField('end')
        with self.assertRaisesRegex(ValueError, 'must have an end date'):
            get_report_summary_v2_from_report_result(self.report_result,
                                                     self.edp_combinations)

    def test_report_result_missing_noisy_report_result_values_raises_error(
            self):
        self.report_result.reporting_set_results[
            0].value.reporting_window_results[0].value.ClearField(
                'noisy_report_result_values')
        with self.assertRaisesRegex(ValueError,
                                    'Missing noisy_report_result_values'):
            get_report_summary_v2_from_report_result(self.report_result,
                                                     self.edp_combinations)

    def test_report_result_with_mismatched_population_raises_error(self):
        mismatched_population_report_result_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: 123
          report_start { year: 2025 month: 10 day: 1 }
          reporting_set_results {
            key {
              external_reporting_set_id: "edp1"
              venn_diagram_region_type: UNION
              external_impression_qualification_filter_id: "ami"
              metric_frequency_spec { total: true }
              groupings {
                path: "person.age_group"
                value { enum_value: "YEARS_18_TO_34" }
              }
            }
            value {
              population_size: 10000
              reporting_window_results {
                key { end { year: 2025 month: 10 day: 15 } }
                value { noisy_report_result_values { cumulative_results { reach { value: 1 } } } }
              }
            }
          }
          reporting_set_results {
            key {
              external_reporting_set_id: "edp2"
              venn_diagram_region_type: UNION
              external_impression_qualification_filter_id: "ami"
              metric_frequency_spec { total: true }
              groupings {
                path: "person.age_group"
                value { enum_value: "YEARS_18_TO_34" }
              }
            }
            value {
              population_size: 20000
              reporting_window_results {
                key { end { year: 2025 month: 10 day: 15 } }
                value { noisy_report_result_values { cumulative_results { reach { value: 1 } } } }
              }
            }
          }
        """
        report_result = text_format.Parse(
            mismatched_population_report_result_textproto,
            report_result_pb2.ReportResult())
        with self.assertRaisesRegex(
                ValueError,
                'Inconsistent population sizes found within the same result group.'
        ):
            get_report_summary_v2_from_report_result(report_result,
                                                     self.edp_combinations)

    def test_get_report_summary_v2_from_empty_report_results(self):
        report_result = report_result_pb2.ReportResult()
        report_result.cmms_measurement_consumer_id = 'faked_id'
        report_result.external_report_result_id = 1
        report_result.report_start.year = 2025
        report_result.report_start.month = 1
        report_result.report_start.day = 1

        report_summaries = get_report_summary_v2_from_report_result(
            report_result, self.edp_combinations)

        self.assertEqual(report_summaries, [])

    def test_report_result_with_only_ami_total_measurements(self):
        """Tests conversion for a report with only non-cumulative total measurements."""
        ami_report_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: 123
          report_start { year: 2025 month: 10 day: 1 }
          reporting_set_results {
            key {
              external_reporting_set_id: "edp1"
              venn_diagram_region_type: UNION
              external_impression_qualification_filter_id: "ami"
              metric_frequency_spec { total: true }
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
            }
            value {
              population_size: 10000
              reporting_window_results {
                key {
                  non_cumulative_start { year: 2025 month: 10 day: 1 }
                  end { year: 2025 month: 10 day: 15 }
                }
                value {
                  noisy_report_result_values {
                    non_cumulative_results {
                      reach {
                        value: 5000
                        univariate_statistics { standard_deviation: 200 }
                      }
                      impression_count { value: 50000 }
                      frequency_histogram {
                        bin_results { key: "1" value { value: 2500 } }
                      }
                    }
                  }
                }
              }
            }
          }
        """
        expected_ami_report_summary_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: "123"
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
            impression_filter: "ami"
            set_operation: "union"
            data_providers: "edp1"
            non_cumulative_results {
              metric_frequency_type: TOTAL
              reach {
                value: 5000
                standard_deviation: 200
                metric: "reach_non_cumulative_edp1_ami_2025_10_15"
              }
              impression_count {
                value: 50000
                metric: "impression_non_cumulative_edp1_ami_2025_10_15"
              }
              frequency {
                bins {
                  key: "1"
                  value { value: 2500 }
                }
                metric: "frequency_non_cumulative_edp1_ami_2025_10_15"
              }
            }
          }
        """
        report_result = text_format.Parse(ami_report_textproto,
                                          report_result_pb2.ReportResult())
        report_summaries = get_report_summary_v2_from_report_result(
            report_result, self.edp_combinations)
        expected_report_summary = text_format.Parse(
            expected_ami_report_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2(),
        )

        self.assertEqual(len(report_summaries), 1)
        self.assertEqual(report_summaries[0], expected_report_summary)

    def test_report_result_with_only_custom_total_measurements(self):
        """Tests conversion for a report with only non-cumulative total measurements."""
        custom_report_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: 123
          report_start { year: 2025 month: 10 day: 1 }
          reporting_set_results {
            key {
              external_reporting_set_id: "edp1"
              venn_diagram_region_type: UNION
              custom: true
              metric_frequency_spec { total: true }
            }
            value {
              population_size: 10000
              reporting_window_results {
                key {
                  non_cumulative_start { year: 2025 month: 10 day: 1 }
                  end { year: 2025 month: 10 day: 15 }
                }
                value {
                  noisy_report_result_values {
                    non_cumulative_results {
                      reach {
                        value: 5000
                        univariate_statistics { standard_deviation: 200 }
                      }
                      impression_count { value: 50000 }
                      frequency_histogram {
                        bin_results { key: "1" value { value: 2500 } }
                      }
                    }
                  }
                }
              }
            }
          }
        """
        expected_custom_report_summary_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: "123"
          population: 10000
          report_summary_set_results {
            impression_filter: "custom"
            set_operation: "union"
            data_providers: "edp1"
            non_cumulative_results {
              metric_frequency_type: TOTAL
              reach {
                value: 5000
                standard_deviation: 200
                metric: "reach_non_cumulative_edp1_custom_2025_10_15"
              }
              impression_count {
                value: 50000
                metric: "impression_non_cumulative_edp1_custom_2025_10_15"
              }
              frequency {
                bins {
                  key: "1"
                  value { value: 2500 }
                }
                metric: "frequency_non_cumulative_edp1_custom_2025_10_15"
              }
            }
          }
        """
        report_result = text_format.Parse(custom_report_textproto,
                                          report_result_pb2.ReportResult())
        report_summaries = get_report_summary_v2_from_report_result(
            report_result, self.edp_combinations)
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
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: 123
          report_start { year: 2025 month: 10 day: 1 }
          reporting_set_results {
            key {
              external_reporting_set_id: "edp1"
              venn_diagram_region_type: UNION
              external_impression_qualification_filter_id: "ami"
              metric_frequency_spec { weekly: MONDAY }
            }
            value {
              population_size: 10000
              reporting_window_results {
                key {
                  non_cumulative_start { year: 2025 month: 10 day: 1 }
                  end { year: 2025 month: 10 day: 8 }
                }
                value {
                  noisy_report_result_values {
                    non_cumulative_results {
                      reach { value: 2000 }
                      impression_count { value: 20000 }
                      frequency_histogram {
                        bin_results { key: "1" value { value: 1000 } }
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
                  noisy_report_result_values {
                    non_cumulative_results {
                      reach {
                        value: 3000
                        univariate_statistics { standard_deviation: 150 }
                      }
                      impression_count { value: 30000 }
                      frequency_histogram {
                        bin_results { key: "1" value { value: 1500 } }
                      }
                    }
                  }
                }
              }
            }
          }
        """
        expected_ami_summary_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: "123"
          population: 10000
          report_summary_set_results {
            impression_filter: "ami"
            set_operation: "union"
            data_providers: "edp1"
            non_cumulative_results {
              metric_frequency_type: WEEKLY
              reach {
                value: 2000
                metric: "reach_non_cumulative_edp1_ami_2025_10_08"
              }
              impression_count {
                value: 20000
                metric: "impression_non_cumulative_edp1_ami_2025_10_08"
              }
              frequency {
                bins {
                  key: "1"
                  value { value: 1000 }
                }
                metric: "frequency_non_cumulative_edp1_ami_2025_10_08"
              }
            }
            non_cumulative_results {
              metric_frequency_type: WEEKLY
              reach {
                value: 3000
                standard_deviation: 150
                metric: "reach_non_cumulative_edp1_ami_2025_10_15"
              }
              impression_count {
                value: 30000
                metric: "impression_non_cumulative_edp1_ami_2025_10_15"
              }
              frequency {
                bins {
                  key: "1"
                  value { value: 1500 }
                }
                metric: "frequency_non_cumulative_edp1_ami_2025_10_15"
              }
            }
          }
        """
        report_result = text_format.Parse(ami_report_textproto,
                                          report_result_pb2.ReportResult())
        report_summaries = get_report_summary_v2_from_report_result(
            report_result, self.edp_combinations)
        expected_report_summary = text_format.Parse(
            expected_ami_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2())

        self.assertEqual(len(report_summaries), 1)
        self.assertEqual(report_summaries[0], expected_report_summary)

    def test_report_result_with_only_non_cumulative_custom_weekly_measurements(
            self):
        """Tests conversion for a report with only non-cumulative weekly measurements."""
        custom_report_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: 123
          report_start { year: 2025 month: 10 day: 1 }
          reporting_set_results {
            key {
              external_reporting_set_id: "edp1"
              venn_diagram_region_type: UNION
              custom: true
              metric_frequency_spec { weekly: MONDAY }
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
            }
            value {
              population_size: 10000
              reporting_window_results {
                key {
                  non_cumulative_start { year: 2025 month: 10 day: 1 }
                  end { year: 2025 month: 10 day: 8 }
                }
                value {
                  noisy_report_result_values {
                    non_cumulative_results {
                      reach { value: 2000 }
                      impression_count { value: 20000 }
                      frequency_histogram {
                        bin_results { key: "1" value { value: 1000 } }
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
                  noisy_report_result_values {
                    non_cumulative_results {
                      reach {
                        value: 3000
                        univariate_statistics { standard_deviation: 150 }
                      }
                      impression_count { value: 30000 }
                      frequency_histogram {
                        bin_results { key: "1" value { value: 1500 } }
                      }
                    }
                  }
                }
              }
            }
          }
        """
        expected_custom_summary_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: "123"
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
            impression_filter: "custom"
            set_operation: "union"
            data_providers: "edp1"
            non_cumulative_results {
              metric_frequency_type: WEEKLY
              reach {
                value: 2000
                metric: "reach_non_cumulative_edp1_custom_2025_10_08"
              }
              impression_count {
                value: 20000
                metric: "impression_non_cumulative_edp1_custom_2025_10_08"
              }
              frequency {
                bins {
                  key: "1"
                  value { value: 1000 }
                }
                metric: "frequency_non_cumulative_edp1_custom_2025_10_08"
              }
            }
            non_cumulative_results {
              metric_frequency_type: WEEKLY
              reach {
                value: 3000
                standard_deviation: 150
                metric: "reach_non_cumulative_edp1_custom_2025_10_15"
              }
              impression_count {
                value: 30000
                metric: "impression_non_cumulative_edp1_custom_2025_10_15"
              }
              frequency {
                bins {
                  key: "1"
                  value { value: 1500 }
                }
                metric: "frequency_non_cumulative_edp1_custom_2025_10_15"
              }
            }
         }
        """
        report_result = text_format.Parse(custom_report_textproto,
                                          report_result_pb2.ReportResult())
        report_summaries = get_report_summary_v2_from_report_result(
            report_result, self.edp_combinations)
        expected_report_summary = text_format.Parse(
            expected_custom_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2(),
        )

        self.assertEqual(len(report_summaries), 1)
        self.assertEqual(report_summaries[0], expected_report_summary)

    def test_report_result_with_only_cumulative_ami_measurements(self):
        """Tests conversion for a report with only cumulative weekly measurements."""
        ami_report_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: 123
          report_start { year: 2025 month: 10 day: 1 }
          reporting_set_results {
            key {
              external_reporting_set_id: "edp1"
              venn_diagram_region_type: UNION
              external_impression_qualification_filter_id: "ami"
              metric_frequency_spec { weekly: MONDAY }
            }
            value {
              population_size: 10000
              reporting_window_results {
                key {
                  non_cumulative_start { year: 2025 month: 10 day: 1 }
                  end { year: 2025 month: 10 day: 8 }
                }
                value {
                  noisy_report_result_values {
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
                  noisy_report_result_values {
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
          }
        """
        expected_ami_summary_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: "123"
          population: 10000
          report_summary_set_results {
            impression_filter: "ami"
            set_operation: "union"
            data_providers: "edp1"
            cumulative_results {
              metric_frequency_type: WEEKLY
              reach {
                value: 2000
                metric: "reach_cumulative_edp1_ami_2025_10_08"
              }
            }
            cumulative_results {
              metric_frequency_type: WEEKLY
              reach {
                value: 3000
                standard_deviation: 150
                metric: "reach_cumulative_edp1_ami_2025_10_15"
              }
            }
          }
        """
        report_result = text_format.Parse(ami_report_textproto,
                                          report_result_pb2.ReportResult())
        report_summaries = get_report_summary_v2_from_report_result(
            report_result, self.edp_combinations)
        expected_report_summary = text_format.Parse(
            expected_ami_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2())

        self.assertEqual(len(report_summaries), 1)
        self.assertEqual(report_summaries[0], expected_report_summary)

    def test_report_result_with_only_cumulative_custom_measurements(self):
        """Tests conversion for a report with only cumulative measurements."""
        custom_report_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: 123
          report_start { year: 2025 month: 10 day: 1 }
          reporting_set_results {
            key {
              external_reporting_set_id: "edp1"
              venn_diagram_region_type: UNION
              custom: true
              metric_frequency_spec { weekly: MONDAY }
            }
            value {
              population_size: 10000
              reporting_window_results {
                key {
                  non_cumulative_start { year: 2025 month: 10 day: 1 }
                  end { year: 2025 month: 10 day: 8 }
                }
                value {
                  noisy_report_result_values {
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
                  noisy_report_result_values {
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
          }
        """
        expected_custom_summary_textproto = """
          cmms_measurement_consumer_id: "abcd"
          external_report_result_id: "123"
          population: 10000
          report_summary_set_results {
            impression_filter: "custom"
            set_operation: "union"
            data_providers: "edp1"
            cumulative_results {
              metric_frequency_type: WEEKLY
              reach {
                value: 2000
                metric: "reach_cumulative_edp1_custom_2025_10_08"
              }
            }
            cumulative_results {
              metric_frequency_type: WEEKLY
              reach {
                value: 3000
                standard_deviation: 150
                metric: "reach_cumulative_edp1_custom_2025_10_15"
              }
            }
          }
        """
        report_result = text_format.Parse(custom_report_textproto,
                                          report_result_pb2.ReportResult())
        report_summaries = get_report_summary_v2_from_report_result(
            report_result, self.edp_combinations)
        expected_report_summary = text_format.Parse(
            expected_custom_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2(),
        )

        self.assertEqual(len(report_summaries), 1)
        self.assertEqual(report_summaries[0], expected_report_summary)

    def test_get_report_summary_v2_from_a_large_sample_report_result(self):
        """Tests that the full report result is parsed into a summary correctly."""

        report_summaries = get_report_summary_v2_from_report_result(
            self.report_result, self.edp_combinations)

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
        expected_reporting_set_result_18_34 = (
            report_summary_v2_pb2.ReportSummaryV2.ReportSummarySetResult())
        expected_reporting_set_result_18_34.impression_filter = 'custom'
        expected_reporting_set_result_18_34.set_operation = 'union'
        expected_reporting_set_result_18_34.data_providers.extend(
            ['edp1', 'edp2', 'edp3'])
        # There are only non-cumulative whole campaign measurements.
        non_cumulative = (
            expected_reporting_set_result_18_34.non_cumulative_results.add())
        non_cumulative.metric_frequency_type = (
            ReportSummaryWindowResult.MetricFrequencyType.TOTAL)
        non_cumulative.reach.value = 19030737
        non_cumulative.reach.standard_deviation = 10000.0
        non_cumulative.reach.metric = (
            'reach_non_cumulative_edp1_edp2_edp3_custom_2025_10_15')
        non_cumulative.impression_count.value = 35936915
        non_cumulative.impression_count.standard_deviation = 10000.0
        non_cumulative.impression_count.metric = (
            'impression_non_cumulative_edp1_edp2_edp3_custom_2025_10_15')
        non_cumulative.frequency.metric = (
            'frequency_non_cumulative_edp1_edp2_edp3_custom_2025_10_15')
        freq_bins = {
            '1': (9822316, 10000.0),
            '2': (4911158, 10000.0),
            '3': (2455579, 10000.0),
            '4': (1227790, 10000.0),
            '5': (613894, 10000.0),
        }
        for k, v in freq_bins.items():
            non_cumulative.frequency.bins[k].value = v[0]
            non_cumulative.frequency.bins[k].standard_deviation = v[1]

        self.assertIn(expected_reporting_set_result_18_34,
                      report_summary_18_34.report_summary_set_results)

        # Verifies that a specific, expected set result is present in the summary.
        expected_reporting_set_result_35_54 = (
            report_summary_v2_pb2.ReportSummaryV2.ReportSummarySetResult())
        expected_reporting_set_result_35_54.impression_filter = 'ami'
        expected_reporting_set_result_35_54.set_operation = 'union'
        expected_reporting_set_result_35_54.data_providers.extend(['edp1'])

        # There are cumulative, non-cumulative whole campaign, and weekly
        # non-cumulative measurements.
        # First cumulative reach.
        cumulative1 = expected_reporting_set_result_35_54.cumulative_results.add(
        )
        cumulative1.metric_frequency_type = (
            ReportSummaryWindowResult.MetricFrequencyType.WEEKLY)
        cumulative1.reach.value = 9992500
        cumulative1.reach.standard_deviation = 10000.0
        cumulative1.reach.metric = 'reach_cumulative_edp1_ami_2025_10_08'

        # First non-cumulative results.
        non_cumulative1 = expected_reporting_set_result_35_54.non_cumulative_results.add(
        )
        non_cumulative1.metric_frequency_type = (
            ReportSummaryWindowResult.MetricFrequencyType.WEEKLY)
        non_cumulative1.reach.value = 10008130
        non_cumulative1.reach.standard_deviation = 10000.0
        non_cumulative1.reach.metric = 'reach_non_cumulative_edp1_ami_2025_10_08'
        non_cumulative1.impression_count.value = 18379493
        non_cumulative1.impression_count.standard_deviation = 10000.0
        non_cumulative1.impression_count.metric = 'impression_non_cumulative_edp1_ami_2025_10_08'
        non_cumulative1.frequency.metric = 'frequency_non_cumulative_edp1_ami_2025_10_08'
        freq_bins1 = {
            '1': (5165486, 10000.0),
            '2': (2582743, 10000.0),
            '3': (1291372, 10000.0),
            '4': (645686, 10000.0),
            '5': (322843, 10000.0),
        }
        for k, v in freq_bins1.items():
            non_cumulative1.frequency.bins[k].value = v[0]
            non_cumulative1.frequency.bins[k].standard_deviation = v[1]

        # Second cumulative reach.
        cumulative2 = expected_reporting_set_result_35_54.cumulative_results.add(
        )
        cumulative2.metric_frequency_type = (
            ReportSummaryWindowResult.MetricFrequencyType.WEEKLY)
        cumulative2.reach.value = 11998422
        cumulative2.reach.standard_deviation = 10000.0
        cumulative2.reach.metric = 'reach_cumulative_edp1_ami_2025_10_15'

        # Second non-cumulative results.
        non_cumulative2 = expected_reporting_set_result_35_54.non_cumulative_results.add(
        )
        non_cumulative2.metric_frequency_type = (
            ReportSummaryWindowResult.MetricFrequencyType.WEEKLY)
        non_cumulative2.reach.value = 2452001
        non_cumulative2.reach.standard_deviation = 10000.0
        non_cumulative2.reach.metric = 'reach_non_cumulative_edp1_ami_2025_10_15'
        non_cumulative2.impression_count.value = 4471035
        non_cumulative2.impression_count.standard_deviation = 10000.0
        non_cumulative2.impression_count.metric = 'impression_non_cumulative_edp1_ami_2025_10_15'
        non_cumulative2.frequency.metric = 'frequency_non_cumulative_edp1_ami_2025_10_15'
        freq_bins2 = {
            '1': (1265549, 10000.0),
            '2': (632775, 10000.0),
            '3': (316388, 10000.0),
            '4': (158194, 10000.0),
            '5': (79095, 10000.0),
        }
        for k, v in freq_bins2.items():
            non_cumulative2.frequency.bins[k].value = v[0]
            non_cumulative2.frequency.bins[k].standard_deviation = v[1]

        self.assertIn(expected_reporting_set_result_35_54,
                      report_summary_35_54.report_summary_set_results)


if __name__ == '__main__':
    unittest.main()
