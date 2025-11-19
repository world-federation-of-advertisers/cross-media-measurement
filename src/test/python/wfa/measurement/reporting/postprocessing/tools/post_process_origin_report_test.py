# Copyright 2024 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import unittest

from google.protobuf.json_format import Parse

from noiseninja.noised_measurements import Measurement

from wfa.measurement.internal.reporting.postprocessing import report_summary_pb2

from tools.post_process_origin_report import ReportSummaryProcessor
from wfa.measurement.internal.reporting.postprocessing import report_post_processor_result_pb2

from google.protobuf import json_format

StatusCode = report_post_processor_result_pb2.ReportPostProcessorStatus.StatusCode
ReportPostProcessorResult = report_post_processor_result_pb2.ReportPostProcessorResult()

TOLERANCE = 1
NOISE_CORRECTION_TOLETANCE = 0.1


class TestOriginReport(unittest.TestCase):
  def test_report_summary_is_parsed_correctly(self):
    report_summary = get_report_summary(
        'src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_summary.json')
    reportSummaryProcessor = ReportSummaryProcessor(report_summary)

    reportSummaryProcessor._process_primitive_measurements()
    reportSummaryProcessor._process_difference_measurements()

    expected_weekly_cumulative_reaches = {
        'custom': {
            frozenset({'edp1', 'edp2'}): [
                Measurement(100, 184302.26,
                            'cumulative/custom/edp1_edp2_00'),
                Measurement(100, 184302.26, 'cumulative/custom/edp1_edp2_01')
            ],
            frozenset({'edp2'}): [
                Measurement(0, 137708.80, 'cumulative/custom/edp2_00'),
                Measurement(0, 137708.80, 'cumulative/custom/edp2_01')
            ],
            frozenset({'edp1'}): [
                Measurement(0, 137708.80, 'cumulative/custom/edp1_00'),
                Measurement(0, 137708.80, 'cumulative/custom/edp1_01')
            ]
        },
        'ami': {
            frozenset({'edp1', 'edp2'}): [
                Measurement(100, 184302.26, 'cumulative/ami/edp1_edp2_00'),
                Measurement(182300, 197680.10, 'cumulative/ami/edp1_edp2_01')
            ],
            frozenset({'edp1'}): [
                Measurement(0, 137708.80, 'cumulative/ami/edp1_00'),
                Measurement(0, 137708.80, 'cumulative/ami/edp1_01')
            ],
            frozenset({'edp2'}): [
                Measurement(168600, 137769.39, 'cumulative/ami/edp2_00'),
                Measurement(101700, 137745.35, 'cumulative/ami/edp2_01')
            ]
        },
        'mrc': {
            frozenset({'edp1', 'edp2'}): [
                Measurement(100, 184302.26, 'cumulative/mrc/edp1_edp2_00'),
                Measurement(100, 184302.26, 'cumulative/mrc/edp1_edp2_01')
            ],
            frozenset({'edp2'}): [
                Measurement(29500, 137719.40, 'cumulative/mrc/edp2_00'),
                Measurement(113200, 137749.48, 'cumulative/mrc/edp2_01')
            ],
            frozenset({'edp1'}): [
                Measurement(234600, 137793.10, 'cumulative/mrc/edp1_00'),
                Measurement(11900, 137713.08, 'cumulative/mrc/edp1_01')
            ]
        }
    }

    expected_whole_campaign_reaches = {
        'custom': {
            frozenset({'edp1', 'edp2'}): Measurement(92459, 145777.47,
                                                     'reach_and_frequency/custom/edp1_edp2'),
            frozenset({'edp2'}): Measurement(0, 102011.28,
                                             'reach_and_frequency/custom/edp2'),
            frozenset({'edp1'}): Measurement(0, 102011.28,
                                             'reach_and_frequency/custom/edp1'),
        },
        'ami': {
            frozenset({'edp1'}): Measurement(0, 102011.28,
                                             'reach_and_frequency/ami/edp1'),
            frozenset({'edp1', 'edp2'}): Measurement(243539, 160194.11,
                                                     'reach_and_frequency/ami/edp1_edp2'),
            frozenset({'edp2'}): Measurement(0, 102011.28,
                                             'reach_and_frequency/ami/edp2')
        },
        'mrc': {
            frozenset({'edp1', 'edp2'}): Measurement(59, 137388.94,
                                                     'reach_and_frequency/mrc/edp1_edp2'),
            frozenset({'edp2'}): Measurement(182759, 102064.11,
                                             'reach_and_frequency/mrc/edp2'),
            frozenset({'edp1'}): Measurement(58679, 102028.24,
                                             'reach_and_frequency/mrc/edp1')
        }
    }

    expected_whole_campaign_k_reaches_measurements = {
        'custom': {
            frozenset({'edp1', 'edp2'}): {
                1: Measurement(0, 49832.83,
                               'reach_and_frequency/custom/edp1_edp2-frequency-1'),
                2: Measurement(52259, 96293.33,
                               'reach_and_frequency/custom/edp1_edp2-frequency-2'),
                3: Measurement(0, 49832.83,
                               'reach_and_frequency/custom/edp1_edp2-frequency-3'),
                4: Measurement(0, 49832.83,
                               'reach_and_frequency/custom/edp1_edp2-frequency-4'),
                5: Measurement(40199, 80625.84,
                               'reach_and_frequency/custom/edp1_edp2-frequency-5'),
            }, frozenset({'edp2'}): {
                1: Measurement(0, 29448.12,
                               'reach_and_frequency/custom/edp2-frequency-1'),
                2: Measurement(0, 29448.12,
                               'reach_and_frequency/custom/edp2-frequency-2'),
                3: Measurement(0, 29448.12,
                               'reach_and_frequency/custom/edp2-frequency-3'),
                4: Measurement(0, 29448.12,
                               'reach_and_frequency/custom/edp2-frequency-4'),
                5: Measurement(0, 29448.12,
                               'reach_and_frequency/custom/edp2-frequency-5'),
            },
            frozenset({'edp1'}): {
                1: Measurement(0, 29448.12,
                               'reach_and_frequency/custom/edp1-frequency-1'),
                2: Measurement(0, 29448.12,
                               'reach_and_frequency/custom/edp1-frequency-2'),
                3: Measurement(0, 29448.12,
                               'reach_and_frequency/custom/edp1-frequency-3'),
                4: Measurement(0, 29448.12,
                               'reach_and_frequency/custom/edp1-frequency-4'),
                5: Measurement(0, 29448.12,
                               'reach_and_frequency/custom/edp1-frequency-5'),
            }
        },
        'ami': {
            frozenset({'edp1'}): {
                1: Measurement(0, 29448.12,
                               'reach_and_frequency/ami/edp1-frequency-1'),
                2: Measurement(0, 29448.12,
                               'reach_and_frequency/ami/edp1-frequency-2'),
                3: Measurement(0, 29448.12,
                               'reach_and_frequency/ami/edp1-frequency-3'),
                4: Measurement(0, 29448.12,
                               'reach_and_frequency/ami/edp1-frequency-4'),
                5: Measurement(0, 29448.12,
                               'reach_and_frequency/ami/edp1-frequency-5'),
            },
            frozenset({'edp1', 'edp2'}): {
                1: Measurement(0, 84149.37,
                               'reach_and_frequency/ami/edp1_edp2-frequency-1'),
                2: Measurement(0, 84149.37,
                               'reach_and_frequency/ami/edp1_edp2-frequency-2'),
                3: Measurement(82755, 100221.13,
                               'reach_and_frequency/ami/edp1_edp2-frequency-3'),
                4: Measurement(111129, 111465.13,
                               'reach_and_frequency/ami/edp1_edp2-frequency-4'),
                5: Measurement(49653, 90265.46,
                               'reach_and_frequency/ami/edp1_edp2-frequency-5'),
            },
            frozenset({'edp2'}): {
                1: Measurement(0, 29448.12,
                               'reach_and_frequency/ami/edp2-frequency-1'),
                2: Measurement(0, 29448.12,
                               'reach_and_frequency/ami/edp2-frequency-2'),
                3: Measurement(0, 29448.12,
                               'reach_and_frequency/ami/edp2-frequency-3'),
                4: Measurement(0, 29448.12,
                               'reach_and_frequency/ami/edp2-frequency-4'),
                5: Measurement(0, 29448.12,
                               'reach_and_frequency/ami/edp2-frequency-5'),
            }},
        'mrc': {
            frozenset({'edp1', 'edp2'}): {
                1: Measurement(18, 58449.20,
                               'reach_and_frequency/mrc/edp1_edp2-frequency-1'),
                2: Measurement(0, 39660.77,
                               'reach_and_frequency/mrc/edp1_edp2-frequency-2'),
                3: Measurement(0, 39660.77,
                               'reach_and_frequency/mrc/edp1_edp2-frequency-3'),
                4: Measurement(40, 102443.67,
                               'reach_and_frequency/mrc/edp1_edp2-frequency-4'),
                5: Measurement(0, 39660.77,
                               'reach_and_frequency/mrc/edp1_edp2-frequency-5'),
            },
            frozenset({'edp2'}): {
                1: Measurement(0, 60427.60,
                               'reach_and_frequency/mrc/edp2-frequency-1'),
                2: Measurement(0, 60427.60,
                               'reach_and_frequency/mrc/edp2-frequency-2'),
                3: Measurement(0, 60427.60,
                               'reach_and_frequency/mrc/edp2-frequency-3'),
                4: Measurement(182759, 118611.04,
                               'reach_and_frequency/mrc/edp2-frequency-4'),
                5: Measurement(0, 60427.60,
                               'reach_and_frequency/mrc/edp2-frequency-5'),
            },
            frozenset({'edp1'}): {
                1: Measurement(37968, 74248.42,
                               'reach_and_frequency/mrc/edp1-frequency-1'),
                2: Measurement(0, 33976.69,
                               'reach_and_frequency/mrc/edp1-frequency-2'),
                3: Measurement(0, 33976.69,
                               'reach_and_frequency/mrc/edp1-frequency-3'),
                4: Measurement(0, 33976.69,
                               'reach_and_frequency/mrc/edp1-frequency-4'),
                5: Measurement(20710, 49508.92,
                               'reach_and_frequency/mrc/edp1-frequency-5'),
            }
        }
    }
    expected_whole_campaign_impressions_measurements = {
        'custom': {
            frozenset({'edp1', 'edp2'}): Measurement(239912, 506550.03,
                                                     'impression_count/custom/edp1_edp2'),
            frozenset({'edp2'}): Measurement(70833, 358181.01,
                                             'impression_count/custom/edp2'),
            frozenset({'edp1'}): Measurement(0, 358175.32,
                                             'impression_count/custom/edp1')
        },
        'ami': {frozenset({'edp1'}): Measurement(0, 358175.32,
                                                 'impression_count/ami/edp1'),
                frozenset({'edp1', 'edp2'}): Measurement(0, 506536.39,
                                                         'impression_count/ami/edp1_edp2'),
                frozenset({'edp2'}): Measurement(0, 358175.32,
                                                 'impression_count/ami/edp2')
                },
        'mrc': {
            frozenset({'edp1', 'edp2'}): Measurement(593825, 506570.14,
                                                     'impression_count/mrc/edp1_edp2'),
            frozenset({'edp2'}): Measurement(0, 358175.32,
                                             'impression_count/mrc/edp2'),
            frozenset({'edp1'}): Measurement(0, 358175.32,
                                             'impression_count/mrc/edp1')
        }}

    expected_unique_reach_map = {
        'difference/custom/edp1_minus_edp2': [
            'reach_and_frequency/custom/edp1_edp2',
            'reach_and_frequency/custom/edp2'
        ],
        'difference/custom/edp2_minus_edp1': [
            'reach_and_frequency/custom/edp1_edp2',
            'reach_and_frequency/custom/edp1'
        ],
        'difference/mrc/edp1_minus_edp2': [
            'reach_and_frequency/mrc/edp1_edp2',
            'reach_and_frequency/mrc/edp2'
        ],
        'difference/mrc/edp2_minus_edp1': [
            'reach_and_frequency/mrc/edp1_edp2',
            'reach_and_frequency/mrc/edp1'
        ],
        'difference/ami/edp1_minus_edp2': [
            'reach_and_frequency/ami/edp1_edp2',
            'reach_and_frequency/ami/edp2'
        ],
        'difference/ami/edp2_minus_edp1': [
            'reach_and_frequency/ami/edp1_edp2',
            'reach_and_frequency/ami/edp1'
        ]
    }

    self.assertDictEqual(reportSummaryProcessor._set_difference_map,
                         expected_unique_reach_map)
    for measurement_policy in ['ami', 'mrc', 'custom']:
      self.assertCountEqual(
          reportSummaryProcessor._weekly_cumulative_reaches[
            measurement_policy].keys(),
          expected_weekly_cumulative_reaches[measurement_policy].keys()
      )

      self.assertCountEqual(
          reportSummaryProcessor._whole_campaign_reaches[
            measurement_policy].keys(),
          expected_whole_campaign_reaches[measurement_policy].keys()
      )

      self.assertCountEqual(
          reportSummaryProcessor._whole_campaign_k_reaches[measurement_policy].keys(),
          expected_whole_campaign_k_reaches_measurements[measurement_policy].keys()
      )

      self.assertCountEqual(
          reportSummaryProcessor._whole_campaign_impressions[measurement_policy].keys(),
          expected_whole_campaign_impressions_measurements[measurement_policy].keys()
      )

      for edp_combination in expected_weekly_cumulative_reaches[
        measurement_policy].keys():
        self._assertMeasurementListsEqual(
            reportSummaryProcessor._weekly_cumulative_reaches[measurement_policy][
              edp_combination],
            expected_weekly_cumulative_reaches[measurement_policy][
              edp_combination],
            TOLERANCE
        )

      for edp_combination in expected_whole_campaign_reaches[
        measurement_policy].keys():
        self._assertMeasurementsEqual(
            reportSummaryProcessor._whole_campaign_reaches[
              measurement_policy][
              edp_combination],
            expected_whole_campaign_reaches[measurement_policy][
              edp_combination],
            TOLERANCE
        )

      for edp_combination in expected_whole_campaign_k_reaches_measurements[
        measurement_policy].keys():
        for frequency in expected_whole_campaign_k_reaches_measurements[measurement_policy][
          edp_combination].keys():
          self._assertMeasurementsEqual(
              reportSummaryProcessor._whole_campaign_k_reaches[measurement_policy][
                edp_combination][frequency],
              expected_whole_campaign_k_reaches_measurements[
                measurement_policy][edp_combination][frequency],
              TOLERANCE
          )

      for edp_combination in expected_whole_campaign_impressions_measurements[
        measurement_policy].keys():
        self._assertMeasurementsEqual(
            reportSummaryProcessor._whole_campaign_impressions[measurement_policy][
              edp_combination],
            expected_whole_campaign_impressions_measurements[measurement_policy][
              edp_combination],
            TOLERANCE
        )

  def test_report_summary_is_corrected_successfully(self):
    report_summary = get_report_summary(
        'src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_summary.json')
    noise_correction_result: ReportPostProcessorResult = ReportSummaryProcessor(
        report_summary).process()

    primitive_edp_combinations = ['edp1', 'edp2', 'edp1_edp2']
    composite_edp_combinations = ['edp1_minus_edp2', 'edp2_minus_edp1']

    num_periods = 2
    num_frequencies = 5

    corrected_measurements_map = noise_correction_result.updated_measurements
    self.assertEqual(noise_correction_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(noise_correction_result.status.primal_equality_residual,
                    NOISE_CORRECTION_TOLETANCE)
    self.assertLess(noise_correction_result.status.primal_inequality_residual,
                    NOISE_CORRECTION_TOLETANCE)
    self.assertFalse(noise_correction_result.uncorrected_measurements)

    # Verifies that the updated reach values are non-negative.
    for value in corrected_measurements_map.values():
      self._assertFuzzyLessEqual(0, value, TOLERANCE)
    # Verifies that cumulative measurements are non-decreasing.
    for i in range(num_periods - 1):
      for edp_combination in primitive_edp_combinations:
        self._assertFuzzyLessEqual(
            corrected_measurements_map[
              'cumulative/ami/' + edp_combination + '_' + str(i).zfill(2)],
            corrected_measurements_map[
              'cumulative/ami/' + edp_combination + '_' + str(i + 1).zfill(2)],
            TOLERANCE
        )
        self._assertFuzzyLessEqual(
            corrected_measurements_map[
              'cumulative/mrc/' + edp_combination + '_' + str(i).zfill(2)],
            corrected_measurements_map[
              'cumulative/mrc/' + edp_combination + '_' + str(i + 1).zfill(2)],
            TOLERANCE
        )
        self._assertFuzzyLessEqual(
            corrected_measurements_map[
              'cumulative/custom/' + edp_combination + '_' + str(i).zfill(2)],
            corrected_measurements_map[
              'cumulative/custom/' + edp_combination + '_' + str(i + 1).zfill(
                  2)],
            TOLERANCE
        )
    # Verifies that the last cumulative measurements are equal to total
    # measurements.
    for edp_combination in primitive_edp_combinations:
      self._assertFuzzyEqual(
          corrected_measurements_map[
            'cumulative/ami/' + edp_combination + '_' + str(
                num_periods - 1).zfill(2)],
          corrected_measurements_map[
            'reach_and_frequency/ami/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyEqual(
          corrected_measurements_map[
            'cumulative/mrc/' + edp_combination + '_' + str(
                num_periods - 1).zfill(2)],
          corrected_measurements_map[
            'reach_and_frequency/mrc/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyEqual(
          corrected_measurements_map[
            'cumulative/custom/' + edp_combination + '_' + str(
                num_periods - 1).zfill(2)],
          corrected_measurements_map[
            'reach_and_frequency/custom/' + edp_combination],
          TOLERANCE
      )

    # Verifies that subset measurements are less than superset measurements
    for i in range(num_periods):
      self._assertFuzzyLessEqual(
          corrected_measurements_map['cumulative/ami/edp1_' + str(i).zfill(2)],
          corrected_measurements_map[
            'cumulative/ami/edp1_edp2_' + str(i).zfill(2)],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map['cumulative/ami/edp2_' + str(i).zfill(2)],
          corrected_measurements_map[
            'cumulative/ami/edp1_edp2_' + str(i).zfill(2)],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map['cumulative/mrc/edp1_' + str(i).zfill(2)],
          corrected_measurements_map[
            'cumulative/mrc/edp1_edp2_' + str(i).zfill(2)],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map['cumulative/mrc/edp2_' + str(i).zfill(2)],
          corrected_measurements_map[
            'cumulative/mrc/edp1_edp2_' + str(i).zfill(2)],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'cumulative/custom/edp1_' + str(i).zfill(2)],
          corrected_measurements_map[
            'cumulative/custom/edp1_edp2_' + str(i).zfill(2)],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'cumulative/custom/edp2_' + str(i).zfill(2)],
          corrected_measurements_map[
            'cumulative/custom/edp1_edp2_' + str(i).zfill(2)],
          TOLERANCE
      )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['reach_and_frequency/ami/edp1'],
        corrected_measurements_map['reach_and_frequency/ami/edp1_edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['reach_and_frequency/ami/edp2'],
        corrected_measurements_map['reach_and_frequency/ami/edp1_edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['reach_and_frequency/mrc/edp1'],
        corrected_measurements_map['reach_and_frequency/mrc/edp1_edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['reach_and_frequency/mrc/edp2'],
        corrected_measurements_map['reach_and_frequency/mrc/edp1_edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['reach_and_frequency/custom/edp1'],
        corrected_measurements_map['reach_and_frequency/custom/edp1_edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['reach_and_frequency/custom/edp2'],
        corrected_measurements_map['reach_and_frequency/custom/edp1_edp2'],
        TOLERANCE
    )

    # Verifies that cover set measurements are less than the sum of child set
    # measurements.
    self._assertFuzzyLessEqual(
        corrected_measurements_map['reach_and_frequency/ami/edp1_edp2'],
        corrected_measurements_map['reach_and_frequency/ami/edp1'] +
        corrected_measurements_map['reach_and_frequency/ami/edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['reach_and_frequency/mrc/edp1_edp2'],
        corrected_measurements_map['reach_and_frequency/mrc/edp1'] +
        corrected_measurements_map['reach_and_frequency/mrc/edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['reach_and_frequency/custom/edp1_edp2'],
        corrected_measurements_map['reach_and_frequency/custom/edp1'] +
        corrected_measurements_map['reach_and_frequency/custom/edp2'],
        TOLERANCE
    )

    # Verifies that total reach is equal to the sum of k_reach.
    for measurement_policy in ['ami', 'mrc', 'custom']:
      for edp_combination in primitive_edp_combinations:
        k_reach_sum = 0.0
        for frequency in range(1, num_frequencies + 1):
          k_reach_sum += corrected_measurements_map[
            'reach_and_frequency/' + measurement_policy + '/' + edp_combination
            + '-frequency-' + str(frequency)]
        self._assertFuzzyEqual(
            corrected_measurements_map[
              'reach_and_frequency/' + measurement_policy + '/' + edp_combination],
            k_reach_sum,
            num_frequencies * TOLERANCE
        )

    # Verifies that the relationship between k_reach and impression holds.
    for measurement_policy in ['ami', 'mrc', 'custom']:
      for edp_combination in primitive_edp_combinations:
        k_reach_sum = 0.0
        for frequency in range(1, num_frequencies + 1):
          k_reach_sum += frequency * corrected_measurements_map[
            'reach_and_frequency/' + measurement_policy + '/' + edp_combination
            + '-frequency-' + str(frequency)]
        self._assertFuzzyLessEqual(
            k_reach_sum,
            corrected_measurements_map[
              'impression_count/' + measurement_policy + '/' + edp_combination],
            TOLERANCE
        )

    # Verifies that the relationship between impression counts holds.
    for measurement_policy in ['ami', 'mrc', 'custom']:
      self._assertFuzzyEqual(
          corrected_measurements_map[
            'impression_count/' + measurement_policy + '/edp1_edp2'],
          corrected_measurements_map[
            'impression_count/' + measurement_policy + '/edp1'] +
          corrected_measurements_map[
            'impression_count/' + measurement_policy + '/edp2'],
          TOLERANCE
      )

    # Verifies that difference measurements are mapped correctly to primitive
    # measurements.
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/ami/edp2_minus_edp1'],
        corrected_measurements_map['reach_and_frequency/ami/edp1_edp2'] -
        corrected_measurements_map['reach_and_frequency/ami/edp1'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/ami/edp1_minus_edp2'],
        corrected_measurements_map['reach_and_frequency/ami/edp1_edp2'] -
        corrected_measurements_map['reach_and_frequency/ami/edp2'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/mrc/edp2_minus_edp1'],
        corrected_measurements_map['reach_and_frequency/mrc/edp1_edp2'] -
        corrected_measurements_map['reach_and_frequency/mrc/edp1'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/mrc/edp1_minus_edp2'],
        corrected_measurements_map['reach_and_frequency/mrc/edp1_edp2'] -
        corrected_measurements_map['reach_and_frequency/mrc/edp2'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/custom/edp2_minus_edp1'],
        corrected_measurements_map['reach_and_frequency/custom/edp1_edp2'] -
        corrected_measurements_map['reach_and_frequency/custom/edp1'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/custom/edp1_minus_edp2'],
        corrected_measurements_map['reach_and_frequency/custom/edp1_edp2'] -
        corrected_measurements_map['reach_and_frequency/custom/edp2'],
        TOLERANCE
    )

    # Verifies that mrc/custom measurements are less than or equal to ami ones.
    for i in range(num_periods):
      for edp_combination in primitive_edp_combinations:
        self._assertFuzzyLessEqual(
            corrected_measurements_map[
              'cumulative/mrc/' + edp_combination + '_' + str(i).zfill(2)],
            corrected_measurements_map[
              'cumulative/ami/' + edp_combination + '_' + str(i).zfill(2)],
            TOLERANCE
        )
        self._assertFuzzyLessEqual(
            corrected_measurements_map[
              'cumulative/custom/' + edp_combination + '_' + str(i).zfill(2)],
            corrected_measurements_map[
              'cumulative/ami/' + edp_combination + '_' + str(i).zfill(2)],
            TOLERANCE
        )
    for edp_combination in primitive_edp_combinations:
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'reach_and_frequency/mrc/' + edp_combination],
          corrected_measurements_map[
            'reach_and_frequency/ami/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'reach_and_frequency/custom/' + edp_combination],
          corrected_measurements_map[
            'reach_and_frequency/ami/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'impression_count/mrc/' + edp_combination],
          corrected_measurements_map[
            'impression_count/ami/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'impression_count/custom/' + edp_combination],
          corrected_measurements_map[
            'impression_count/ami/' + edp_combination],
          TOLERANCE
      )
    for edp_combination in composite_edp_combinations:
      self._assertFuzzyLessEqual(
          corrected_measurements_map['difference/mrc/' + edp_combination],
          corrected_measurements_map['difference/ami/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map['difference/custom/' + edp_combination],
          corrected_measurements_map['difference/ami/' + edp_combination],
          TOLERANCE
      )

  def test_report_summary_without_whole_campaign_reach_is_corrected_successfully(
      self):
    report_summary = get_report_summary(
        'src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_summary_without_whole_campaign_reach.json')
    noise_correction_result: ReportPostProcessorResult = ReportSummaryProcessor(
        report_summary).process()

    primitive_edp_combinations = ['edp1', 'edp2', 'edp1_edp2']
    composite_edp_combinations = ['edp1_minus_edp2', 'edp2_minus_edp1']

    num_periods = 2
    num_frequencies = 5

    corrected_measurements_map = noise_correction_result.updated_measurements
    self.assertEqual(noise_correction_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(noise_correction_result.status.primal_equality_residual,
                    NOISE_CORRECTION_TOLETANCE)
    self.assertLess(noise_correction_result.status.primal_inequality_residual,
                    NOISE_CORRECTION_TOLETANCE)
    self.assertFalse(noise_correction_result.uncorrected_measurements)

    # Verifies that the updated reach values are non-negative.
    for value in corrected_measurements_map.values():
      self._assertFuzzyLessEqual(0, value, TOLERANCE)

    # Verifies that cumulative measurements are non-decreasing.
    for i in range(num_periods - 1):
      for edp_combination in primitive_edp_combinations:
        self._assertFuzzyLessEqual(
            corrected_measurements_map[
              'cumulative/ami/' + edp_combination + '_' + str(i).zfill(2)],
            corrected_measurements_map[
              'cumulative/ami/' + edp_combination + '_' + str(i + 1).zfill(2)],
            TOLERANCE
        )
        self._assertFuzzyLessEqual(
            corrected_measurements_map[
              'cumulative/mrc/' + edp_combination + '_' + str(i).zfill(2)],
            corrected_measurements_map[
              'cumulative/mrc/' + edp_combination + '_' + str(i + 1).zfill(2)],
            TOLERANCE
        )
        self._assertFuzzyLessEqual(
            corrected_measurements_map[
              'cumulative/custom/' + edp_combination + '_' + str(i).zfill(2)],
            corrected_measurements_map[
              'cumulative/custom/' + edp_combination + '_' + str(i + 1).zfill(
                  2)],
            TOLERANCE
        )

    # Verifies that the last cumulative measurements are equal to the
    # generated total measurements.
    for edp_combination in primitive_edp_combinations:
      self._assertFuzzyEqual(
          corrected_measurements_map[
            'cumulative/ami/' + edp_combination + '_' + str(
                num_periods - 1).zfill(2)],
          corrected_measurements_map[
            'derived_reach/ami/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyEqual(
          corrected_measurements_map[
            'cumulative/mrc/' + edp_combination + '_' + str(
                num_periods - 1).zfill(2)],
          corrected_measurements_map[
            'derived_reach/mrc/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyEqual(
          corrected_measurements_map[
            'cumulative/custom/' + edp_combination + '_' + str(
                num_periods - 1).zfill(2)],
          corrected_measurements_map[
            'derived_reach/custom/' + edp_combination],
          TOLERANCE
      )

    # Verifies that subset measurements are less than superset measurements
    for i in range(num_periods):
      self._assertFuzzyLessEqual(
          corrected_measurements_map['cumulative/ami/edp1_' + str(i).zfill(2)],
          corrected_measurements_map[
            'cumulative/ami/edp1_edp2_' + str(i).zfill(2)],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map['cumulative/ami/edp2_' + str(i).zfill(2)],
          corrected_measurements_map[
            'cumulative/ami/edp1_edp2_' + str(i).zfill(2)],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map['cumulative/mrc/edp1_' + str(i).zfill(2)],
          corrected_measurements_map[
            'cumulative/mrc/edp1_edp2_' + str(i).zfill(2)],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map['cumulative/mrc/edp2_' + str(i).zfill(2)],
          corrected_measurements_map[
            'cumulative/mrc/edp1_edp2_' + str(i).zfill(2)],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'cumulative/custom/edp1_' + str(i).zfill(2)],
          corrected_measurements_map[
            'cumulative/custom/edp1_edp2_' + str(i).zfill(2)],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'cumulative/custom/edp2_' + str(i).zfill(2)],
          corrected_measurements_map[
            'cumulative/custom/edp1_edp2_' + str(i).zfill(2)],
          TOLERANCE
      )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/ami/edp1'],
        corrected_measurements_map['derived_reach/ami/edp1_edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/ami/edp2'],
        corrected_measurements_map['derived_reach/ami/edp1_edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/mrc/edp1'],
        corrected_measurements_map['derived_reach/mrc/edp1_edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/mrc/edp2'],
        corrected_measurements_map['derived_reach/mrc/edp1_edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/custom/edp1'],
        corrected_measurements_map['derived_reach/custom/edp1_edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/custom/edp2'],
        corrected_measurements_map['derived_reach/custom/edp1_edp2'],
        TOLERANCE
    )

    # Verifies that cover set measurements are less than the sum of child set
    # measurements.
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/ami/edp1_edp2'],
        corrected_measurements_map['derived_reach/ami/edp1'] +
        corrected_measurements_map['derived_reach/ami/edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/mrc/edp1_edp2'],
        corrected_measurements_map['derived_reach/mrc/edp1'] +
        corrected_measurements_map['derived_reach/mrc/edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/custom/edp1_edp2'],
        corrected_measurements_map['derived_reach/custom/edp1'] +
        corrected_measurements_map['derived_reach/custom/edp2'],
        TOLERANCE
    )

    # Verifies that the relationship between impression counts holds.
    for measurement_policy in ['ami', 'mrc', 'custom']:
      self._assertFuzzyEqual(
          corrected_measurements_map[
            'impression_count/' + measurement_policy + '/edp1_edp2'],
          corrected_measurements_map[
            'impression_count/' + measurement_policy + '/edp1'] +
          corrected_measurements_map[
            'impression_count/' + measurement_policy + '/edp2'],
          TOLERANCE
      )

    # Verifies that difference measurements are mapped correctly to primitive
    # measurements.
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/ami/edp2_minus_edp1'],
        corrected_measurements_map['derived_reach/ami/edp1_edp2'] -
        corrected_measurements_map['derived_reach/ami/edp1'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/ami/edp1_minus_edp2'],
        corrected_measurements_map['derived_reach/ami/edp1_edp2'] -
        corrected_measurements_map['derived_reach/ami/edp2'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/mrc/edp2_minus_edp1'],
        corrected_measurements_map['derived_reach/mrc/edp1_edp2'] -
        corrected_measurements_map['derived_reach/mrc/edp1'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/mrc/edp1_minus_edp2'],
        corrected_measurements_map['derived_reach/mrc/edp1_edp2'] -
        corrected_measurements_map['derived_reach/mrc/edp2'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/custom/edp2_minus_edp1'],
        corrected_measurements_map['derived_reach/custom/edp1_edp2'] -
        corrected_measurements_map['derived_reach/custom/edp1'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/custom/edp1_minus_edp2'],
        corrected_measurements_map['derived_reach/custom/edp1_edp2'] -
        corrected_measurements_map['derived_reach/custom/edp2'],
        TOLERANCE
    )

    # Verifies that mrc/custom measurements are less than or equal to ami ones.
    for i in range(num_periods):
      for edp_combination in primitive_edp_combinations:
        self._assertFuzzyLessEqual(
            corrected_measurements_map[
              'cumulative/mrc/' + edp_combination + '_' + str(i).zfill(2)],
            corrected_measurements_map[
              'cumulative/ami/' + edp_combination + '_' + str(i).zfill(2)],
            TOLERANCE
        )
        self._assertFuzzyLessEqual(
            corrected_measurements_map[
              'cumulative/custom/' + edp_combination + '_' + str(i).zfill(2)],
            corrected_measurements_map[
              'cumulative/ami/' + edp_combination + '_' + str(i).zfill(2)],
            TOLERANCE
        )
    for edp_combination in primitive_edp_combinations:
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'derived_reach/mrc/' + edp_combination],
          corrected_measurements_map[
            'derived_reach/ami/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'derived_reach/custom/' + edp_combination],
          corrected_measurements_map[
            'derived_reach/ami/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'impression_count/mrc/' + edp_combination],
          corrected_measurements_map[
            'impression_count/ami/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'impression_count/custom/' + edp_combination],
          corrected_measurements_map[
            'impression_count/ami/' + edp_combination],
          TOLERANCE
      )
    for edp_combination in composite_edp_combinations:
      self._assertFuzzyLessEqual(
          corrected_measurements_map['difference/mrc/' + edp_combination],
          corrected_measurements_map['difference/ami/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map['difference/custom/' + edp_combination],
          corrected_measurements_map['difference/ami/' + edp_combination],
          TOLERANCE
      )

  def test_report_summary_without_cumulative_reaches_is_corrected_successfully(
      self):
    report_summary = get_report_summary(
        'src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_summary_without_cumulative_reaches.json')
    noise_correction_result: ReportPostProcessorResult = ReportSummaryProcessor(
        report_summary).process()

    primitive_edp_combinations = ['edp1', 'edp2', 'edp1_edp2']
    composite_edp_combinations = ['edp1_minus_edp2', 'edp2_minus_edp1']

    num_periods = 2
    num_frequencies = 5

    corrected_measurements_map = noise_correction_result.updated_measurements
    self.assertEqual(noise_correction_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(noise_correction_result.status.primal_equality_residual,
                    NOISE_CORRECTION_TOLETANCE)
    self.assertLess(noise_correction_result.status.primal_inequality_residual,
                    NOISE_CORRECTION_TOLETANCE)
    self.assertFalse(noise_correction_result.uncorrected_measurements)

    # Verifies that the updated reach values are non-negative.
    for value in corrected_measurements_map.values():
      self._assertFuzzyLessEqual(0, value, TOLERANCE)

    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/ami/edp1'],
        corrected_measurements_map['derived_reach/ami/edp1_edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/ami/edp2'],
        corrected_measurements_map['derived_reach/ami/edp1_edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/mrc/edp1'],
        corrected_measurements_map['derived_reach/mrc/edp1_edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/mrc/edp2'],
        corrected_measurements_map['derived_reach/mrc/edp1_edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/custom/edp1'],
        corrected_measurements_map['derived_reach/custom/edp1_edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/custom/edp2'],
        corrected_measurements_map['derived_reach/custom/edp1_edp2'],
        TOLERANCE
    )

    # Verifies that cover set measurements are less than the sum of child set
    # measurements.
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/ami/edp1_edp2'],
        corrected_measurements_map['derived_reach/ami/edp1'] +
        corrected_measurements_map['derived_reach/ami/edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/mrc/edp1_edp2'],
        corrected_measurements_map['derived_reach/mrc/edp1'] +
        corrected_measurements_map['derived_reach/mrc/edp2'],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map['derived_reach/custom/edp1_edp2'],
        corrected_measurements_map['derived_reach/custom/edp1'] +
        corrected_measurements_map['derived_reach/custom/edp2'],
        TOLERANCE
    )

    # Verifies that the relationship between impression counts holds.
    for measurement_policy in ['ami', 'mrc', 'custom']:
      self._assertFuzzyEqual(
          corrected_measurements_map[
            'impression_count/' + measurement_policy + '/edp1_edp2'],
          corrected_measurements_map[
            'impression_count/' + measurement_policy + '/edp1'] +
          corrected_measurements_map[
            'impression_count/' + measurement_policy + '/edp2'],
          TOLERANCE
      )

    # Verifies that difference measurements are mapped correctly to primitive
    # measurements.
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/ami/edp2_minus_edp1'],
        corrected_measurements_map['derived_reach/ami/edp1_edp2'] -
        corrected_measurements_map['derived_reach/ami/edp1'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/ami/edp1_minus_edp2'],
        corrected_measurements_map['derived_reach/ami/edp1_edp2'] -
        corrected_measurements_map['derived_reach/ami/edp2'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/mrc/edp2_minus_edp1'],
        corrected_measurements_map['derived_reach/mrc/edp1_edp2'] -
        corrected_measurements_map['derived_reach/mrc/edp1'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/mrc/edp1_minus_edp2'],
        corrected_measurements_map['derived_reach/mrc/edp1_edp2'] -
        corrected_measurements_map['derived_reach/mrc/edp2'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/custom/edp2_minus_edp1'],
        corrected_measurements_map['derived_reach/custom/edp1_edp2'] -
        corrected_measurements_map['derived_reach/custom/edp1'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/custom/edp1_minus_edp2'],
        corrected_measurements_map['derived_reach/custom/edp1_edp2'] -
        corrected_measurements_map['derived_reach/custom/edp2'],
        TOLERANCE
    )

    for edp_combination in primitive_edp_combinations:
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'derived_reach/mrc/' + edp_combination],
          corrected_measurements_map[
            'derived_reach/ami/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'derived_reach/custom/' + edp_combination],
          corrected_measurements_map[
            'derived_reach/ami/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'impression_count/mrc/' + edp_combination],
          corrected_measurements_map[
            'impression_count/ami/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            'impression_count/custom/' + edp_combination],
          corrected_measurements_map[
            'impression_count/ami/' + edp_combination],
          TOLERANCE
      )
    for edp_combination in composite_edp_combinations:
      self._assertFuzzyLessEqual(
          corrected_measurements_map['difference/mrc/' + edp_combination],
          corrected_measurements_map['difference/ami/' + edp_combination],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map['difference/custom/' + edp_combination],
          corrected_measurements_map['difference/ami/' + edp_combination],
          TOLERANCE
      )

  def test_report_summary_with_unreferenced_unique_reaches(self):
    report_summary = get_report_summary(
        'src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_summary_with_unreferenced_unique_reaches.json'
    )
    noise_correction_result: ReportPostProcessorResult = ReportSummaryProcessor(
        report_summary).process()

    corrected_measurements_map = noise_correction_result.updated_measurements
    self.assertEqual(noise_correction_result.status.status_code,
                     StatusCode.SOLUTION_FOUND_WITH_HIGHS)
    self.assertLess(noise_correction_result.status.primal_equality_residual,
                    NOISE_CORRECTION_TOLETANCE)
    self.assertLess(noise_correction_result.status.primal_inequality_residual,
                    NOISE_CORRECTION_TOLETANCE)
    self.assertEqual(
        sorted(noise_correction_result.uncorrected_measurements),
        [
            'measurementConsumers/ONYdS_z_3xo/metrics/a29783908-e0ee-4667-9647-6386bdf034b9',
            'measurementConsumers/ONYdS_z_3xo/metrics/a609869b8-fff7-4080-ba46-0c5a28d9efad',
            'measurementConsumers/ONYdS_z_3xo/metrics/a716ef311-3b93-4ff1-8c82-ace82320d9a2',
            'measurementConsumers/ONYdS_z_3xo/metrics/a81890700-9f0e-48bd-8554-055bdfb20873'
        ]
    )

  def _assertFuzzyEqual(self, x: int, y: int, tolerance: int):
    self.assertLessEqual(abs(x - y), tolerance)

  def _assertFuzzyLessEqual(self, x: int, y: int, tolerance: int):
    self.assertLessEqual(x, y + tolerance)

  def _assertMeasurementsEqual(self, measurement1: Measurement,
      measurement2: Measurement, tolerance: float):
    self.assertEqual(measurement1.name, measurement2.name)
    self._assertFuzzyEqual(measurement1.value, measurement2.value, tolerance)
    self._assertFuzzyEqual(measurement1.sigma, measurement2.sigma, tolerance)

  def _assertMeasurementListsEqual(self, list1: list[Measurement],
      list2: list[Measurement], tolerance: float):
    sorted_list1 = sorted(list1, key=lambda measurement: measurement.name)
    sorted_list2 = sorted(list2, key=lambda measurement: measurement.name)
    for i in range(len(sorted_list1)):
      self._assertMeasurementsEqual(sorted_list1[i], sorted_list2[i], tolerance)


def read_file_to_string(filename: str) -> str:
  try:
    with open(filename, 'r') as file:
      return file.read()
  except FileNotFoundError:
    sys.exit(num_periods - 1)


def get_report_summary(filename: str):
  input = read_file_to_string(filename)
  report_summary = report_summary_pb2.ReportSummary()
  Parse(input, report_summary)
  return report_summary


if __name__ == '__main__':
  unittest.main()
