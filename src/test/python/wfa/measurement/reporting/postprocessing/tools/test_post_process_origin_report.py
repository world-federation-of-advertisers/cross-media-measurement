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

import json
import numpy as np
import os
import statistics
import sys
import unittest

from google.protobuf.json_format import Parse
from src.main.proto.wfa.measurement.reporting.postprocessing.v2alpha import \
  report_summary_pb2
from tools.post_process_origin_report import ReportSummaryProcessor

EDP_MAP = {
    "edp1": {"edp1"},
    "edp2": {"edp2"},
    "union": {"edp1", "edp2"},
}

AMI_MEASUREMENTS = {
    'edp1': [701155, 1387980, 1993909, 2530351, 3004251, 3425139, 3798300,
             4130259, 4425985, 4689161, 4924654, 5134209, 5321144, 5488320,
             5638284, 5772709, 5893108],
    'edp2': [17497550, 26248452, 28434726, 29254557, 29613105, 29781657,
             29863471, 29903985, 29923599, 29933436, 29938318, 29940737,
             29941947, 29942509, 29942840, 29942982, 29943048],
    'union': [17848693, 26596529, 28810116, 29670899, 30076858, 30293844,
              30422560, 30507247, 30567675, 30614303, 30652461, 30684582,
              30712804, 30737507, 30759392, 30778972, 30796521],
}
MRC_MEASUREMENTS = {
    'edp1': [630563, 1248838, 1794204, 2276856, 2703592, 3082468, 3418615,
             3717626, 3983983, 4220849, 4432799, 4621453, 4789932, 4940394,
             5075337, 5196132, 5304490],
    'edp2': [15747807, 23623080, 25590863, 26328935, 26651567, 26803189,
             26876867, 26913336, 26930960, 26939827, 26944204, 26946392,
             26947485, 26947981, 26948285, 26948410, 26948472],
    'union': [16063679, 23936163, 25928613, 26703382, 27068800, 27263915,
              27379780, 27456089, 27510475, 27552474, 27586849, 27615813,
              27641241, 27663446, 27683138, 27700680, 27716450],
}

# DP params:
# a) Direct measurement: (eps, delta) = (0.00643, 1e-15) --> sigma = 1051
# b) MPC measurement:    (eps, delta) = (0.01265, 1e-15) --> sigma =  542
SIGMAS = {
    'edp1': 1051.0,
    'edp2': 1051.0,
    'union': 542.0,
}

def count_equal_values(noisy_measurement_map, corrected_measurement_map):
  count = 0
  for key in noisy_measurement_map:
    if abs(noisy_measurement_map[key] - corrected_measurement_map[
      key]) <= DIFF_THRESHOLD:
      count += 1
  return count


def get_statistics(array, variance_list, bias_list):
  variance_list.append(np.var(array))
  bias_list.append(np.mean(array))


TOLERANCE = 1
DIFF_THRESHOLD = 10


class TestOriginReport(unittest.TestCase):
  def calculate_means_and_variances(self, true_measurements, noisy_measurements,
      corrected_measurements):
    # Number of measurements.
    num_measurements = len(true_measurements)
    noisy_diff = {}
    corrected_diff = {}

    for i in range(num_measurements):
      noisy_diff[i] = []
      corrected_diff[i] = []

    for i in noisy_measurements:
      for k in range(num_measurements):
        noisy_diff[k].append(noisy_measurements[i][k] - true_measurements[k])
        corrected_diff[k].append(
          corrected_measurements[i][k] - true_measurements[k])

    noisy_means = []
    noisy_variances = []
    corrected_means = []
    corrected_variances = []

    for k in range(num_measurements):
      noisy_means.append(statistics.mean(noisy_diff[k]))
      noisy_variances.append(statistics.variance(noisy_diff[k]))
      corrected_means.append(statistics.mean(corrected_diff[k]))
      corrected_variances.append(statistics.variance(corrected_diff[k]))

    print(f"noisy_means: {noisy_means}")
    print(f"noisy_variances: {noisy_variances}")
    print(f"corrected_means: {corrected_means}")
    print(f"corrected_variances: {corrected_variances}")
    for num in [x / y for x, y in zip(noisy_variances, corrected_variances)]:
      if num < 0.8:
        print(f"{num:.2f}")

  def test_variance(self):
    true_report_summary, true_measurement_map, noisy_measurement_map = \
      get_report_summary_from_data(AMI_MEASUREMENTS, MRC_MEASUREMENTS)
    true_measurements = [
        value for key, value in sorted(true_measurement_map.items())
    ]

    true_unique_reach = true_measurement_map['total_metric_union_ami_'] - \
                        true_measurement_map['total_metric_edp2_ami_']

    for key, value in sorted(noisy_measurement_map.items()):
      print(f"{key} -> {value}")

    noisy_results = {}
    corrected_results = {}
    noisy_unique_reach_diffs_1 = []
    corrected_unique_reach_diffs_1 = []
    noisy_unique_reach_diffs_2 = []
    corrected_unique_reach_diffs_2 = []

    diffs = []

    for i in range(0, 100):
      print(f"iteration {i}")
      report_summary, true_measurement_map, noisy_measurement_map = \
        get_report_summary_from_data(AMI_MEASUREMENTS, MRC_MEASUREMENTS)

      sorted_noisy_measurements = [
          value for key, value in sorted(noisy_measurement_map.items())
      ]

      corrected_measurement_map = ReportSummaryProcessor(
          report_summary).process()
      sorted_corrected_measurements = [
          value for key, value in sorted(corrected_measurement_map.items())
      ]
      noisy_results[i] = sorted_noisy_measurements
      corrected_results[i] = sorted_corrected_measurements

      for a, b in zip(sorted_noisy_measurements, sorted_corrected_measurements):
        diffs.append(abs(a - b))

      noisy_unique_reach_diffs_1.append(
          noisy_measurement_map['total_metric_union_ami_'] -
          noisy_measurement_map['total_metric_edp2_ami_'] -
          true_unique_reach
      )

      corrected_unique_reach_diffs_1.append(
          corrected_measurement_map['total_metric_union_ami_'] -
          corrected_measurement_map['total_metric_edp2_ami_'] -
          true_unique_reach
      )

      noisy_unique_reach_diffs_2.append(
          noisy_measurement_map['total_metric_union_ami_'] -
          noisy_measurement_map['total_metric_edp1_ami_'] -
          true_unique_reach
      )

      corrected_unique_reach_diffs_2.append(
          corrected_measurement_map['total_metric_union_ami_'] -
          corrected_measurement_map['total_metric_edp1_ami_'] -
          true_unique_reach
      )

    # print(f"Diffs between noisy and corrected: {diffs}")
    # print(f"Noisy unique reach diff: {noisy_unique_reach_diffs}")
    # print(f"Corrected unique reach diff: {corrected_unique_reach_diffs}")
    print(f"Noisy unique reach mean: {statistics.mean(noisy_unique_reach_diffs_1)}")
    print(f"Noisy unique reach variance: {statistics.variance(noisy_unique_reach_diffs_1)}")
    print(f"Corrected unique reach mean: {statistics.mean(corrected_unique_reach_diffs_1)}")
    print(f"Corrected unique reach variance: {statistics.variance(corrected_unique_reach_diffs_1)}")
    print(f"Noisy unique reach mean: {statistics.mean(noisy_unique_reach_diffs_2)}")
    print(f"Noisy unique reach variance: {statistics.variance(noisy_unique_reach_diffs_2)}")
    print(f"Corrected unique reach mean: {statistics.mean(corrected_unique_reach_diffs_2)}")
    print(f"Corrected unique reach variance: {statistics.variance(corrected_unique_reach_diffs_2)}")

    self.calculate_means_and_variances(true_measurements, noisy_results,
                                       corrected_results)


  # def test_report_summary_is_corrected_successfully(self):
  #   report_summary = report_summary_pb2.ReportSummary()
  #   # Generates report summary from the measurements. For each edp combination,
  #   # all measurements except the last one are cumulative measurements, and the
  #   # last one is the whole campaign measurement.
  #   num_periods = len(AMI_MEASUREMENTS['edp1']) - 1
  #   for edp in EDP_MAP:
  #     ami_measurement_detail = report_summary.measurement_details.add()
  #     ami_measurement_detail.measurement_policy = "ami"
  #     ami_measurement_detail.set_operation = "cumulative"
  #     ami_measurement_detail.is_cumulative = True
  #     ami_measurement_detail.data_providers.extend(EDP_MAP[edp])
  #     for i in range(len(AMI_MEASUREMENTS[edp]) - 1):
  #       ami_result = ami_measurement_detail.measurement_results.add()
  #       ami_result.reach = AMI_MEASUREMENTS[edp][i]
  #       ami_result.standard_deviation = SIGMAS[edp]
  #       ami_result.metric = "cumulative_metric_" + edp + "_ami_" + str(i).zfill(
  #           5)
  #
  #     mrc_measurement_detail = report_summary.measurement_details.add()
  #     mrc_measurement_detail.measurement_policy = "mrc"
  #     mrc_measurement_detail.set_operation = "cumulative"
  #     mrc_measurement_detail.is_cumulative = True
  #     mrc_measurement_detail.data_providers.extend(EDP_MAP[edp])
  #     for i in range(num_periods):
  #       mrc_result = mrc_measurement_detail.measurement_results.add()
  #       mrc_result.reach = MRC_MEASUREMENTS[edp][i]
  #       mrc_result.standard_deviation = SIGMAS[edp]
  #       mrc_result.metric = "cumulative_metric_" + edp + "_mrc_" + str(i).zfill(
  #           5)
  #
  #   for edp in EDP_MAP:
  #     ami_measurement_detail = report_summary.measurement_details.add()
  #     ami_measurement_detail.measurement_policy = "ami"
  #     ami_measurement_detail.set_operation = "union"
  #     ami_measurement_detail.is_cumulative = False
  #     ami_measurement_detail.data_providers.extend(EDP_MAP[edp])
  #     ami_result = ami_measurement_detail.measurement_results.add()
  #     ami_result.reach = AMI_MEASUREMENTS[edp][len(AMI_MEASUREMENTS[edp]) - 1]
  #     ami_result.standard_deviation = SIGMAS[edp]
  #     ami_result.metric = "total_metric_" + edp + "_ami_"
  #
  #     mrc_measurement_detail = report_summary.measurement_details.add()
  #     mrc_measurement_detail.measurement_policy = "mrc"
  #     mrc_measurement_detail.set_operation = "union"
  #     mrc_measurement_detail.is_cumulative = False
  #     mrc_measurement_detail.data_providers.extend(EDP_MAP[edp])
  #     mrc_result = mrc_measurement_detail.measurement_results.add()
  #     mrc_result.reach = MRC_MEASUREMENTS[edp][len(MRC_MEASUREMENTS[edp]) - 1]
  #     mrc_result.standard_deviation = SIGMAS[edp]
  #     mrc_result.metric = "total_metric_" + edp + "_mrc_"
  #
  #   corrected_measurements_map = ReportSummaryProcessor(
  #       report_summary).process()
  #
  #   # Verifies that the updated reach values are consistent.
  #   for edp in EDP_MAP:
  #     cumulative_ami_metric_prefix = "cumulative_metric_" + edp + "_ami_"
  #     cumulative_mrc_metric_prefix = "cumulative_metric_" + edp + "_mrc_"
  #     total_ami_metric = "total_metric_" + edp + "_ami_"
  #     total_mrc_metric = "total_metric_" + edp + "_mrc_"
  #     # Verifies that cumulative measurements are consistent.
  #     for i in range(num_periods - 1):
  #       self.assertLessEqual(
  #           corrected_measurements_map[
  #             cumulative_ami_metric_prefix + str(i).zfill(5)],
  #           corrected_measurements_map[
  #             cumulative_ami_metric_prefix + str(i + 1).zfill(5)])
  #       self.assertLessEqual(
  #           corrected_measurements_map[
  #             cumulative_mrc_metric_prefix + str(i).zfill(5)],
  #           corrected_measurements_map[
  #             cumulative_mrc_metric_prefix + str(i + 1).zfill(5)])
  #     # Verifies that the mrc measurements is less than or equal to the ami ones.
  #     for i in range(num_periods):
  #       self.assertLessEqual(
  #           corrected_measurements_map[
  #             cumulative_mrc_metric_prefix + str(i).zfill(5)],
  #           corrected_measurements_map[
  #             cumulative_ami_metric_prefix + str(i).zfill(5)]
  #       )
  #     # Verifies that the total reach is greater than or equal to the last
  #     # cumulative reach.
  #
  #     self.assertLessEqual(
  #         corrected_measurements_map[
  #           cumulative_ami_metric_prefix + str(num_periods - 1).zfill(5)],
  #         corrected_measurements_map[total_ami_metric]
  #     )
  #     self.assertLessEqual(
  #         corrected_measurements_map[
  #           cumulative_mrc_metric_prefix + str(num_periods - 1).zfill(5)],
  #         corrected_measurements_map[total_mrc_metric]
  #     )
  #
  #   # Verifies that the union reach is less than or equal to the sum of
  #   # individual reaches.
  #   for i in range(num_periods - 1):
  #     self._assertFuzzyLessEqual(
  #         corrected_measurements_map[
  #           "cumulative_metric_union_ami_" + str(i).zfill(5)],
  #         corrected_measurements_map[
  #           "cumulative_metric_edp1_ami_" + str(i).zfill(5)] +
  #         corrected_measurements_map[
  #           "cumulative_metric_edp2_ami_" + str(i).zfill(5)],
  #         TOLERANCE
  #     )
  #     self._assertFuzzyLessEqual(
  #         corrected_measurements_map[
  #           "cumulative_metric_union_mrc_" + str(i).zfill(5)],
  #         corrected_measurements_map[
  #           "cumulative_metric_edp1_mrc_" + str(i).zfill(5)] +
  #         corrected_measurements_map[
  #           "cumulative_metric_edp2_mrc_" + str(i).zfill(5)],
  #         TOLERANCE
  #     )
  #   self._assertFuzzyLessEqual(
  #       corrected_measurements_map["total_metric_union_ami_"],
  #       corrected_measurements_map["total_metric_edp1_ami_"] +
  #       corrected_measurements_map["total_metric_edp2_ami_"],
  #       TOLERANCE
  #   )
  #   self._assertFuzzyLessEqual(
  #       corrected_measurements_map["total_metric_union_mrc_"],
  #       corrected_measurements_map["total_metric_edp1_mrc_"] +
  #       corrected_measurements_map["total_metric_edp2_mrc_"],
  #       TOLERANCE
  #   )
  #
  # def test_report_with_unique_reach_is_parsed_correctly(self):
  #   report_summary = get_report_summary(
  #       "src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_summary_with_unique_reach.json")
  #   reportSummaryProcessor = ReportSummaryProcessor(report_summary)
  #
  #   reportSummaryProcessor._process_primitive_measurements()
  #   reportSummaryProcessor._process_unique_reach_measurements()
  #
  #   expected_unique_reach_map = {
  #       'difference/ami/unique_reach_edp2': ['union/ami/edp1_edp2_edp3',
  #                                            'union/ami/edp1_edp3'],
  #       'difference/ami/unique_reach_edp1': ['union/ami/edp1_edp2_edp3',
  #                                            'union/ami/edp2_edp3'],
  #       'difference/ami/unique_reach_edp3': ['union/ami/edp1_edp2_edp3',
  #                                            'union/ami/edp1_edp2'],
  #   }
  #
  #   self.assertDictEqual(reportSummaryProcessor._set_difference_map,
  #                        expected_unique_reach_map)
  #
  # def test_report_with_unique_reach_is_corrected_successfully(self):
  #   report_summary = get_report_summary(
  #       "src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_summary_with_unique_reach.json")
  #   corrected_measurements_map = ReportSummaryProcessor(
  #       report_summary).process()
  #
  #   # Cumulative measurements are less than or equal to total measurements.
  #   self.assertLessEqual(corrected_measurements_map['cumulative/ami/edp1'],
  #                        corrected_measurements_map['union/ami/edp1'])
  #   self.assertLessEqual(corrected_measurements_map['cumulative/ami/edp2'],
  #                        corrected_measurements_map['union/ami/edp2'])
  #   self.assertLessEqual(corrected_measurements_map['cumulative/ami/edp3'],
  #                        corrected_measurements_map['union/ami/edp3'])
  #
  #   # Subset measurements are less than or equal to superset measurements.
  #   self.assertLessEqual(corrected_measurements_map['union/ami/edp1'],
  #                        corrected_measurements_map['union/ami/edp1_edp2'])
  #   self.assertLessEqual(corrected_measurements_map['union/ami/edp1'],
  #                        corrected_measurements_map['union/ami/edp1_edp3'])
  #   self.assertLessEqual(corrected_measurements_map['union/ami/edp1'],
  #                        corrected_measurements_map['union/ami/edp1_edp2_edp3'])
  #   self.assertLessEqual(corrected_measurements_map['union/ami/edp2'],
  #                        corrected_measurements_map['union/ami/edp1_edp2'])
  #   self.assertLessEqual(corrected_measurements_map['union/ami/edp2'],
  #                        corrected_measurements_map['union/ami/edp2_edp3'])
  #   self.assertLessEqual(corrected_measurements_map['union/ami/edp2'],
  #                        corrected_measurements_map['union/ami/edp1_edp2_edp3'])
  #   self.assertLessEqual(corrected_measurements_map['union/ami/edp3'],
  #                        corrected_measurements_map['union/ami/edp1_edp3'])
  #   self.assertLessEqual(corrected_measurements_map['union/ami/edp3'],
  #                        corrected_measurements_map['union/ami/edp2_edp3'])
  #   self.assertLessEqual(corrected_measurements_map['union/ami/edp3'],
  #                        corrected_measurements_map['union/ami/edp1_edp2_edp3'])
  #   self.assertLessEqual(corrected_measurements_map['union/ami/edp1_edp2'],
  #                        corrected_measurements_map['union/ami/edp1_edp2_edp3'])
  #   self.assertLessEqual(corrected_measurements_map['union/ami/edp1_edp3'],
  #                        corrected_measurements_map['union/ami/edp1_edp2_edp3'])
  #   self.assertLessEqual(corrected_measurements_map['union/ami/edp2_edp3'],
  #                        corrected_measurements_map['union/ami/edp1_edp2_edp3'])
  #
  #   # Checks cover relationships.
  #   self._assertFuzzyLessEqual(
  #       corrected_measurements_map['union/ami/edp1_edp2_edp3'],
  #       corrected_measurements_map['union/ami/edp1'] +
  #       corrected_measurements_map['union/ami/edp2'] +
  #       corrected_measurements_map['union/ami/edp3'], TOLERANCE)
  #   self._assertFuzzyLessEqual(
  #       corrected_measurements_map['union/ami/edp1_edp2_edp3'],
  #       corrected_measurements_map['union/ami/edp1'] +
  #       corrected_measurements_map['union/ami/edp2_edp3'], TOLERANCE)
  #   self._assertFuzzyLessEqual(
  #       corrected_measurements_map['union/ami/edp1_edp2_edp3'],
  #       corrected_measurements_map['union/ami/edp2'] +
  #       corrected_measurements_map['union/ami/edp1_edp3'], TOLERANCE)
  #   self._assertFuzzyLessEqual(
  #       corrected_measurements_map['union/ami/edp1_edp2_edp3'],
  #       corrected_measurements_map['union/ami/edp3'] +
  #       corrected_measurements_map['union/ami/edp1_edp2'], TOLERANCE)
  #   self._assertFuzzyLessEqual(
  #       corrected_measurements_map['union/ami/edp1_edp2'],
  #       corrected_measurements_map['union/ami/edp1'] +
  #       corrected_measurements_map['union/ami/edp2'], TOLERANCE)
  #   self._assertFuzzyLessEqual(
  #       corrected_measurements_map['union/ami/edp1_edp3'],
  #       corrected_measurements_map['union/ami/edp1'] +
  #       corrected_measurements_map['union/ami/edp3'], TOLERANCE)
  #   self._assertFuzzyLessEqual(
  #       corrected_measurements_map['union/ami/edp2_edp3'],
  #       corrected_measurements_map['union/ami/edp2'] +
  #       corrected_measurements_map['union/ami/edp3'], TOLERANCE)
  #
  #   # Checks unique reach measurements.
  #   self._assertFuzzyEqual(
  #       corrected_measurements_map['difference/ami/unique_reach_edp1'],
  #       corrected_measurements_map['union/ami/edp1_edp2_edp3'] -
  #       corrected_measurements_map['union/ami/edp2_edp3'],
  #       TOLERANCE
  #   )
  #   self._assertFuzzyEqual(
  #       corrected_measurements_map['difference/ami/unique_reach_edp2'],
  #       corrected_measurements_map['union/ami/edp1_edp2_edp3'] -
  #       corrected_measurements_map['union/ami/edp1_edp3'],
  #       TOLERANCE
  #   )
  #   self._assertFuzzyEqual(
  #       corrected_measurements_map['difference/ami/unique_reach_edp3'],
  #       corrected_measurements_map['union/ami/edp1_edp2_edp3'] -
  #       corrected_measurements_map['union/ami/edp1_edp2'],
  #       TOLERANCE
  #   )

  def _assertFuzzyEqual(self, x: int, y: int, tolerance: int):
    self.assertLessEqual(abs(x - y), tolerance)

  def _assertFuzzyLessEqual(self, x: int, y: int, tolerance: int):
    self.assertLessEqual(x, y + tolerance)


def read_file_to_string(filename: str) -> str:
  try:
    with open(filename, 'r') as file:
      return file.read()
  except FileNotFoundError:
    sys.exit(1)


def get_report_summary(filename: str):
  input = read_file_to_string(filename)
  report_summary = report_summary_pb2.ReportSummary()
  Parse(input, report_summary)
  return report_summary


def get_report_summary_from_data(ami_measurements, mrc_measurements):
  report_summary = report_summary_pb2.ReportSummary()
  # Generates report summary from the measurements. For each edp combination,
  # all measurements except the last one are cumulative measurements, and the
  # last one is the whole campaign measurement.
  num_periods = len(mrc_measurements['edp1']) - 1
  noisy_measurement_map = {}
  true_measurement_map = {}
  for edp in EDP_MAP:
    ami_measurement_detail = report_summary.measurement_details.add()
    ami_measurement_detail.measurement_policy = "ami"
    ami_measurement_detail.set_operation = "cumulative"
    ami_measurement_detail.is_cumulative = True
    ami_measurement_detail.data_providers.extend(EDP_MAP[edp])

    for i in range(num_periods):
      ami_result = ami_measurement_detail.measurement_results.add()
      ami_result.standard_deviation = SIGMAS[edp]
      ami_result.metric = "cumulative_metric_" + edp + "_ami_" + str(i).zfill(5)

      ami_result.reach = ami_measurements[edp][i] + int(np.random.normal(0, SIGMAS[edp], 1)[0])
      true_measurement_map[ami_result.metric] = ami_measurements[edp][i]
      noisy_measurement_map[ami_result.metric] = ami_result.reach

    mrc_measurement_detail = report_summary.measurement_details.add()
    mrc_measurement_detail.measurement_policy = "mrc"
    mrc_measurement_detail.set_operation = "cumulative"
    mrc_measurement_detail.is_cumulative = True
    mrc_measurement_detail.data_providers.extend(EDP_MAP[edp])
    for i in range(num_periods):
      mrc_result = mrc_measurement_detail.measurement_results.add()
      mrc_result.standard_deviation = SIGMAS[edp]
      mrc_result.metric = "cumulative_metric_" + edp + "_mrc_" + str(i).zfill(5)
      mrc_result.reach = mrc_measurements[edp][i] + int(np.random.normal(0, SIGMAS[edp], 1)[0])
      true_measurement_map[mrc_result.metric] = mrc_measurements[edp][i]
      noisy_measurement_map[mrc_result.metric] = mrc_result.reach

  for edp in EDP_MAP:
    ami_measurement_detail = report_summary.measurement_details.add()
    ami_measurement_detail.measurement_policy = "ami"
    ami_measurement_detail.set_operation = "union"
    ami_measurement_detail.is_cumulative = False
    ami_measurement_detail.data_providers.extend(EDP_MAP[edp])
    ami_result = ami_measurement_detail.measurement_results.add()
    ami_result.standard_deviation = SIGMAS[edp]
    ami_result.metric = "total_metric_" + edp + "_ami_"
    ami_result.reach = ami_measurements[edp][num_periods] + int(np.random.normal(0, SIGMAS[edp], 1)[0])
    true_measurement_map[ami_result.metric] = ami_measurements[edp][num_periods]
    noisy_measurement_map[ami_result.metric] = ami_result.reach

    mrc_measurement_detail = report_summary.measurement_details.add()
    mrc_measurement_detail.measurement_policy = "mrc"
    mrc_measurement_detail.set_operation = "union"
    mrc_measurement_detail.is_cumulative = False
    mrc_measurement_detail.data_providers.extend(EDP_MAP[edp])
    mrc_result = mrc_measurement_detail.measurement_results.add()
    mrc_result.standard_deviation = SIGMAS[edp]
    mrc_result.metric = "total_metric_" + edp + "_mrc_"
    mrc_result.reach = mrc_measurements[edp][num_periods] + int(np.random.normal(0, SIGMAS[edp], 1)[0])
    true_measurement_map[mrc_result.metric] = mrc_measurements[edp][num_periods]
    noisy_measurement_map[mrc_result.metric] = mrc_result.reach

  return report_summary, true_measurement_map, noisy_measurement_map


if __name__ == "__main__":
  unittest.main()
