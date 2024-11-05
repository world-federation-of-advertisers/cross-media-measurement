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

import numpy as np
import unittest

from src.main.proto.wfa.measurement.reporting.postprocessing.v2alpha import \
  report_summary_pb2
from tools.post_process_origin_report import processReportSummary

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

STDS = {
    'edp1': 13000.0,
    'edp2': 13000.0,
    'union': 1300.0,
}

SIGMAS = {
    'edp1': 1.0,
    'edp2': 1.0,
    'union': 0.1,
}


def get_report_summary_from_data(ami_measurements, mrc_measurements, has_noise):
  report_summary = report_summary_pb2.ReportSummary()
  measurement_map = {}
  # Generates report summary from the measurements
  for edp in EDP_MAP:
    ami_measurement_detail = report_summary.measurement_details.add()
    ami_measurement_detail.measurement_policy = "ami"
    ami_measurement_detail.set_operation = "cumulative"
    ami_measurement_detail.is_cumulative = True
    ami_measurement_detail.data_providers.extend(EDP_MAP[edp])
    for i in range(len(ami_measurements[edp])):
      ami_result = ami_measurement_detail.measurement_results.add()
      ami_result.reach = ami_measurements[edp][i]
      ami_result.standard_deviation = 0
      if has_noise:
        ami_result.reach += int(np.random.normal(0, STDS[edp], 1)[0])
        ami_result.standard_deviation = SIGMAS[edp]
      ami_result.metric = "cumulative_metric_" + edp + "_ami_" + str(i).zfill(
          5)
      measurement_map[ami_result.metric] = ami_result.reach

    mrc_measurement_detail = report_summary.measurement_details.add()
    mrc_measurement_detail.measurement_policy = "mrc"
    mrc_measurement_detail.set_operation = "cumulative"
    mrc_measurement_detail.is_cumulative = True
    mrc_measurement_detail.data_providers.extend(EDP_MAP[edp])
    for i in range(len(mrc_measurements[edp])):
      mrc_result = mrc_measurement_detail.measurement_results.add()
      mrc_result.reach = mrc_measurements[edp][i]
      mrc_result.standard_deviation = 0
      if has_noise:
        mrc_result.reach += int(np.random.normal(0, STDS[edp], 1)[0])
        mrc_result.standard_deviation = SIGMAS[edp]
      mrc_result.metric = "cumulative_metric_" + edp + "_mrc_" + str(i).zfill(
          5)
      measurement_map[mrc_result.metric] = mrc_result.reach
  return report_summary, measurement_map


def get_statistics(array, variance_list, bias_list):
  variance_list.append(np.var(array))
  bias_list.append(np.mean(array))

class
class TestOriginReport(unittest.TestCase):
  def test_variance(self):
    true_report_summary, true_measurement_map = get_report_summary_from_data(
        AMI_MEASUREMENTS, MRC_MEASUREMENTS, 0)
    sorted_ground_truth = np.array(
        list(dict(sorted(true_measurement_map.items())).values()))

    edp1_gt = sorted_ground_truth[0:17]
    edp2_gt = sorted_ground_truth[17:34]
    union_gt = sorted_ground_truth[34:51]
    unique1_gt = union_gt - edp2_gt
    unique2_gt = union_gt - edp1_gt

    noisy_total_variances = []
    noisy_total_bias = []
    noisy_unique_reach_1_variances = []
    noisy_unique_reach_2_variances = []
    noisy_unique_reach_1_bias = []
    noisy_unique_reach_2_bias = []
    corrected_total_variances = []
    corrected_total_bias = []
    corrected_unique_reach_1_variances = []
    corrected_unique_reach_2_variances = []
    corrected_unique_reach_1_bias = []
    corrected_unique_reach_2_bias = []

    for i in range(0, 50):
      report_summary, noisy_measurement_map = get_report_summary_from_data(
          AMI_MEASUREMENTS,
          MRC_MEASUREMENTS, 1)

      corrected_measurements = np.array(list(dict(
          sorted(processReportSummary(report_summary).items())).values()))

      noisy_measurement_values = np.array(list(dict(
          sorted(noisy_measurement_map.items())).values()))

      corrected_edp1 = corrected_measurements[0:17]
      corrected_edp2 = corrected_measurements[17:34]
      corrected_union = corrected_measurements[34:51]
      corrected_unique1 = corrected_union - corrected_edp2
      corrected_unique2 = corrected_union - corrected_edp1

      get_statistics(corrected_measurements - sorted_ground_truth,
                     corrected_total_variances, corrected_total_bias)
      get_statistics(corrected_unique1 - unique1_gt,
                     corrected_unique_reach_1_variances,
                     corrected_unique_reach_1_bias)
      get_statistics(corrected_unique2 - unique2_gt,
                     corrected_unique_reach_2_variances,
                     corrected_unique_reach_2_bias)

      noisy_edp1 = noisy_measurement_values[0:17]
      noisy_edp2 = noisy_measurement_values[17:34]
      noisy_union = noisy_measurement_values[34:51]
      noisy_unique1 = noisy_union - noisy_edp2
      noisy_unique2 = noisy_union - noisy_edp1

      get_statistics(noisy_measurement_values - sorted_ground_truth,
                     noisy_total_variances, noisy_total_bias)
      get_statistics(noisy_unique1 - unique1_gt, noisy_unique_reach_1_variances,
                     noisy_unique_reach_1_bias)
      get_statistics(noisy_unique2 - unique2_gt, noisy_unique_reach_2_variances,
                     noisy_unique_reach_2_bias)

    print(', '.join(str(x) for x in np.array(noisy_total_variances)))
    print(', '.join(str(x) for x in np.array(corrected_total_variances)))

    print(', '.join(str(x) for x in np.array(noisy_unique_reach_1_variances)))
    print(
        ', '.join(str(x) for x in np.array(corrected_unique_reach_1_variances)))

    print(', '.join(str(x) for x in np.array(noisy_unique_reach_2_variances)))
    print(
        ', '.join(str(x) for x in np.array(corrected_unique_reach_2_variances)))

    print(', '.join(str(x) for x in np.array(noisy_total_bias)))
    print(', '.join(str(x) for x in np.array(corrected_total_bias)))

    print(', '.join(str(x) for x in np.array(noisy_unique_reach_1_bias)))
    print(', '.join(str(x) for x in np.array(corrected_unique_reach_1_bias)))

    print(', '.join(str(x) for x in np.array(noisy_unique_reach_2_bias)))
    print(', '.join(str(x) for x in np.array(corrected_unique_reach_2_bias)))

  def test_report_summary_is_corrected_successfully(self):
    report_summary = report_summary_pb2.ReportSummary()
    # Generates report summary from the measurements
    for edp in EDP_MAP:
      ami_measurement_detail = report_summary.measurement_details.add()
      ami_measurement_detail.measurement_policy = "ami"
      ami_measurement_detail.set_operation = "cumulative"
      ami_measurement_detail.is_cumulative = True
      ami_measurement_detail.data_providers.extend(EDP_MAP[edp])
      for i in range(len(AMI_MEASUREMENTS[edp]) - 1):
        ami_result = ami_measurement_detail.measurement_results.add()
        ami_result.reach = AMI_MEASUREMENTS[edp][i]
        ami_result.standard_deviation = SIGMAS[edp]
        ami_result.metric = "cumulative_metric_" + edp + "_ami_" + str(i).zfill(
            5)

      mrc_measurement_detail = report_summary.measurement_details.add()
      mrc_measurement_detail.measurement_policy = "mrc"
      mrc_measurement_detail.set_operation = "cumulative"
      mrc_measurement_detail.is_cumulative = True
      mrc_measurement_detail.data_providers.extend(EDP_MAP[edp])
      for i in range(len(MRC_MEASUREMENTS[edp]) - 1):
        mrc_result = mrc_measurement_detail.measurement_results.add()
        mrc_result.reach = MRC_MEASUREMENTS[edp][i]
        mrc_result.standard_deviation = SIGMAS[edp]
        mrc_result.metric = "cumulative_metric_" + edp + "_mrc_" + str(i).zfill(
            5)

    for edp in EDP_MAP:
      ami_measurement_detail = report_summary.measurement_details.add()
      ami_measurement_detail.measurement_policy = "ami"
      ami_measurement_detail.set_operation = "union"
      ami_measurement_detail.is_cumulative = False
      ami_measurement_detail.data_providers.extend(EDP_MAP[edp])
      ami_result = ami_measurement_detail.measurement_results.add()
      ami_result.reach = AMI_MEASUREMENTS[edp][len(AMI_MEASUREMENTS[edp]) - 1]
      ami_result.standard_deviation = SIGMAS[edp]
      ami_result.metric = "total_metric_" + edp + "_ami_"

      mrc_measurement_detail = report_summary.measurement_details.add()
      mrc_measurement_detail.measurement_policy = "mrc"
      mrc_measurement_detail.set_operation = "union"
      mrc_measurement_detail.is_cumulative = False
      mrc_measurement_detail.data_providers.extend(EDP_MAP[edp])
      mrc_result = mrc_measurement_detail.measurement_results.add()
      mrc_result.reach = MRC_MEASUREMENTS[edp][len(MRC_MEASUREMENTS[edp]) - 1]
      mrc_result.standard_deviation = SIGMAS[edp]
      mrc_result.metric = "total_metric_" + edp + "_mrc_"

    corrected_measurements_map = processReportSummary(report_summary)

    # Verifies that the updated reach values are consistent.
    for edp in EDP_MAP:
      cumulative_ami_metric_prefix = "cumulative_metric_" + edp + "_ami_"
      cumulative_mrc_metric_prefix = "cumulative_metric_" + edp + "_mrc_"
      total_ami_metric = "total_metric_" + edp + "_ami_"
      total_mrc_metric = "total_metric_" + edp + "_mrc_"
      # Verifies that cumulative measurements are consistent.
      for i in range(len(AMI_MEASUREMENTS) - 2):
        self.assertTrue(
            corrected_measurements_map[
              cumulative_ami_metric_prefix + str(i).zfill(5)] <=
            corrected_measurements_map[
              cumulative_ami_metric_prefix + str(i + 1).zfill(5)])
        self.assertTrue(
            corrected_measurements_map[
              cumulative_mrc_metric_prefix + str(i).zfill(5)] <=
            corrected_measurements_map[
              cumulative_mrc_metric_prefix + str(i + 1).zfill(5)])
      # Verifies that the mrc measurements is less than or equal to the ami ones.
      for i in range(len(AMI_MEASUREMENTS) - 1):
        self.assertTrue(
            corrected_measurements_map[
              cumulative_mrc_metric_prefix + str(i).zfill(5)] <=
            corrected_measurements_map[
              cumulative_ami_metric_prefix + str(i).zfill(5)]
        )
      # Verifies that the total reach is greater than or equal to the last
      # cumulative reach.
      index = len(AMI_MEASUREMENTS) - 1
      self.assertTrue(
          corrected_measurements_map[
            cumulative_ami_metric_prefix + str(index).zfill(5)] <=
          corrected_measurements_map[total_ami_metric]
      )
      self.assertTrue(
          corrected_measurements_map[
            cumulative_mrc_metric_prefix + str(index).zfill(5)] <=
          corrected_measurements_map[total_mrc_metric]
      )

    # Verifies that the union reach is less than or equal to the sum of
    # individual reaches.
    for i in range(len(AMI_MEASUREMENTS) - 1):
      self.assertTrue(
          corrected_measurements_map[
            "cumulative_metric_union_ami_" + str(i).zfill(5)] <=
          corrected_measurements_map[
            "cumulative_metric_edp1_ami_" + str(i).zfill(5)] +
          corrected_measurements_map[
            "cumulative_metric_edp2_ami_" + str(i).zfill(5)]
      )
      self.assertTrue(
          corrected_measurements_map[
            "cumulative_metric_union_mrc_" + str(i).zfill(5)] <=
          corrected_measurements_map[
            "cumulative_metric_edp1_mrc_" + str(i).zfill(5)] +
          corrected_measurements_map[
            "cumulative_metric_edp2_mrc_" + str(i).zfill(5)]
      )
    self.assertTrue(
        corrected_measurements_map["total_metric_union_ami_"] <=
        corrected_measurements_map["total_metric_edp1_ami_"] +
        corrected_measurements_map["total_metric_edp2_ami_"]
    )
    self.assertTrue(
        corrected_measurements_map["total_metric_union_mrc_"] <=
        corrected_measurements_map["total_metric_edp1_mrc_"] +
        corrected_measurements_map["total_metric_edp2_mrc_"]
    )


if __name__ == "__main__":
  unittest.main()
