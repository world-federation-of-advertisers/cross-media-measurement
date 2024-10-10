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

from experimental.dp_consistency.src.main.proto.wfa.measurement.reporting.postprocess import \
  report_summary_pb2
from tools.post_process_origin_report import correctExcelFile, readExcel, \
  processReportSummary

EDP_MAP = {
    "edp1": {"edp1"},
    "edp2": {"edp2"},
    "union": {"edp1", "edp2"},
}

AMI_MEASUREMENTS = {
    'edp1': [6333, 3585, 7511, 1037, 0, 10040, 0, 2503, 7907, 0, 0, 0, 0, 1729,
             0, 1322, 0],
    'edp2': [24062000, 29281000, 31569000, 31569000, 31569000, 31569000,
             31569000, 31569000, 31569000, 31569000, 31569000, 31569000,
             31569000, 31569000, 31569000, 31569000, 31569000],
    'union': [24129432, 29152165, 31474050, 31352346, 31685183, 31425302,
              31655739, 31643458, 31438532, 31600739, 31386917, 31785206,
              31627169, 31453865, 31582783, 31806702, 31477620],
}
MRC_MEASUREMENTS = {
    'edp1': [0, 2196, 2014, 0, 129, 0, 2018, 81, 0, 0, 288, 0, 0, 0, 0, 0, 0],
    'edp2': [24062000, 29281000, 31569000, 31569000, 31569000, 31569000,
             31569000, 31569000, 31569000, 31569000, 31569000, 31569000,
             31569000, 31569000, 31569000, 31569000, 31569000],
    'union': [24299684, 29107595, 31680517, 31513613, 32127776, 31517198,
              31786057, 31225783, 31237872, 31901620, 31720183, 31263524,
              31775635, 31917650, 31478465, 31784354, 31542065],
}

class TestOriginReport(unittest.TestCase):
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
        ami_result.standard_deviation = 1.0
        ami_result.metric = "metric_" + edp + "_ami_" + str(i).zfill(5)

      mrc_measurement_detail = report_summary.measurement_details.add()
      mrc_measurement_detail.measurement_policy = "mrc"
      mrc_measurement_detail.set_operation = "cumulative"
      mrc_measurement_detail.is_cumulative = True
      mrc_measurement_detail.data_providers.extend(EDP_MAP[edp])
      for i in range(len(MRC_MEASUREMENTS[edp]) - 1):
        mrc_result = mrc_measurement_detail.measurement_results.add()
        mrc_result.reach = MRC_MEASUREMENTS[edp][i]
        mrc_result.standard_deviation = 1.0
        mrc_result.metric = "metric_" + edp + "_mrc_" + str(i).zfill(5)

    for edp in EDP_MAP:
      ami_measurement_detail = report_summary.measurement_details.add()
      ami_measurement_detail.measurement_policy = "ami"
      ami_measurement_detail.set_operation = "union"
      ami_measurement_detail.is_cumulative = False
      ami_measurement_detail.data_providers.extend(EDP_MAP[edp])
      ami_result = ami_measurement_detail.measurement_results.add()
      ami_result.reach = AMI_MEASUREMENTS[edp][len(AMI_MEASUREMENTS[edp]) - 1]
      ami_result.standard_deviation = 1.0
      ami_result.metric = "metric_" + edp + "_ami_" + str(
          len(AMI_MEASUREMENTS[edp]) - 1).zfill(5)

      mrc_measurement_detail = report_summary.measurement_details.add()
      mrc_measurement_detail.measurement_policy = "mrc"
      mrc_measurement_detail.set_operation = "union"
      mrc_measurement_detail.is_cumulative = False
      mrc_measurement_detail.data_providers.extend(EDP_MAP[edp])
      mrc_result = mrc_measurement_detail.measurement_results.add()
      mrc_result.reach = MRC_MEASUREMENTS[edp][len(MRC_MEASUREMENTS[edp]) - 1]
      mrc_result.standard_deviation = 1.0
      mrc_result.metric = "metric_" + edp + "_mrc_" + str(
          len(MRC_MEASUREMENTS[edp]) - 1).zfill(5)

    corrected_measurements_map = processReportSummary(report_summary)
    # Verifies that the updated reach values are consistent.
    for edp in EDP_MAP:
      ami_metric_prefix = "metric_" + edp + "_ami_"
      mrc_metric_prefix = "metric_" + edp + "_mrc_"
      # Verifies that cumulative measurements are consistent.
      for i in range(len(AMI_MEASUREMENTS) - 1):
        self.assertTrue(
            corrected_measurements_map[ami_metric_prefix + str(i).zfill(5)] <=
            corrected_measurements_map[ami_metric_prefix + str(i + 1).zfill(5)])
        self.assertTrue(
            corrected_measurements_map[mrc_metric_prefix + str(i).zfill(5)] <=
            corrected_measurements_map[mrc_metric_prefix + str(i + 1).zfill(5)])
      # Verifies that the mrc measurements is less than or equal to the ami ones.
      for i in range(len(AMI_MEASUREMENTS)):
        self.assertTrue(
            corrected_measurements_map[mrc_metric_prefix + str(i).zfill(5)] <=
            corrected_measurements_map[ami_metric_prefix + str(i).zfill(5)]
        )

    # Verifies that the union reach is less than the sum of individual reaches.
    for i in range(len(AMI_MEASUREMENTS) - 1):
      self.assertTrue(
          corrected_measurements_map["metric_union_ami_" + str(i).zfill(5)] <=
          corrected_measurements_map["metric_edp1_ami_" + str(i).zfill(5)] +
          corrected_measurements_map["metric_edp2_ami_" + str(i).zfill(5)]
      )
      self.assertTrue(
          corrected_measurements_map["metric_union_mrc_" + str(i).zfill(5)] <=
          corrected_measurements_map["metric_edp1_mrc_" + str(i).zfill(5)] +
          corrected_measurements_map["metric_edp2_mrc_" + str(i).zfill(5)]
      )

if __name__ == "__main__":
  unittest.main()
