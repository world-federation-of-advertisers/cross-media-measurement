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

SIGMAS = {
    'edp1': 13000.0,
    'edp2': 13000.0,
    'union': 1300.0,
}

TOLERANCE = 1


class TestOriginReport(unittest.TestCase):
  def test_report_summary_is_corrected_successfully(self):
    report_summary = report_summary_pb2.ReportSummary()
    # Generates report summary from the measurements. For each edp combination,
    # all measurements except the last one are cumulative measurements, and the
    # last one is the whole campaign measurement.
    num_periods = len(AMI_MEASUREMENTS['edp1']) - 1
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
      for i in range(num_periods):
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

    corrected_measurements_map = ReportSummaryProcessor(
        report_summary).process()

    # Verifies that the updated reach values are consistent.
    for edp in EDP_MAP:
      cumulative_ami_metric_prefix = "cumulative_metric_" + edp + "_ami_"
      cumulative_mrc_metric_prefix = "cumulative_metric_" + edp + "_mrc_"
      total_ami_metric = "total_metric_" + edp + "_ami_"
      total_mrc_metric = "total_metric_" + edp + "_mrc_"
      # Verifies that cumulative measurements are consistent.
      for i in range(num_periods - 1):
        self.assertLessEqual(
            corrected_measurements_map[
              cumulative_ami_metric_prefix + str(i).zfill(5)],
            corrected_measurements_map[
              cumulative_ami_metric_prefix + str(i + 1).zfill(5)])
        self.assertLessEqual(
            corrected_measurements_map[
              cumulative_mrc_metric_prefix + str(i).zfill(5)],
            corrected_measurements_map[
              cumulative_mrc_metric_prefix + str(i + 1).zfill(5)])
      # Verifies that the mrc measurements is less than or equal to the ami ones.
      for i in range(num_periods):
        self.assertLessEqual(
            corrected_measurements_map[
              cumulative_mrc_metric_prefix + str(i).zfill(5)],
            corrected_measurements_map[
              cumulative_ami_metric_prefix + str(i).zfill(5)]
        )
      # Verifies that the total reach is greater than or equal to the last
      # cumulative reach.
      self.assertLessEqual(
          corrected_measurements_map[
            cumulative_ami_metric_prefix + str(num_periods - 1).zfill(5)],
          corrected_measurements_map[total_ami_metric]
      )
      self.assertLessEqual(
          corrected_measurements_map[
            cumulative_mrc_metric_prefix + str(num_periods - 1).zfill(5)],
          corrected_measurements_map[total_mrc_metric]
      )

    # Verifies that the union reach is less than or equal to the sum of
    # individual reaches.
    for i in range(num_periods - 1):
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            "cumulative_metric_union_ami_" + str(i).zfill(5)],
          corrected_measurements_map[
            "cumulative_metric_edp1_ami_" + str(i).zfill(5)] +
          corrected_measurements_map[
            "cumulative_metric_edp2_ami_" + str(i).zfill(5)],
          TOLERANCE
      )
      self._assertFuzzyLessEqual(
          corrected_measurements_map[
            "cumulative_metric_union_mrc_" + str(i).zfill(5)],
          corrected_measurements_map[
            "cumulative_metric_edp1_mrc_" + str(i).zfill(5)] +
          corrected_measurements_map[
            "cumulative_metric_edp2_mrc_" + str(i).zfill(5)],
          TOLERANCE
      )
    self._assertFuzzyLessEqual(
        corrected_measurements_map["total_metric_union_ami_"],
        corrected_measurements_map["total_metric_edp1_ami_"] +
        corrected_measurements_map["total_metric_edp2_ami_"],
        TOLERANCE
    )
    self._assertFuzzyLessEqual(
        corrected_measurements_map["total_metric_union_mrc_"],
        corrected_measurements_map["total_metric_edp1_mrc_"] +
        corrected_measurements_map["total_metric_edp2_mrc_"],
        TOLERANCE
    )

  def test_report_with_unique_reach_is_parsed_correctly(self):
    report_summary = get_report_summary(
        "src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_summary_with_unique_reach.json")
    reportSummaryProcessor = ReportSummaryProcessor(report_summary)

    reportSummaryProcessor._process_primitive_measurements()
    reportSummaryProcessor._process_unique_reach_measurements()

    expected_unique_reach_map = {
        'difference/ami/unique_reach_edp2': ['union/ami/edp1_edp2_edp3',
                                             'union/ami/edp1_edp3'],
        'difference/ami/unique_reach_edp1': ['union/ami/edp1_edp2_edp3',
                                             'union/ami/edp2_edp3'],
        'difference/ami/unique_reach_edp3': ['union/ami/edp1_edp2_edp3',
                                             'union/ami/edp1_edp2'],
    }

    self.assertDictEqual(reportSummaryProcessor._set_difference_map,
                         expected_unique_reach_map)

  def test_report_with_unique_reach_is_corrected_successfully(self):
    report_summary = get_report_summary(
        "src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_summary_with_unique_reach.json")
    corrected_measurements_map = ReportSummaryProcessor(
        report_summary).process()

    # Cumulative measurements are less than or equal to total measurements.
    self.assertLessEqual(corrected_measurements_map['cumulative/ami/edp1'],
                         corrected_measurements_map['union/ami/edp1'])
    self.assertLessEqual(corrected_measurements_map['cumulative/ami/edp2'],
                         corrected_measurements_map['union/ami/edp2'])
    self.assertLessEqual(corrected_measurements_map['cumulative/ami/edp3'],
                         corrected_measurements_map['union/ami/edp3'])

    # Subset measurements are less than or equal to superset measurements.
    self.assertLessEqual(corrected_measurements_map['union/ami/edp1'],
                         corrected_measurements_map['union/ami/edp1_edp2'])
    self.assertLessEqual(corrected_measurements_map['union/ami/edp1'],
                         corrected_measurements_map['union/ami/edp1_edp3'])
    self.assertLessEqual(corrected_measurements_map['union/ami/edp1'],
                         corrected_measurements_map['union/ami/edp1_edp2_edp3'])
    self.assertLessEqual(corrected_measurements_map['union/ami/edp2'],
                         corrected_measurements_map['union/ami/edp1_edp2'])
    self.assertLessEqual(corrected_measurements_map['union/ami/edp2'],
                         corrected_measurements_map['union/ami/edp2_edp3'])
    self.assertLessEqual(corrected_measurements_map['union/ami/edp2'],
                         corrected_measurements_map['union/ami/edp1_edp2_edp3'])
    self.assertLessEqual(corrected_measurements_map['union/ami/edp3'],
                         corrected_measurements_map['union/ami/edp1_edp3'])
    self.assertLessEqual(corrected_measurements_map['union/ami/edp3'],
                         corrected_measurements_map['union/ami/edp2_edp3'])
    self.assertLessEqual(corrected_measurements_map['union/ami/edp3'],
                         corrected_measurements_map['union/ami/edp1_edp2_edp3'])
    self.assertLessEqual(corrected_measurements_map['union/ami/edp1_edp2'],
                         corrected_measurements_map['union/ami/edp1_edp2_edp3'])
    self.assertLessEqual(corrected_measurements_map['union/ami/edp1_edp3'],
                         corrected_measurements_map['union/ami/edp1_edp2_edp3'])
    self.assertLessEqual(corrected_measurements_map['union/ami/edp2_edp3'],
                         corrected_measurements_map['union/ami/edp1_edp2_edp3'])

    # Checks cover relationships.
    self._assertFuzzyLessEqual(
        corrected_measurements_map['union/ami/edp1_edp2_edp3'],
        corrected_measurements_map['union/ami/edp1'] +
        corrected_measurements_map['union/ami/edp2'] +
        corrected_measurements_map['union/ami/edp3'], TOLERANCE)
    self._assertFuzzyLessEqual(
        corrected_measurements_map['union/ami/edp1_edp2_edp3'],
        corrected_measurements_map['union/ami/edp1'] +
        corrected_measurements_map['union/ami/edp2_edp3'], TOLERANCE)
    self._assertFuzzyLessEqual(
        corrected_measurements_map['union/ami/edp1_edp2_edp3'],
        corrected_measurements_map['union/ami/edp2'] +
        corrected_measurements_map['union/ami/edp1_edp3'], TOLERANCE)
    self._assertFuzzyLessEqual(
        corrected_measurements_map['union/ami/edp1_edp2_edp3'],
        corrected_measurements_map['union/ami/edp3'] +
        corrected_measurements_map['union/ami/edp1_edp2'], TOLERANCE)
    self._assertFuzzyLessEqual(
        corrected_measurements_map['union/ami/edp1_edp2'],
        corrected_measurements_map['union/ami/edp1'] +
        corrected_measurements_map['union/ami/edp2'], TOLERANCE)
    self._assertFuzzyLessEqual(
        corrected_measurements_map['union/ami/edp1_edp3'],
        corrected_measurements_map['union/ami/edp1'] +
        corrected_measurements_map['union/ami/edp3'], TOLERANCE)
    self._assertFuzzyLessEqual(
        corrected_measurements_map['union/ami/edp2_edp3'],
        corrected_measurements_map['union/ami/edp2'] +
        corrected_measurements_map['union/ami/edp3'], TOLERANCE)

    # Checks unique reach measurements.
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/ami/unique_reach_edp1'],
        corrected_measurements_map['union/ami/edp1_edp2_edp3'] -
        corrected_measurements_map['union/ami/edp2_edp3'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/ami/unique_reach_edp2'],
        corrected_measurements_map['union/ami/edp1_edp2_edp3'] -
        corrected_measurements_map['union/ami/edp1_edp3'],
        TOLERANCE
    )
    self._assertFuzzyEqual(
        corrected_measurements_map['difference/ami/unique_reach_edp3'],
        corrected_measurements_map['union/ami/edp1_edp2_edp3'] -
        corrected_measurements_map['union/ami/edp1_edp2'],
        TOLERANCE
    )

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


if __name__ == "__main__":
  unittest.main()
