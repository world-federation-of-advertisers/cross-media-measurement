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

from noiseninja.noised_measurements import Measurement
from report.report import Report, MetricReport
from tools.post_process_origin_report import correctExcelFile, readExcel, \
  processReportSummary

CUML_REACH_COL_NAME = "Cumulative Reach 1+"
TOTAL_REACH_COL_NAME = "Total Reach (1+)"
FILTER_COL_NAME = "Impression Filter"

AMI_FILTER = "AMI"
MRC_FILTER = "MRC"

ami = "ami"
mrc = "mrc"

EDP_MAP = {
    "Google": {"Google"},
    "Linear TV": {"Linear TV"},
    "Total Campaign": {"Google", "Linear TV"},
}


def generateMeasurements(input, sigma, name):
  measurements = []
  for x in input:
    measurements.append(Measurement(x, sigma, name + str(x).zfill(2)))
  return measurements


class TestOriginSheetReport(unittest.TestCase):
  def test_report_summary_is_corrected_successfully(self):
    (measurements, excel) = readExcel(
        "experimental/dp_consistency/src/test/python/wfa/measurement/reporting/postprocess/tools/example_origin_report.xlsx",
        "Linear TV")
    report_summary = report_summary_pb2.ReportSummary()
    for edp in EDP_MAP:
      ami_measurement_detail = report_summary.measurement_details.add()
      ami_measurement_detail.measurement_policy = "ami"
      ami_measurement_detail.set_operation = "cumulative"
      ami_measurement_detail.is_cumulative = True
      ami_measurement_detail.data_providers.extend(EDP_MAP[edp])
      for i in range(len(measurements[edp]["AMI"]) - 1):
        result = measurements[edp]["AMI"][i]
        ami_result = ami_measurement_detail.measurement_results.add()
        ami_result.reach = result.value
        ami_result.standard_deviation = result.sigma
        ami_result.metric = "metric_" + edp + "_ami_" + str(i).zfill(5)

      mrc_measurement_detail = report_summary.measurement_details.add()
      mrc_measurement_detail.measurement_policy = "mrc"
      mrc_measurement_detail.set_operation = "cumulative"
      mrc_measurement_detail.is_cumulative = True
      mrc_measurement_detail.data_providers.extend(EDP_MAP[edp])
      for i in range(len(measurements[edp]["MRC"]) - 1):
        result = measurements[edp]["MRC"][i]
        mrc_result = mrc_measurement_detail.measurement_results.add()
        mrc_result.reach = result.value
        mrc_result.standard_deviation = result.sigma
        mrc_result.metric = "metric_" + edp + "_mrc_" + str(i).zfill(5)

    for edp in EDP_MAP:
      ami_measurement_detail = report_summary.measurement_details.add()
      ami_measurement_detail.measurement_policy = "ami"
      ami_measurement_detail.set_operation = "union"
      ami_measurement_detail.is_cumulative = False
      ami_measurement_detail.data_providers.extend(EDP_MAP[edp])
      ami_measurement = measurements[edp]["AMI"][
        len(measurements[edp]["AMI"]) - 1]
      ami_result = ami_measurement_detail.measurement_results.add()
      ami_result.reach = ami_measurement.value
      ami_result.standard_deviation = ami_measurement.sigma
      ami_result.metric = "metric_" + edp + "_ami_" + str(
          len(measurements[edp]["AMI"]) - 1).zfill(5)

      mrc_measurement_detail = report_summary.measurement_details.add()
      mrc_measurement_detail.measurement_policy = "mrc"
      mrc_measurement_detail.set_operation = "union"
      mrc_measurement_detail.is_cumulative = False
      mrc_measurement_detail.data_providers.extend(EDP_MAP[edp])
      mrc_measurement = measurements[edp]["MRC"][
        len(measurements[edp]["MRC"]) - 1]
      mrc_result = mrc_measurement_detail.measurement_results.add()
      mrc_result.reach = mrc_measurement.value
      mrc_result.standard_deviation = mrc_measurement.sigma
      mrc_result.metric = "metric_" + edp + "_mrc_" + str(
          len(measurements[edp]["MRC"]) - 1).zfill(5)

    processReportSummary(report_summary)

  def test_get_origin_report_corrected_successfully(self):
    correctedExcel = correctExcelFile(
        "experimental/dp_consistency/src/test/python/wfa/measurement/reporting/postprocess/tools/example_origin_report.xlsx",
        "Linear TV"
    )
    (google_ami_rows, google_mrc_rows) = self.get_edp_rows(correctedExcel,
                                                           "Google")
    (tv_ami_rows, tv_mrc_rows) = self.get_edp_rows(correctedExcel, "Linear TV")
    (total_ami_rows, total_mrc_rows) = self.get_edp_rows(
        correctedExcel, "Total Campaign"
    )

    # Ensure that subset relations are correct by checking larger time periods have more reach than smaller ones.
    self.__assert_is_monotonically_increasing(google_ami_rows)
    self.__assert_is_monotonically_increasing(google_mrc_rows)

    self.__assert_is_monotonically_increasing(tv_ami_rows)
    self.__assert_is_monotonically_increasing(tv_mrc_rows)

    self.__assert_is_monotonically_increasing(total_ami_rows)
    self.__assert_is_monotonically_increasing(total_mrc_rows)

    # Ensure that cover relations are correct by checking the sum of EDPs have larger reach than the total reach.
    self.__assert_pairwise_sum_greater(google_ami_rows, tv_ami_rows,
                                       total_ami_rows)
    self.__assert_pairwise_sum_greater(google_mrc_rows, tv_mrc_rows,
                                       total_mrc_rows)

    # Ensure that metric subset relation is correct by checking AMI is always larger than MRC.
    self.__assert_pairwise_greater(google_ami_rows, google_mrc_rows)
    self.__assert_pairwise_greater(tv_ami_rows, tv_mrc_rows)
    self.__assert_pairwise_greater(total_ami_rows, total_mrc_rows)

  def get_edp_rows(self, df, edp):
    tot_sheet = df[edp]
    cum_sheet = df[f"Cuml. Reach ({edp})"]
    ami_rows = (
        cum_sheet[cum_sheet[FILTER_COL_NAME] == AMI_FILTER][
          CUML_REACH_COL_NAME
        ].tolist()
        + tot_sheet[tot_sheet[FILTER_COL_NAME] == AMI_FILTER][
          TOTAL_REACH_COL_NAME
        ].tolist()
    )
    mrc_rows = (
        cum_sheet[cum_sheet[FILTER_COL_NAME] == MRC_FILTER][
          CUML_REACH_COL_NAME
        ].tolist()
        + tot_sheet[tot_sheet[FILTER_COL_NAME] == MRC_FILTER][
          TOTAL_REACH_COL_NAME
        ].tolist()
    )
    return (ami_rows, mrc_rows)

  def __assert_is_monotonically_increasing(self, lst):
    """Checks if a list is monotonically increasing."""
    self.assertTrue(all(lst[i] <= lst[i + 1] for i in range(len(lst) - 1)))

  def __assert_pairwise_sum_greater(self, list1, list2, list3):
    """Checks if the pairwise sum of the first two lists is pairwise greater than the third list."""
    if len(list1) != len(list2) or len(list1) != len(list3):
      raise ValueError("Lists must have the same length")
    self.assertTrue(
        all(list1[i] + list2[i] >= list3[i] for i in range(len(list1))))

  def __assert_pairwise_greater(self, list1, list2):
    """Checks if the first list is pairwise greater than the second list."""
    if len(list1) != len(list2):
      raise ValueError("Lists must have the same length")
    self.assertTrue(all(list1[i] >= list2[i] for i in range(len(list1))))


if __name__ == "__main__":
  unittest.main()
