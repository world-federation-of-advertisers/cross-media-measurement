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

import base64
import json
import math
import pandas as pd
import sys

from experimental.dp_consistency.src.main.proto.wfa.measurement.reporting.postprocess import \
  report_summary_pb2
from functools import partial
from noiseninja.noised_measurements import Measurement
from report.report import Report, MetricReport

# This is a demo script that has the following assumptions :
#   1. There are 2 EDPs one with Name Google, the other Linear TV.
#   2. CUSTOM filters are not yet supported in this tool.
#   3. AMI is a parent of MRC and there are no other relationships between metrics.
#   4. The standard deviation for all Measurements are assumed to be 1
#   5. Frequency results are not corrected.
#   6. Impression results are not corrected.

SIGMA = 1

AMI_FILTER = "AMI"
MRC_FILTER = "MRC"

# TODO(uakyol) : Read the EDP names dynamically from the excel sheet
# TODO(uakyol) : Make this work for 3 EDPs
EDP_ONE = "Google"
EDP_TWO = "Linear TV"
TOTAL_CAMPAIGN = "Total Campaign"

edp_names = [EDP_ONE, EDP_TWO]

CUML_REACH_PREFIX = "Cuml. Reach"

EDP_MAP = {
    edp_name: {"sheet": f"{CUML_REACH_PREFIX} ({edp_name})", "ind": ind}
    for ind, edp_name in enumerate(edp_names + [TOTAL_CAMPAIGN])
}

CUML_REACH_COL_NAME = "Cumulative Reach 1+"
TOTAL_REACH_COL_NAME = "Total Reach (1+)"
FILTER_COL_NAME = "Impression Filter"

ami = "ami"
mrc = "mrc"


def createMeasurements(rows, reach_col_name, sigma, metric=""):
  # These rows are already sorted by timestamp.
  return [
      Measurement(measured_value, sigma, metric)
      for measured_value in list(rows[reach_col_name])
  ]


def getMeasurements(df, reach_col_name, sigma):
  ami_rows = df[df[FILTER_COL_NAME] == AMI_FILTER]
  mrc_rows = df[df[FILTER_COL_NAME] == MRC_FILTER]

  ami_measurements = createMeasurements(ami_rows, reach_col_name, sigma)
  mrc_measurements = createMeasurements(mrc_rows, reach_col_name, sigma)

  return (ami_measurements, mrc_measurements)


def readExcel(excel_file_path, unnoised_edps):
  measurements = {}
  dfs = pd.read_excel(excel_file_path, sheet_name=None)
  for edp in EDP_MAP:
    sigma = 0 if edp in unnoised_edps else SIGMA

    cumilative_sheet_name = EDP_MAP[edp]["sheet"]
    (
        cumilative_ami_measurements,
        cumilative_mrc_measurements) = getMeasurements(
        dfs[cumilative_sheet_name], CUML_REACH_COL_NAME, sigma
    )

    (total_ami_measurements, total_mrc_measurements) = getMeasurements(
        dfs[edp], TOTAL_REACH_COL_NAME, sigma
    )

    # There has to be 1 row for AMI and MRC metrics in the total reach sheet.
    assert len(total_mrc_measurements) == 1 and len(total_ami_measurements) == 1

    measurements[edp] = {
        AMI_FILTER: cumilative_ami_measurements + total_ami_measurements,
        MRC_FILTER: cumilative_mrc_measurements + total_mrc_measurements,
    }
  return (measurements, dfs)


# Processes a report summary and returns a consistent one.
#
# Currently, the function only supports ami and mrc measurements and primitive
# set operations (cumulative and union).
# TODO(@ple13): Extend the function to support custom measurements and composite
#  set operations such as difference, incremental.
def processReportSummary(report_summary: report_summary_pb2.ReportSummary()):
  ami_measurements: Dict[FrozenSet[str], List[Measurement]] = {}
  mrc_measurements: Dict[FrozenSet[str], List[Measurement]] = {}

  # Processes cumulative measurements first.
  for entry in report_summary.measurement_details:
    if entry.set_operation == "cumulative":
      data_providers = frozenset(entry.data_providers)
      measurements = [
          Measurement(result.reach, result.standard_deviation,
                      result.metric)
          for result in entry.measurement_results
      ]
      if entry.measurement_policy == "ami":
        ami_measurements[data_providers] = measurements
      elif entry.measurement_policy == "mrc":
        mrc_measurements[data_providers] = measurements

  edp_comb_list = ami_measurements.keys()
  if len(edp_comb_list) == 0:
    edp_comb_list = mrc_measurements.keys()

  # Processes non-cumulative union measurements.
  for entry in report_summary.measurement_details:
    if (entry.set_operation == "union") and (
        entry.is_cumulative == False) and (
        frozenset(entry.data_providers) in edp_comb_list):
      measurements = [
          Measurement(result.reach, result.standard_deviation,
                      result.metric)
          for result in entry.measurement_results
      ]
      if entry.measurement_policy == "ami":
        ami_measurements[frozenset(entry.data_providers)].extend(
            measurements)
      elif entry.measurement_policy == "mrc":
        mrc_measurements[frozenset(entry.data_providers)].extend(
            measurements)

  # Builds the report based on the above measurements.
  report = Report(
      {
          policy: MetricReport(measurements)
          for policy, measurements in
          [("ami", ami_measurements), ("mrc", mrc_measurements)]
          if measurements  # Only include if measurements is not empty
      },
      metric_subsets_by_parent={ami: [mrc]},
      cumulative_inconsistency_allowed_edp_combs={},
  )

  # Gets the corrected report.
  corrected_report = report.get_corrected_report()

  # Gets the mapping between a measurement and its corrected value.
  metric_name_to_value: dict[str][int] = {}
  measurements_policies = corrected_report.get_metrics()
  for policy in measurements_policies:
    metric_report = corrected_report.get_metric_report(policy)
    for edp in metric_report.get_edp_combs():
      for index in range(metric_report.get_number_of_periods()):
        entry = metric_report.get_edp_comb_measurement(edp, index)
        metric_name_to_value.update(
            {entry.metric_name: int(entry.value)})

  return metric_name_to_value


def getCorrectedReport(measurements):
  report = Report(
      {
          ami: MetricReport(
              reach_time_series_by_edp_combination={
                  frozenset({EDP_ONE, EDP_TWO}): measurements[TOTAL_CAMPAIGN][
                    AMI_FILTER
                  ],
                  frozenset({EDP_ONE}): measurements[EDP_ONE][AMI_FILTER],
                  frozenset({EDP_TWO}): measurements[EDP_TWO][AMI_FILTER],
              }
          ),
          mrc: MetricReport(
              reach_time_series_by_edp_combination={
                  frozenset({EDP_ONE, EDP_TWO}): measurements[TOTAL_CAMPAIGN][
                    MRC_FILTER
                  ],
                  frozenset({EDP_ONE}): measurements[EDP_ONE][MRC_FILTER],
                  frozenset({EDP_TWO}): measurements[EDP_TWO][MRC_FILTER],
              }
          ),
      },
      # AMI is a parent of MRC
      metric_subsets_by_parent={ami: [mrc]},
      cumulative_inconsistency_allowed_edp_combs={},
  )

  return report.get_corrected_report()


def correctSheetMetric(df, rows, func):
  for period, (index, row) in enumerate(rows.iterrows()):
    df.at[index, CUML_REACH_COL_NAME] = math.ceil(func(period).value)


def correctCumSheet(df, ami_func, mrc_func):
  ami_rows = df[df[FILTER_COL_NAME] == AMI_FILTER]
  mrc_rows = df[df[FILTER_COL_NAME] == MRC_FILTER]
  correctSheetMetric(df, ami_rows, ami_func)
  correctSheetMetric(df, mrc_rows, mrc_func)
  return df


def correctTotSheet(df, ami_val, mrc_val):
  ami_rows = df[df[FILTER_COL_NAME] == AMI_FILTER]
  mrc_rows = df[df[FILTER_COL_NAME] == MRC_FILTER]

  # There has to be 1 row for AMI and MRC metrics in the total reach sheet.
  assert ami_rows.shape[0] == 1 and mrc_rows.shape[0] == 1
  df.at[ami_rows.index[0], TOTAL_REACH_COL_NAME] = math.ceil(ami_val)
  df.at[mrc_rows.index[0], TOTAL_REACH_COL_NAME] = math.ceil(mrc_val)
  return df


def buildCorrectedExcel(correctedReport, excel):
  ami_metric_report = correctedReport.get_metric_report(ami)
  mrc_metric_report = correctedReport.get_metric_report(mrc)

  for edp in EDP_MAP:
    edp_index = EDP_MAP[edp]["ind"]
    amiFunc = (
        partial(ami_metric_report.get_edp_comb_measurement,
                frozenset({EDP_ONE, EDP_TWO}))
        if (edp == TOTAL_CAMPAIGN)
        else partial(ami_metric_report.get_edp_comb_measurement,
                     frozenset({edp}))
    )
    mrcFunc = (
        partial(mrc_metric_report.get_edp_comb_measurement,
                frozenset({EDP_ONE, EDP_TWO}))
        if (edp == TOTAL_CAMPAIGN)
        else partial(mrc_metric_report.get_edp_comb_measurement,
                     frozenset({edp}))
    )

    cumilative_sheet_name = EDP_MAP[edp]["sheet"]
    excel[cumilative_sheet_name] = correctCumSheet(
        excel[cumilative_sheet_name], amiFunc, mrcFunc
    )

    # The last value of the corrected measurement series is the total reach.
    totAmiVal = (
        ami_metric_report.get_edp_comb_measurement(
            frozenset({EDP_ONE, EDP_TWO}), -1).value
        if (edp == TOTAL_CAMPAIGN)
        else ami_metric_report.get_edp_comb_measurement(frozenset({edp}),
                                                        -1).value
    )
    totMrcVal = (
        mrc_metric_report.get_edp_comb_measurement(
            frozenset({EDP_ONE, EDP_TWO}), -1).value
        if (edp == TOTAL_CAMPAIGN)
        else mrc_metric_report.get_edp_comb_measurement(frozenset({edp}),
                                                        -1).value
    )
    total_sheet_name = edp
    excel[total_sheet_name] = correctTotSheet(
        excel[total_sheet_name], totAmiVal, totMrcVal
    )
  return excel


def writeCorrectedExcel(path, corrected_excel):
  with pd.ExcelWriter(path) as writer:
    # Write each dataframe to a different sheet
    for sheet_name in corrected_excel:
      corrected_excel[sheet_name].to_excel(
          writer, sheet_name=sheet_name, index=False
      )


def correctExcelFile(path_to_report, unnoised_edps):
  (measurements, excel) = readExcel(path_to_report, unnoised_edps)
  correctedReport = getCorrectedReport(measurements)
  return buildCorrectedExcel(correctedReport, excel)


def main():
  report_summary = report_summary_pb2.ReportSummary()
  # Read the encoded serialized report summary from stdin and convert it back to
  # ReportSummary proto.
  report_summary.ParseFromString(sys.stdin.buffer.read())
  corrected_measurements_dict = processReportSummary(report_summary)

  # Sends the JSON representation of corrected_measurements_dict to the parent
  # program.
  print(json.dumps(corrected_measurements_dict))


if __name__ == "__main__":
  main()
