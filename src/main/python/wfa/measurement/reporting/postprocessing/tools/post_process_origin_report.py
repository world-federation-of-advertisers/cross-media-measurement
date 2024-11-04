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
import math
import pandas as pd
import sys

from src.main.proto.wfa.measurement.reporting.postprocessing.v2alpha import \
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


# Processes a report summary and returns a consistent one.
#
# Currently, the function only supports ami and mrc measurements and primitive
# set operations (cumulative and union).
# TODO(@ple13): Extend the function to support custom measurements and composite
#  set operations such as difference, incremental.
def processReportSummary(report_summary: report_summary_pb2.ReportSummary()):
  cumulative_ami_measurements: Dict[FrozenSet[str], List[Measurement]] = {}
  cumulative_mrc_measurements: Dict[FrozenSet[str], List[Measurement]] = {}
  total_ami_measurements: Dict[FrozenSet[str], Measurement] = {}
  total_mrc_measurements: Dict[FrozenSet[str], Measurement] = {}

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
        cumulative_ami_measurements[data_providers] = measurements
      elif entry.measurement_policy == "mrc":
        cumulative_mrc_measurements[data_providers] = measurements

  # Processes total union measurements.
  for entry in report_summary.measurement_details:
    if (entry.set_operation == "union") and (entry.is_cumulative == False):
      measurements = [
          Measurement(result.reach, result.standard_deviation, result.metric)
          for result in entry.measurement_results
      ]
      if entry.measurement_policy == "ami":
        total_ami_measurements[frozenset(entry.data_providers)] = measurements[
          0]
      elif entry.measurement_policy == "mrc":
        total_mrc_measurements[frozenset(entry.data_providers)] = measurements[
          0]

  # Builds the report based on the above measurements.
  report = Report(
      {
          policy: MetricReport(cumulative_measurements, total_measurements)
          for policy, cumulative_measurements, total_measurements in
          [("ami", cumulative_ami_measurements, total_ami_measurements),
           ("mrc", cumulative_mrc_measurements, total_mrc_measurements)]
          if cumulative_measurements
          # Only include if measurements is not empty
      },
      metric_subsets_by_parent={ami: [mrc]},
      cumulative_inconsistency_allowed_edp_combinations={},
  )

  # Gets the corrected report.
  corrected_report = report.get_corrected_report()

  # Gets the mapping between a measurement and its corrected value.
  metric_name_to_value: dict[str][int] = {}
  measurements_policies = corrected_report.get_metrics()
  for policy in measurements_policies:
    metric_report = corrected_report.get_metric_report(policy)
    for edp_combination in metric_report.get_cumulative_edp_combinations():
      for index in range(metric_report.get_number_of_periods()):
        entry = metric_report.get_cumulative_measurement(edp_combination, index)
        metric_name_to_value.update({entry.name: int(entry.value)})
    for edp_combination in metric_report.get_whole_campaign_edp_combinations():
      entry = metric_report.get_whole_campaign_measurement(edp_combination)
      metric_name_to_value.update({entry.name: int(entry.value)})

  return metric_name_to_value


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
