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
import sys

from src.main.proto.wfa.measurement.reporting.postprocessing.v2alpha import \
  report_summary_pb2
from noiseninja.noised_measurements import Measurement
from report.report import Report, MetricReport
from typing import FrozenSet

# This is a demo script that has the following assumptions :
# 1. CUSTOM filters are not yet supported in this tool.
# 2. AMI is a parent of MRC and there are no other relationships between metrics.
# 3. Impression results are not corrected.

ami = "ami"
mrc = "mrc"

# TODO(@ple13): Extend the class to support custom measurements and composite
# set operations such as incremental.
class ReportSummaryProcessor:
  """Processes a ReportSummary and corrects the measurements."""
  def __init__(self, report_summary: report_summary_pb2.ReportSummary()):
    """Initializes ReportSummaryProcessor with a ReportSummary.

    Args:
      report_summary: The ReportSummary proto to process.
    """
    self._report_summary = report_summary
    self._cumulative_measurements = {}
    self._whole_campaign_measurements = {}
    self._set_difference_map = {}

  def process(self) -> dict[str, int]:
    """
    Processes the report summary and returns the adjusted value for each
    measurement.

    Currently, the function only supports ami and mrc measurements, primitive
    set operations (cumulative and union), and unique reach measurements.

    :return: a mapping between measurement name and its adjusted value.
    """
    # Processes primitive measurements (cumulative and union) first.
    self._process_primitive_measurements()

    # Process unique reach measurements.
    self._process_unique_reach_measurements()

    return self._get_corrected_measurements()

  def _get_corrected_measurements(self):
    """
    Correct the report and returns the adjusted value for each measurement.
    """
    # Builds the report based on the extracted primitive measurements.
    report = Report(
        {
            policy: MetricReport(self._cumulative_measurements[policy],
                                 self._whole_campaign_measurements[policy])
            for policy in self._cumulative_measurements
        },
        metric_subsets_by_parent={
            ami: [mrc]} if "mrc" in self._cumulative_measurements else {},
        cumulative_inconsistency_allowed_edp_combinations={},
    )

    # Gets the corrected report.
    corrected_report = report.get_corrected_report()

    # Gets the mapping between a measurement and its corrected value.
    metric_name_to_value: dict[str, int] = {}
    measurements_policies = corrected_report.get_metrics()
    for policy in measurements_policies:
      metric_report = corrected_report.get_metric_report(policy)
      for edp_combination in metric_report.get_cumulative_edp_combinations():
        for index in range(metric_report.get_number_of_periods()):
          entry = metric_report.get_cumulative_measurement(edp_combination,
                                                           index)
          metric_name_to_value.update({entry.name: int(entry.value)})
      for edp_combination in metric_report.get_whole_campaign_edp_combinations():
        entry = metric_report.get_whole_campaign_measurement(edp_combination)
        metric_name_to_value.update({entry.name: int(entry.value)})

    # Updates unique reach measurements.
    for key, value in self._set_difference_map.items():
      metric_name_to_value.update({key: (
          metric_name_to_value[value[0]] - metric_name_to_value[value[1]])})

    return metric_name_to_value

  def _process_primitive_measurements(self):
    """Extract the primitive measurements from the report summary."""
    for entry in self._report_summary.measurement_details:
      measurements = [
          Measurement(result.reach, result.standard_deviation, result.metric)
          for result in entry.measurement_results
      ]
      if entry.set_operation == "cumulative":
        if entry.measurement_policy not in self._cumulative_measurements:
          self._cumulative_measurements[entry.measurement_policy] = {}
        self._cumulative_measurements[entry.measurement_policy][
          frozenset(entry.data_providers)] = measurements
      elif (entry.set_operation == "union") and (entry.is_cumulative == False):
        if entry.measurement_policy not in self._whole_campaign_measurements:
          self._whole_campaign_measurements[entry.measurement_policy] = {}
        self._whole_campaign_measurements[entry.measurement_policy][
          frozenset(entry.data_providers)] = measurements[0]

  def _process_unique_reach_measurements(self):
    """Extract unique reach measurements from the report summary."""
    for entry in self._report_summary.measurement_details:
      if (entry.set_operation == "difference") and (
          entry.unique_reach_target != ""):
        subset = frozenset([edp for edp in entry.data_providers if
                            edp != entry.unique_reach_target])
        measurements = [
            Measurement(result.reach, result.standard_deviation, result.metric)
            for result in entry.measurement_results
        ]
        superset_measurement = \
          self._whole_campaign_measurements[entry.measurement_policy][
            frozenset(entry.data_providers)]

        if subset in self._whole_campaign_measurements[
          entry.measurement_policy].keys():
          self._set_difference_map[entry.metric] = [superset_measurement.name,
                                                    self._whole_campaign_measurements[
                                                      entry.measurement_policy][
                                                      subset].name]
        else:
          # Add the measurement of the edp_comb that is derived from the
          # unique_reach(A) and reach(A U edp_comb). As std(unique(A) =
          # sqrt(std(rach(A U edp_comb))^2 + std(reach(edp_comb))^2), we have
          # std(edp_comb) = sqrt(std(unique(A))^2 - std(reach(A U edp_comb))^2).
          measurement = Measurement(
              superset_measurement.value - measurements[0].value,
              math.sqrt(
                  measurements[0].sigma ** 2 - superset_measurement.sigma ** 2),
              "union/" + entry.measurement_policy + "/" + "_".join(sorted(subset)))
          self._whole_campaign_measurements[entry.measurement_policy][
            subset] = measurement
          self._set_difference_map[measurements[0].name] = [
              superset_measurement.name,
              measurement.name]

def main():
  report_summary = report_summary_pb2.ReportSummary()
  # Read the encoded serialized report summary from stdin and convert it back to
  # ReportSummary proto.
  report_summary.ParseFromString(sys.stdin.buffer.read())

  corrected_measurements_dict = ReportSummaryProcessor(report_summary).process()

  # Sends the JSON representation of corrected_measurements_dict to the parent
  # program.
  print(json.dumps(corrected_measurements_dict))


if __name__ == "__main__":
  main()
