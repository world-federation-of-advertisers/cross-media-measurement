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
  """
  Processes a ReportSummary and corrects the measurements.

  This class takes a ReportSummary as input and performs the following steps:
  1. Extracts cumulative and whole campaign measurements from the ReportSummary.
  2. Extracts unique reach measurements from the ReportSummary.
  3. Processes the measurements in the ReportSummary so that they are
     consistent.

  Attributes:
      _report_summary: The ReportSummary to process.
      _cumulative_measurements: A dictionary mapping measurement policies
                                to cumulative measurements.
      _whole_campaign_measurements: A dictionary mapping measurement policies
                                     to whole campaign measurements.
      _set_difference_map: A dictionary mapping set different measurements to
                           the corresponding primitive measurements. A different
                           measurement, e.g. unique reach, can be computed from
                           two union measurements, e.g. unique_reach(A) =
                           reach(A U B U C) - reach(B U C).
  """

  def __init__(self, report_summary: report_summary_pb2.ReportSummary()):
    """Initializes ReportSummaryProcessor with a ReportSummary.

    Args:
      report_summary: The ReportSummary proto to process.
    """
    self._report_summary = report_summary
    self._cumulative_measurements: dict[
      str, dict[FrozenSet[str], list[Measurement]]] = {}
    self._whole_campaign_measurements: dict[
      str, dict[FrozenSet[str], Measurement]] = {}
    self._set_difference_map: dict[str, tuple[str, str]] = {}

  def process(self) -> dict[str, int]:
    """
    Processes the report summary and returns the adjusted value for each
    measurement.

    Currently, the function only supports ami and mrc measurements, primitive
    set operations (cumulative and union), and unique reach measurements.

    :return: a mapping between measurement name and its adjusted value.
    """
    # Processes primitive measurements (cumulative and union). This step needs
    # to be completed before processing different measurements (e.g. unique
    # reach) as we need to map every different measurement to two primitive
    # measurements.
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
    """Processes unique reach measurements in the report summary.

    Let Z = EDP_1 U EDP_2 U ... U EDP_N be the union of all the EDPs. The unique
    reach of an EDP X is computed as:
       unique_reach(X) = reach(Z) - reach(Z \ {X}).

    This method extracts unique reach measurements from the ReportSummary by
    identifying entries with 'difference' set operations, then maps it to the
    corresponding primitive measurements. For the measurement unique_reach(X),
    the mapping (X -> (Z, Z \ {X})) will be stored.

    In the report, the reach of the union of all EDPs, reach(Z), always exists,
    however, the intermediate measurement reach(Z \ {X}) may not. In that case,
    we need to derive the measurement reach(Z \ {X}) from reach(Z) and
    unique_reach(X) and add that to the measurement set before adding the above
    mapping.
    """
    for entry in self._report_summary.measurement_details:
      if (entry.set_operation == "difference") and (
          entry.unique_reach_target != ""):
        # subset = entry.data_providers \ {entry.unique_reach_target}.
        # entry.data_providers is the union of all the EDPs.
        # entry.unique_reach_target is a single EDP.
        subset = frozenset([edp for edp in entry.data_providers if
                            edp != entry.unique_reach_target])

        # The unique reach measurements for subset. Note that there is exactly 1
        # measurement in this list.
        measurements = [
            Measurement(result.reach, result.standard_deviation, result.metric)
            for result in entry.measurement_results
        ]
        # Gets the reach of the union of all EDPs. This measurement always
        # always exists in the report summary.
        superset_measurement = \
          self._whole_campaign_measurements[entry.measurement_policy][
            frozenset(entry.data_providers)]

        # Now we need to get the measurement that corresponds to reach(subset)
        # where subset = entry.data_providers \ {entry.unique_reach_target}.
        # If reach(subset) measurement exists in the report summary, maps the
        # unique reach measurement to the tuple (superset, subset). However, if
        # reach(subset) measurement does not exist, it needs to be derived from
        # the superset measurement and the unique reach measurements before the
        # mapping.
        if subset in self._whole_campaign_measurements[
          entry.measurement_policy].keys():
          self._set_difference_map[entry.metric] = [
              superset_measurement.name,
              self._whole_campaign_measurements[entry.measurement_policy][
                subset].name
          ]
        else:
          # Add the measurement of the edp_comb that is derived from the
          # unique_reach(A) and reach(A U edp_comb). As std(unique(A) =
          # sqrt(std(rach(A U edp_comb))^2 + std(reach(edp_comb))^2), we have
          # std(edp_comb) = sqrt(std(unique(A))^2 - std(reach(A U edp_comb))^2).
          measurement = Measurement(
              superset_measurement.value - measurements[0].value,
              math.sqrt(
                  measurements[0].sigma ** 2 - superset_measurement.sigma ** 2),
              "union/" + entry.measurement_policy + "/" + "_".join(
                sorted(subset)))
          self._whole_campaign_measurements[entry.measurement_policy][
            subset] = measurement
          self._set_difference_map[measurements[0].name] = [
              superset_measurement.name,
              measurement.name
          ]


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
