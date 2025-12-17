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
import sys
from typing import FrozenSet

from absl import app
from absl import flags
from absl import logging
from typing import TypeAlias

from noiseninja.noised_measurements import Measurement
from noiseninja.noised_measurements import MeasurementSet
from noiseninja.noised_measurements import KReachMeasurements

from report.report import build_measurement_set
from report.report import EdpCombination
from report.report import MetricReport
from report.report import Report

from wfa.measurement.internal.reporting.postprocessing import report_summary_pb2
from wfa.measurement.internal.reporting.postprocessing import report_post_processor_result_pb2

ReportPostProcessorStatus = report_post_processor_result_pb2.ReportPostProcessorStatus
ReportPostProcessorResult = report_post_processor_result_pb2.ReportPostProcessorResult

MeasurementPolicy: TypeAlias = str

FLAGS = flags.FLAGS

flags.DEFINE_boolean("debug", False, "Enable debug mode.")

ami = "ami"
mrc = "mrc"


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
      _weekly_cumulative_reaches: A dictionary mapping measurement policies
                                to cumulative measurements.
      _whole_campaign_reaches: A dictionary mapping measurement policies
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
    self._weekly_cumulative_reaches: dict[MeasurementPolicy, dict[EdpCombination,
                                                  list[Measurement]]] = {}
    self._whole_campaign_reaches: dict[MeasurementPolicy,
                                            dict[EdpCombination, Measurement]] = {}
    self._whole_campaign_k_reaches: dict[MeasurementPolicy,
                        dict[EdpCombination, KReachMeasurements]] = {}
    self._whole_campaign_impressions: dict[MeasurementPolicy,
                           dict[EdpCombination, Measurement]] = {}
    self._set_difference_map: dict[MeasurementPolicy, tuple[str, str]] = {}
    self._uncorrected_measurements: set[str] = set()

  def process(self) -> ReportPostProcessorResult:
    """
    Processes the report summary and returns the adjusted value for each
    measurement.

    Currently, the function only supports ami and mrc measurements, primitive
    set operations (cumulative and union), and unique reach measurements.

    :return: a mapping between measurement name and its adjusted value.
    """
    logging.info("Processing the report summary.")

    # Processes primitive measurements (cumulative and union). This step needs
    # to be completed before processing different measurements (e.g. unique
    # reach) as we need to map every different measurement to two primitive
    # measurements.
    self._process_primitive_measurements()

    # Process difference measurements (e.g. unique reach, incremental reach).
    self._process_difference_measurements()

    return self._get_corrected_measurements()

  def _get_corrected_measurements(self) -> ReportPostProcessorResult:
    """
    Correct the report and returns the adjusted value for each measurement.
    """
    logging.info("Building a report from the report summary.")

    all_policies = (
        set(self._weekly_cumulative_reaches.keys())
        | set(self._whole_campaign_reaches.keys())
        | set(self._whole_campaign_k_reaches.keys())
        | set(self._whole_campaign_impressions.keys())
    )

    metric_reports = {}
    for policy in all_policies:
      whole_campaign_measurements = build_measurement_set(
          self._whole_campaign_reaches.get(policy, {}),
          self._whole_campaign_k_reaches.get(policy, {}),
          self._whole_campaign_impressions.get(policy, {})
      )
      metric_reports[policy] = MetricReport(
          weekly_cumulative_reaches=self._weekly_cumulative_reaches.get(policy, {}),
          whole_campaign_measurements=whole_campaign_measurements,
          weekly_non_cumulative_measurements={},
      )

    children_metric = []
    if "mrc" in all_policies:
      children_metric.append("mrc")
    if "custom" in all_policies:
      children_metric.append("custom")

    # Builds the report based on the extracted primitive measurements.
    report = Report(
        metric_reports,
        metric_subsets_by_parent={
            ami: children_metric} if "ami" in all_policies and children_metric else {},
        cumulative_inconsistency_allowed_edp_combinations={},
        population_size=self._report_summary.population,
    )

    corrected_report, report_post_processor_result = \
      report.get_corrected_report()

    report_post_processor_result.pre_correction_report_summary.CopyFrom(
        self._report_summary)

    logging.info(
        "Generating the mapping between between measurement name and its "
        "adjusted value."
    )

    # If the QP solver does not converge, return the report post processor
    # result that contains an empty map of updated measurements.
    if not corrected_report:
      return report_post_processor_result

    # If the QP solver finds a solution, update the report post processor result
    # with the updated measurements map.
    metric_name_to_value: dict[str, float] = {}
    measurements_policies = corrected_report.get_metrics()
    for policy in measurements_policies:
      metric_report = corrected_report.get_metric_report(policy)
      for edp_combination in metric_report.get_weekly_cumulative_reach_edp_combinations():
        for index in range(metric_report.get_number_of_periods()):
          entry = metric_report.get_weekly_cumulative_reach_measurement(
              edp_combination, index)
          metric_name_to_value.update({entry.name: entry.value})
      for edp_combination in metric_report.get_whole_campaign_reach_edp_combinations():
        entry = metric_report.get_whole_campaign_reach_measurement(edp_combination)
        metric_name_to_value.update({entry.name: entry.value})
      for edp_combination in metric_report.get_whole_campaign_k_reach_edp_combinations():
        for frequency in range(1,
                               metric_report.get_number_of_frequencies() + 1):
          entry = metric_report.get_whole_campaign_k_reach_measurement(
              edp_combination, frequency)
          metric_name_to_value.update({entry.name: entry.value})
      for edp_combination in metric_report.get_whole_campaign_impression_edp_combinations():
        entry = metric_report.get_whole_campaign_impression_measurement(
            edp_combination)
        metric_name_to_value.update({entry.name: entry.value})

    # Updates difference measurements.
    for key, value in self._set_difference_map.items():
      metric_name_to_value.update(
        {key: metric_name_to_value[value[0]] - metric_name_to_value[value[1]]})

    logging.info("Finished correcting the report.")

    # Update the report post processor result with the new measurements.
    report_post_processor_result.updated_measurements.update(
        metric_name_to_value)

    # Updates the report post processor result with the uncorrected
    # measurements.
    report_post_processor_result.uncorrected_measurements.extend(
        self._uncorrected_measurements
    )

    return report_post_processor_result

  def _process_primitive_measurements(self):
    """Extract the primitive measurements from the report summary.

    This method iterates through the measurement details in the report summary
    and extracts the cumulative and whole campaign measurements.

    For each measurement detail entry:

    - If the set_operation is "cumulative", the measurement is added to the
      `_weekly_cumulative_reaches` dictionary, keyed by the measurement policy
      and the set of data providers.
    - If the set_operation is "union" and is_cumulative is False, the
      measurement is added to the `_whole_campaign_reaches`, or
      `_whole_campaign_k_reaches`, or `_whole_campaign_impressions` dictionary keyed by the measurement policy and
       the set of data providers.
    """
    logging.info(
        "Processing primitive measurements (cumulative and union)."
    )

    seen_measurement_policies: set[str] = set()
    for entry in self._report_summary.measurement_details:
      measurement_policy = entry.measurement_policy
      if measurement_policy not in seen_measurement_policies:
        seen_measurement_policies.add(measurement_policy)
        self._weekly_cumulative_reaches[measurement_policy] = {}
        self._whole_campaign_reaches[measurement_policy] = {}
        self._whole_campaign_k_reaches[measurement_policy] = {}
        self._whole_campaign_impressions[measurement_policy] = {}

      if entry.set_operation == "cumulative":
        logging.debug(
            f"Processing {measurement_policy} cumulative measurements for the "
            f"EDP combination {entry.data_providers}."
        )
        if not all(
            result.HasField('reach') for result in entry.measurement_results):
          raise ValueError(
              "Cumulative measurements must be reach measurements.")
        measurements = [
            Measurement(result.reach.value, result.reach.standard_deviation,
                        result.metric)
            for result in entry.measurement_results
        ]
        self._weekly_cumulative_reaches[measurement_policy][
          frozenset(entry.data_providers)] = measurements
      elif (entry.set_operation == "union") and (entry.is_cumulative == False):
        logging.debug(
            f"Processing {measurement_policy} total campaign measurements for "
            f"the EDP combination {entry.data_providers}."
        )
        if not all(result.HasField('reach') or result.HasField(
            'reach_and_frequency') or result.HasField(
            'impression_count') for result in entry.measurement_results):
          raise ValueError(
              "Total campaign measurements must be either reach, reach and "
              "frequency, or impression count."
          )
        for measurement_result in entry.measurement_results:
          if measurement_result.HasField('reach_and_frequency'):
            self._whole_campaign_reaches[measurement_policy][
              frozenset(entry.data_providers)] = Measurement(
                measurement_result.reach_and_frequency.reach.value,
                measurement_result.reach_and_frequency.reach.standard_deviation,
                measurement_result.metric)
            self._whole_campaign_k_reaches[entry.measurement_policy][
              frozenset(entry.data_providers)] = {}
            for bin in measurement_result.reach_and_frequency.frequency.bins:
              self._whole_campaign_k_reaches[measurement_policy][
                frozenset(entry.data_providers)][int(bin.label)] = Measurement(
                  bin.value,
                  bin.standard_deviation,
                  measurement_result.metric + "-frequency-" + bin.label)
          elif measurement_result.HasField('reach'):
            self._whole_campaign_reaches[measurement_policy][
              frozenset(entry.data_providers)] = Measurement(
                measurement_result.reach.value,
                measurement_result.reach.standard_deviation,
                measurement_result.metric)
          elif measurement_result.HasField('impression_count'):
            self._whole_campaign_impressions[measurement_policy][
              frozenset(entry.data_providers)] = Measurement(
                measurement_result.impression_count.value,
                measurement_result.impression_count.standard_deviation,
                measurement_result.metric)

    # Goes through the measurements, if cumulative measurements for
    # edp_combination exists, but there is no corresponding whole campaign
    # measurement, we use the last week of the cumulative ones as the whole
    # campaign measurement.
    for measurement_policy in self._weekly_cumulative_reaches.keys():
      for edp_combination in self._weekly_cumulative_reaches[
        measurement_policy].keys():
        if edp_combination not in self._whole_campaign_reaches[
          measurement_policy].keys():
          self._whole_campaign_reaches[measurement_policy][
            edp_combination] = Measurement(
              self._weekly_cumulative_reaches[measurement_policy][
                edp_combination][-1].value,
              self._weekly_cumulative_reaches[measurement_policy][
                edp_combination][-1].sigma,
              "derived_reach/" + measurement_policy + "/" + "_".join(
                  sorted(edp_combination))
          )

    logging.info("Finished processing primitive measurements.")

  def _process_difference_measurements(self):
    """Processes difference measurements in the report summary.

    Given two edp combinations X and Y, the set difference measurement between
    X and Y is defined as: difference(X, Y) = reach(X U Y) - reach(Y).

    Let the set of EDPs be EDP_1, ..., EDP_N, the unique reach and incremental
    reach measurements are two special case of set difference.

    1. Incremental reach: Let Y be a subset of Z = EDP_1 U EDP_2 U ... U EDP_N
    and EDP_i is not in Y then
        incremental_reach(EDP_i, Y) = reach(Y U {EDP_i}) - reach(Y)

    2. Unique reach measurement: When Y = Z \ {EDP_i}, then the incremental
    reach is called unique reach.
        unique_reach(EDP_i) = incremental_reach(EDP_i, Z \ {EDP_i})

    This function extracts incremental reach (and unique reach) measurements
    from the ReportSummary by identifying entries with 'difference' set
    operations,and maps it to the corresponding primitive measurements.

    For each measurement incremental_reach(EDP_i, Y) where EDP_i is not in Y,
    the mapping ((EDP_i, Y) -> (EDP_i U Y, Y)) will be stored. For the
    measurement unique_reach(EDP_i), ((EDP_i, Z \ {EDP_i}) -> (Z, Z \ {EDP_i}))
    is stored.

    In the report, the reach of the union of all EDPs, reach(Z), always exists,
    however, the intermediate measurements such as reach(Z \ {X}) may not. In
    that case, we need to derive the measurement reach(Z \ {X}) from existing
    measurements such as reach(Z) and unique_reach(X) and add that to the
    measurement set before adding the above mapping.
    """
    logging.info(
        "Processing difference measurements (unique reach, incremental reach)"
    )
    difference_measurements = []
    for entry in self._report_summary.measurement_details:
      if entry.set_operation == "difference":
        measurements = [
            Measurement(result.reach.value, result.reach.standard_deviation,
                        result.metric)
            for result in entry.measurement_results
        ]
        subset = frozenset([edp for edp in entry.right_hand_side_targets])
        superset = subset.union(
            frozenset([edp for edp in entry.left_hand_side_targets]))
        logging.debug(
            f"Processing the difference measurement {measurements[0]}. The "
            f"left hand side and right hand side EDP combinations are "
            f"{superset} and {subset} respectively."
        )
        # The incremental reach (and unique reach) is computed as:
        # incremental_reach(superset \ subset, subset) = reach(superset) -
        # reach(subset). The set (superset \ subset) consists of a single EDP,
        # while the subset contains one or more EDPs.
        difference_measurements.append(
            [superset, subset, entry.measurement_policy, measurements[0]])
        logging.debug(
            "The left hand side and right hand side EDP combinations are "
            f"{superset} and {subset} respectively."
        )

    # Sorts the difference measurements based of the length of the superset.
    # In the report, the reach of union of all EDPs always exists, i.e.
    # reach(EDP_1 U ... U EDP_N). However, intermediate reaches such as
    # reach(EDP_1 U ... U EDP_{N-1}), reach(EDP_1 U ... U EDP_{N-2}) do not and
    # needs to be inferred. Sorting the difference measurements based on the
    # superset length allows us to infer all intermediate reaches in a single
    # pass: reach(subset) where len(subset) = k - 1 will be inferred from
    # reach(superset) and corresponding incremental reach, where reach(superset)
    # either exists in the reported, or is inferred previously.
    difference_measurements = sorted(difference_measurements,
                                     key=lambda sublist: len(sublist[0]),
                                     reverse=True)

    # TODO(@ple13): Update the logic that handles difference measurements so
    # that it supports reports that do not contain union measurements. The
    # current logic assumes that a report always contains measurements for union
    # of all EDPs, and measurements for each individual EDP.
    for (superset, subset, measurement_policy,
         difference_measurement) in difference_measurements:
      # When both cumulative measurements and total campaign measurements are
      # not in the report, but unique reach or incremental reach measurements
      # exists, the total reach measurements for superset do not exist. In this
      # case, the report post-processor just skip this difference measurement.
      if superset not in self._whole_campaign_reaches[measurement_policy]:
        self._uncorrected_measurements.add(difference_measurement.name)
        logging.warning(
            f'The measurement {difference_measurement.name} cannot be '
            f'corrected due to missing measurement for {superset}.'
        )
        continue
      # Gets the reach of the union of all EDPs. This measurement either
      # exists in the report summary or has been inferred in prior steps.
      superset_measurement = \
        self._whole_campaign_reaches[measurement_policy][superset]

      # Now we need to get the measurement that corresponds to reach(subset).
      # If reach(subset) measurement exists in the report summary, maps the
      # difference measurement to the tuple (superset, subset). However, if
      # reach(subset) measurement does not exist, it needs to be derived from
      # the superset measurement and the difference measurement before the
      # mapping.
      if subset in self._whole_campaign_reaches[measurement_policy].keys():
        self._set_difference_map[difference_measurement.name] = [
            superset_measurement.name,
            self._whole_campaign_reaches[measurement_policy][subset].name
        ]
      else:
        # Add the measurement of the edp_comb that is derived from the
        # incremental_reach(A) and reach(A U subset).
        logging.debug(
            f"Estimating the {measurement_policy} reach of {subset} from "
            f"{superset_measurement.name} and {difference_measurement.name}.")
        # TODO(world-federation-of-advertisers/cross-media-measurement#2136):
        # Use the correct formula to find the standard deviation of the derived
        # metric.
        derived_standard_deviation = max(difference_measurement.sigma,
                                         superset_measurement.sigma)
        subset_measurement = Measurement(
            superset_measurement.value - difference_measurement.value,
            derived_standard_deviation,
            "union/" + measurement_policy + "/" + "_".join(sorted(subset))
        )
        self._whole_campaign_reaches[measurement_policy][
          subset] = subset_measurement
        self._set_difference_map[difference_measurement.name] = [
            superset_measurement.name,
            subset_measurement.name
        ]
    logging.info(
        "Finished processing difference measurements (unique reach, incremental"
        " reach)"
    )


def main(argv):
  # Sends the log to stderr.
  FLAGS.logtostderr = True

  # Sets the log level base on the --debug flag.
  logging.set_verbosity(logging.DEBUG if FLAGS.debug else logging.INFO)

  report_summary = report_summary_pb2.ReportSummary()
  logging.info("Reading the report summary from stdin.")
  report_summary.ParseFromString(sys.stdin.buffer.read())

  logging.info("Processing the report summary.")
  report_post_processor_result = ReportSummaryProcessor(
      report_summary).process()

  logging.info("Serializing the report post processor result.")
  serialized_data = report_post_processor_result.SerializeToString()

  logging.info(
      "Sending serialized ReportPostProcessorResult to the parent program."
  )
  sys.stdout.buffer.write(serialized_data)
  sys.stdout.flush()


if __name__ == "__main__":
  app.run(main)
