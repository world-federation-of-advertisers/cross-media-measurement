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
import openpyxl
import statistics
import sys
import unittest
import os

from google.protobuf.json_format import Parse
from src.main.proto.wfa.measurement.reporting.postprocessing.v2alpha import \
  report_summary_pb2

from tools.post_process_origin_report import ReportSummaryProcessor


class MetricReport:
  reach_time_series: dict[str, list[int]]
  reach_whole_campaign: dict[str, int]

  def __init__(self, reach_time_series: dict[str, list[int]],
      reach_whole_campaign: dict[str, int]):
    self.reach_time_series = reach_time_series
    self.reach_whole_campaign = reach_whole_campaign


CUMULATIVE_EDP = {
    "edp1": frozenset({"edp1"}),
    "edp2": frozenset({"edp2"}),
    "edp3": frozenset({"edp3"}),
    "edp4": frozenset({"edp4"}),
    "union": frozenset({"edp1", "edp2", "edp3", "edp4"}),
}

EDP_MAP = {
    "edp1": frozenset({"edp1"}),
    "edp2": frozenset({"edp2"}),
    "edp3": frozenset({"edp3"}),
    "edp4": frozenset({"edp4"}),
    "edp12": frozenset({"edp1", "edp2"}),
    "edp13": frozenset({"edp1", "edp3"}),
    "edp14": frozenset({"edp1", "edp4"}),
    "edp23": frozenset({"edp2", "edp3"}),
    "edp24": frozenset({"edp2", "edp4"}),
    "edp34": frozenset({"edp3", "edp4"}),
    "edp123": frozenset({"edp1", "edp2", "edp3"}),
    "edp124": frozenset({"edp1", "edp2", "edp4"}),
    "edp134": frozenset({"edp1", "edp3", "edp4"}),
    "edp234": frozenset({"edp2", "edp3", "edp4"}),
    "union": frozenset({"edp1", "edp2", "edp3", "edp4"}),
}

probabilities = {"edp1": 0.015, "edp2": 0.02, "edp3": 0.2, "edp4": 0.25}
scales = {"edp1": 0.9, "edp2": 0.9, "edp3": 0.5, "edp4": 0.5}

mrc_to_ami_rate = 0.9

MEASUREMENT_POLICIES = ["ami", "mrc"]
POPULATION_SIZE = 55000000
CUMULATIVE_LENGTH = 16
ITERATIONS = 50

# DP params:
# a) Direct measurement: (eps, delta) = (0.00643, 1e-15) --> sigma = 1051
# b) MPC measurement:    (eps, delta) = (0.01265, 1e-15) --> sigma =  542
SIGMAS = {
    "edp1": 1051.0,
    "edp2": 1051.0,
    "edp3": 1051.0,
    "edp4": 1051.0,
    "union": 542.0,
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


def generate_union_measurements(reach_time_series, reach_whole_campaign):
  for i in range(CUMULATIVE_LENGTH):
    reach_time_series[EDP_MAP["union"]].append(
        int((1.0 - (
            1 - reach_time_series[EDP_MAP["edp1"]][i] / POPULATION_SIZE) * (
                 1 -
                 reach_time_series[
                   EDP_MAP[
                     "edp2"]][
                   i] / POPULATION_SIZE) * (
                 1 -
                 reach_time_series[
                   EDP_MAP[
                     "edp3"]][
                   i] / POPULATION_SIZE) * (
                 1 -
                 reach_time_series[
                   EDP_MAP[
                     "edp4"]][
                   i] / POPULATION_SIZE)) * POPULATION_SIZE)
    )

  reach_whole_campaign[EDP_MAP["edp12"]] = \
    int((1.0 - (
        1 - reach_whole_campaign[EDP_MAP["edp1"]] / POPULATION_SIZE) * (
             1 - reach_whole_campaign[
           EDP_MAP[
             "edp2"]] / POPULATION_SIZE)) * POPULATION_SIZE)
  reach_whole_campaign[EDP_MAP["edp13"]] = \
    int((1.0 - (
        1 - reach_whole_campaign[EDP_MAP["edp1"]] / POPULATION_SIZE) * (
             1 - reach_whole_campaign[
           EDP_MAP[
             "edp3"]] / POPULATION_SIZE)) * POPULATION_SIZE)
  reach_whole_campaign[EDP_MAP["edp14"]] = \
    int((1.0 - (
        1 - reach_whole_campaign[EDP_MAP["edp1"]] / POPULATION_SIZE) * (
             1 - reach_whole_campaign[
           EDP_MAP[
             "edp4"]] / POPULATION_SIZE)) * POPULATION_SIZE)
  reach_whole_campaign[EDP_MAP["edp23"]] = \
    int((1.0 - (
        1 - reach_whole_campaign[EDP_MAP["edp2"]] / POPULATION_SIZE) * (
             1 - reach_whole_campaign[
           EDP_MAP[
             "edp3"]] / POPULATION_SIZE)) * POPULATION_SIZE)
  reach_whole_campaign[EDP_MAP["edp24"]] = \
    int((1.0 - (
        1 - reach_whole_campaign[EDP_MAP["edp2"]] / POPULATION_SIZE) * (
             1 - reach_whole_campaign[
           EDP_MAP[
             "edp4"]] / POPULATION_SIZE)) * POPULATION_SIZE)
  reach_whole_campaign[EDP_MAP["edp34"]] = \
    int((1.0 - (
        1 - reach_whole_campaign[EDP_MAP["edp3"]] / POPULATION_SIZE) * (
             1 - reach_whole_campaign[
           EDP_MAP[
             "edp4"]] / POPULATION_SIZE)) * POPULATION_SIZE)
  reach_whole_campaign[EDP_MAP["edp123"]] = \
    int((1.0 - (
        1 - reach_whole_campaign[EDP_MAP["edp1"]] / POPULATION_SIZE) * (
             1 -
             reach_whole_campaign[
               EDP_MAP[
                 "edp2"]] / POPULATION_SIZE) * (
             1 -
             reach_whole_campaign[
               EDP_MAP[
                 "edp3"]] / POPULATION_SIZE)) * POPULATION_SIZE)
  reach_whole_campaign[EDP_MAP["edp124"]] = \
    int((1.0 - (
        1 - reach_whole_campaign[EDP_MAP["edp1"]] / POPULATION_SIZE) * (
             1 -
             reach_whole_campaign[
               EDP_MAP[
                 "edp2"]] / POPULATION_SIZE) * (
             1 -
             reach_whole_campaign[
               EDP_MAP[
                 "edp4"]] / POPULATION_SIZE)) * POPULATION_SIZE)
  reach_whole_campaign[EDP_MAP["edp134"]] = \
    int((1.0 - (
        1 - reach_whole_campaign[EDP_MAP["edp1"]] / POPULATION_SIZE) * (
             1 -
             reach_whole_campaign[
               EDP_MAP[
                 "edp3"]] / POPULATION_SIZE) * (
             1 -
             reach_whole_campaign[
               EDP_MAP[
                 "edp4"]] / POPULATION_SIZE)) * POPULATION_SIZE)
  reach_whole_campaign[EDP_MAP["edp234"]] = \
    int((1.0 - (
        1 - reach_whole_campaign[EDP_MAP["edp2"]] / POPULATION_SIZE) * (
             1 -
             reach_whole_campaign[
               EDP_MAP[
                 "edp3"]] / POPULATION_SIZE) * (
             1 -
             reach_whole_campaign[
               EDP_MAP[
                 "edp4"]] / POPULATION_SIZE)) * POPULATION_SIZE)
  reach_whole_campaign[EDP_MAP["union"]] = \
    int((1.0 - (
        1 - reach_whole_campaign[EDP_MAP["edp1"]] / POPULATION_SIZE) * (
             1 -
             reach_whole_campaign[
               EDP_MAP[
                 "edp2"]] / POPULATION_SIZE) * (
             1 -
             reach_whole_campaign[
               EDP_MAP[
                 "edp3"]] / POPULATION_SIZE) * (
             1 -
             reach_whole_campaign[
               EDP_MAP[
                 "edp4"]] / POPULATION_SIZE)) * POPULATION_SIZE)


def generate_test_report(probabilities: dict[str, float]):
  metric_reports = {"ami": {}, "mrc": {}}

  # Generate AMI report.
  reach_time_series = {}
  reach_whole_campaign = {}
  for key in probabilities.keys():
    reach_time_series[EDP_MAP[key]] = []
    reach_whole_campaign[EDP_MAP[key]] = 0
    probability = probabilities[key]
    scale = scales[key]
    current_sum = 0
    for i in range(CUMULATIVE_LENGTH):
      reach_time_series[EDP_MAP[key]].append(
          int(probability * (POPULATION_SIZE - current_sum)))
      probability = scale * probability
      if i > 0:
        reach_time_series[EDP_MAP[key]][i] += reach_time_series[EDP_MAP[key]][
          i - 1]
      current_sum = reach_time_series[EDP_MAP[key]][i]
    reach_whole_campaign[EDP_MAP[key]] = \
      int(probability * (POPULATION_SIZE - current_sum)) + \
      reach_time_series[EDP_MAP[key]][CUMULATIVE_LENGTH - 1]

  reach_time_series[EDP_MAP["union"]] = []
  reach_whole_campaign[EDP_MAP["union"]] = 0

  generate_union_measurements(reach_time_series, reach_whole_campaign)
  metric_reports["ami"] = MetricReport(reach_time_series, reach_whole_campaign)

  # for key, value in metric_reports["ami"].reach_time_series.items():
  #   print(f"{key}: {value}")
  #
  # for key, value in metric_reports["ami"].reach_whole_campaign.items():
  #   print(f"{key}: {value}")

  # Generate MRC report
  reach_time_series = {}
  reach_whole_campaign = {}
  for key in probabilities.keys():
    reach_time_series[EDP_MAP[key]] = []
    reach_whole_campaign[EDP_MAP[key]] = 0
    for i in range(CUMULATIVE_LENGTH):
      reach_time_series[EDP_MAP[key]].append(
          int((0.9 + 0.09 * i / CUMULATIVE_LENGTH) *
              metric_reports["ami"].reach_time_series[EDP_MAP[key]][i]))
    reach_whole_campaign[EDP_MAP[key]] = int(0.999 *
                                             metric_reports[
                                               "ami"].reach_whole_campaign[
                                               EDP_MAP[key]])
  reach_time_series[EDP_MAP["union"]] = []
  reach_whole_campaign[EDP_MAP["union"]] = 0
  generate_union_measurements(reach_time_series, reach_whole_campaign)
  metric_reports["mrc"] = MetricReport(reach_time_series, reach_whole_campaign)

  # for key, value in metric_reports["mrc"].reach_time_series.items():
  #   print(f"{key}: {value}")
  #
  # for key, value in metric_reports["mrc"].reach_whole_campaign.items():
  #   print(f"{key}: {value}")

  return metric_reports


def get_report_summary_from_data(metric_reports):
  report_summary = report_summary_pb2.ReportSummary()
  # Generates report summary from the measurements. For each edp combination,
  # all measurements except the last one are cumulative measurements, and the
  # last one is the whole campaign measurement.
  noisy_measurement_map = {}
  true_measurement_map = {}

  # Processes cumulative measurements.
  for policy in MEASUREMENT_POLICIES:
    for edp in CUMULATIVE_EDP:
      measurement_detail = report_summary.measurement_details.add()
      measurement_detail.measurement_policy = policy
      measurement_detail.set_operation = "cumulative"
      measurement_detail.is_cumulative = True
      measurement_detail.data_providers.extend(EDP_MAP[edp])

      for i in range(CUMULATIVE_LENGTH):
        result = measurement_detail.measurement_results.add()
        result.standard_deviation = SIGMAS[edp]
        result.metric = policy + "_cumulative_" + edp + "_" + str(i).zfill(2)

        result.reach = metric_reports[policy].reach_time_series[EDP_MAP[edp]][i] \
                       + int(np.random.normal(0, SIGMAS[edp], 1)[0])
        true_measurement_map[result.metric] = \
          metric_reports[policy].reach_time_series[EDP_MAP[edp]][i]
        noisy_measurement_map[result.metric] = result.reach

  # Processes total campaign measurements.
  for policy in MEASUREMENT_POLICIES:
    for edp in EDP_MAP:
      measurement_detail = report_summary.measurement_details.add()
      measurement_detail.measurement_policy = policy
      measurement_detail.set_operation = "union"
      measurement_detail.is_cumulative = False
      measurement_detail.data_providers.extend(EDP_MAP[edp])
      result = measurement_detail.measurement_results.add()
      result.standard_deviation = SIGMAS["union"]
      result.metric = "total_" + policy + "_" + edp
      result.reach = metric_reports[policy].reach_whole_campaign[EDP_MAP[edp]] \
                     + int(np.random.normal(0, SIGMAS["union"], 1)[0])
      true_measurement_map[result.metric] = \
        metric_reports[policy].reach_whole_campaign[EDP_MAP[edp]]
      noisy_measurement_map[result.metric] = result.reach

  return report_summary, true_measurement_map, noisy_measurement_map


def calculate_means_and_variances(true_measurements, noisy_measurements,
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


def get_measurement_name_and_ground_truth():
  metric_reports = generate_test_report(probabilities)
  true_report_summary, true_measurement_map, noisy_measurement_map = \
    get_report_summary_from_data(metric_reports)

  print("Measurement name:")
  for key, value in sorted(true_measurement_map.items()):
    print(f"{key} \t {value}")

  sorted_measurement_names = [
      key for key, value in sorted(true_measurement_map.items())
  ]
  sorted_true_measurements = [
      value for key, value in sorted(true_measurement_map.items())
  ]
  return sorted_measurement_names, sorted_true_measurements


def get_test_results():
  noisy_results = {}
  corrected_results = {}

  for i in range(0, ITERATIONS):
    print(f"iteration {i}")
    metric_reports = generate_test_report(probabilities)
    report_summary, true_measurement_map, noisy_measurement_map = \
      get_report_summary_from_data(metric_reports)

    corrected_measurement_map = ReportSummaryProcessor(
        report_summary).process()

    sorted_noisy_measurements = [
        value for key, value in sorted(noisy_measurement_map.items())
    ]

    sorted_corrected_measurements = [
        value for key, value in sorted(corrected_measurement_map.items())
    ]
    noisy_results[i] = sorted_noisy_measurements
    corrected_results[i] = sorted_corrected_measurements

  return noisy_results, corrected_results


def compute_statistic(true_values, measurements):
  # Convert the measurements to an array of size ITERATIONS x NUM_VARIABLES.
  # Each row of the array is the result of a test run.
  measurements_array = np.array(list(measurements.values()))

  # Calculate the mean of the measurements.
  mean_measurements = np.mean(measurements_array, axis=0)

  # Calculate the relative bias.
  relative_bias = (mean_measurements - true_values) / np.array(true_values)

  # Calculate the relative variance for each true value
  relative_variances = np.var(measurements_array, axis=0) / np.array(
    true_values)

  # Calculate relative error for each measurement
  relative_errors = np.abs(
    measurements_array - np.array(true_values)) / np.array(true_values)

  # Calculate the % of measurements with relative error > 2% for each measurement.
  percent_high_rel_error = (np.sum(relative_errors > 0.02, axis=0) / float(len(measurements))) * 100

  avg_rel_error = np.mean(relative_errors)
  median_rel_error = np.median(relative_errors)
  worst_rel_error = np.max(relative_errors)
  quantile_75_rel_error = np.percentile(relative_errors, 75)

  avg_rel_var = np.mean(relative_variances)
  median_rel_var = np.median(relative_variances)
  worst_rel_var = np.max(relative_variances)
  quantile_75_rel_var = np.percentile(relative_variances, 75)


  print("Relative bias:")
  print_array(relative_bias)

  print("Relative var:")
  print_array(relative_variances)

  print("High error percentage:")
  print_array(percent_high_rel_error)

  print(f"Average relative error\t{avg_rel_error}")
  print(f"Median relative error\t{median_rel_error}")
  print(f"Worst relative error\t{worst_rel_error}")
  print(f"Quantile 75 relative error\t{quantile_75_rel_error}")

  print(f"Average relative var\t{avg_rel_var}")
  print(f"Median relative var\t{median_rel_var}")
  print(f"Worst relative var\t{worst_rel_var}")
  print(f"Quantile 75 relative var\t{quantile_75_rel_var}")

  return relative_bias, relative_variances, percent_high_rel_error

def get_measurement_name_to_index(measurement_names):
  measurement_name_to_index = {}
  for i, string in enumerate(measurement_names):
    measurement_name_to_index[string] = i
  return measurement_name_to_index

def compute_unique_reach_statistic(true_values, measurements, parent_index, child_index):
  measurements_array = np.array(list(measurements.values()))
  # Calculate the true difference between measurements at indices parent_index and child_index
  true_diff = true_values[parent_index] - true_values[child_index]

  # Calculate the difference between measurements at indices i and j
  estimated_diff = measurements_array[:, parent_index] - measurements_array[:, child_index]

  # Calculate the mean of the measurements.
  mean_measurements = np.mean(estimated_diff,axis=0)

  # Calculate the relative bias.
  relative_bias = (mean_measurements - true_diff) / true_diff

  # Calculate the relative variance for each true value
  relative_variances = np.var(estimated_diff,axis=0) / true_diff

  # Calculate relative error for each measurement
  relative_errors = np.abs(estimated_diff - true_diff) / true_diff

  # Calculate the % of measurements with relative error > 2% for each measurement.
  percent_high_rel_error = np.sum(relative_errors > 0.02, axis=0) / len(
      estimated_diff) * 100.0

  print("Relative bias:")
  print(relative_bias)

  print("Relative variance:")
  print(relative_variances)

  print("High error percentage:")
  print(percent_high_rel_error)

  return relative_bias, relative_variances, percent_high_rel_error

def print_array(arr):
  for val in arr:
    print(val)


class TestOriginReport(unittest.TestCase):
  def test_variance(self):
    sorted_measurement_names, sorted_true_measurements = \
      get_measurement_name_and_ground_truth()
    noisy_measurements, corrected_measurements = get_test_results()

    print("Noisy measurement statistics:")
    compute_statistic(sorted_true_measurements, noisy_measurements)

    print("Corrected measurement statistics:")
    compute_statistic(sorted_true_measurements, corrected_measurements)

    measurement_name_to_index = get_measurement_name_to_index(sorted_measurement_names)

    print("unique reach edp2 \ edp1 statistics:")
    union_edp1_edp2_index = measurement_name_to_index["total_ami_edp12"]
    union_edp1_index = measurement_name_to_index["total_ami_edp1"]

    print("Noisy measurement:")
    compute_unique_reach_statistic(sorted_true_measurements,
                                   noisy_measurements,
                                   union_edp1_edp2_index,
                                   union_edp1_index
                                   )

    print("Corrected measurement:")
    compute_unique_reach_statistic(sorted_true_measurements,
                                   corrected_measurements,
                                   union_edp1_edp2_index,
                                   union_edp1_index
                                   )

    print("unique reach edp3 \ edp1 statistics:")
    union_edp1_edp2_index = measurement_name_to_index["total_ami_edp13"]
    union_edp1_index = measurement_name_to_index["total_ami_edp1"]

    print("Noisy measurement:")
    compute_unique_reach_statistic(sorted_true_measurements,
                                   noisy_measurements,
                                   union_edp1_edp2_index,
                                   union_edp1_index
                                   )

    print("Corrected measurement:")
    compute_unique_reach_statistic(sorted_true_measurements,
                                   corrected_measurements,
                                   union_edp1_edp2_index,
                                   union_edp1_index
                                   )

    print("unique reach edp1 \ edp3 statistics:")
    union_edp1_edp2_index = measurement_name_to_index["total_ami_edp13"]
    union_edp1_index = measurement_name_to_index["total_ami_edp3"]

    print("Noisy measurement:")
    compute_unique_reach_statistic(sorted_true_measurements,
                                   noisy_measurements,
                                   union_edp1_edp2_index,
                                   union_edp1_index
                                   )

    print("Corrected measurement:")
    compute_unique_reach_statistic(sorted_true_measurements,
                                   corrected_measurements,
                                   union_edp1_edp2_index,
                                   union_edp1_index
                                   )

    print("unique reach edp4 \ edp3 statistics:")
    union_edp1_edp2_index = measurement_name_to_index["total_ami_edp34"]
    union_edp1_index = measurement_name_to_index["total_ami_edp3"]

    print("Noisy measurement:")
    compute_unique_reach_statistic(sorted_true_measurements,
                                   noisy_measurements,
                                   union_edp1_edp2_index,
                                   union_edp1_index
                                   )

    print("Corrected measurement:")
    compute_unique_reach_statistic(sorted_true_measurements,
                                   corrected_measurements,
                                   union_edp1_edp2_index,
                                   union_edp1_index
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
