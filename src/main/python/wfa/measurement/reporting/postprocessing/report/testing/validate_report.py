# Copyright 2025 The Cross-Media Measurement Authors
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

from typing import FrozenSet

from absl import logging

from noiseninja.noised_measurements import OrderedSets

from src.main.python.wfa.measurement.reporting.postprocessing.report.report import (
    Report,
    fuzzy_less_equal,
    get_edps_from_edp_combination,
)


def are_overlap_constraints_consistent(report: Report, tolerance: float = 0.0):
  is_consistent: bool = True
  # Verifies overlap constraints for cumulative measurements.
  for metric in report._metric_reports:
    metric_report = report._metric_reports[metric]
    cumulative_edp_combinations = \
      metric_report.get_weekly_cumulative_reach_edp_combinations()
    for edp_combination in cumulative_edp_combinations:
      if len(edp_combination) <= 1:
        continue
      edps: list[FrozenSet[str]] = get_edps_from_edp_combination(
          edp_combination, cumulative_edp_combinations
      )
      if len(edps) != len(edp_combination):
        continue
      # Let X = {X_i} be cumulative measurements at period k and Y = {Y_i}
      # be cumulative measurements at period (k+1). As X_i is a subset of Y_i
      # for i in [0, k], we verify that:
      # |X_1| + ... + |X_k| - |X_1 U ... U X_k| <=
      # |Y_1| + ... + |Y_k| - |Y_1 U ... U Y_k|.
      for period in range(0, report._num_periods - 1):
        smaller_sum = - metric_report.get_weekly_cumulative_reach_measurement(
            edp_combination, period
        ).value

        larger_sum = - metric_report.get_weekly_cumulative_reach_measurement(
            edp_combination, period + 1
        ).value

        for edp in edps:
          smaller_sum += metric_report.get_weekly_cumulative_reach_measurement(
              edp, period
          ).value
          larger_sum += metric_report.get_weekly_cumulative_reach_measurement(
              edp, period + 1
          ).value

        if not fuzzy_less_equal(smaller_sum, larger_sum, 2 * (
            1 + len(edps)) * tolerance):
          logging.info(
              f'Overlap constraint check for {metric} cumulative measurements '
              f'is not satisfied for {edp_combination}. At the {period}-th '
              f'period, the smaller sum {smaller_sum} is larger than the larger'
              f' sum {larger_sum}.'
          )
          is_consistent = False

  # Verifies overlap constraints for measurements across metrics.
  for parent_metric in report._metric_subsets_by_parent:
    for child_metric in report._metric_subsets_by_parent[parent_metric]:
      parent_metric_report = report._metric_reports[parent_metric]
      child_metric_report = report._metric_reports[child_metric]
      # Verifies comulative measurements.
      common_cumulative_edp_combinations = \
        parent_metric_report.get_weekly_cumulative_reach_edp_combinations().intersection(
            child_metric_report.get_weekly_cumulative_reach_edp_combinations())
      for edp_combination in common_cumulative_edp_combinations:
        if len(edp_combination) <= 1:
          continue
        edps: list[FrozenSet[str]] = get_edps_from_edp_combination(
            edp_combination, common_cumulative_edp_combinations
        )
        if len(edps) != len(edp_combination):
          continue
        # Let X = {X_i} be the child cumulative measurements and X = {Y_i} be
        # the parent cumulative measurements. As X_i is a subset of Y_i for i
        # in [0, k], we verify that:
        # |X_1| + ... + |X_k| - |X_1 U ... U X_k| <=
        # |Y_1| + ... + |Y_k| - |Y_1 U ... U Y_k|.
        for period in range(0, report._num_periods):
          smaller_sum = - child_metric_report.get_weekly_cumulative_reach_measurement(
              edp_combination, period
          ).value
          larger_sum = - parent_metric_report.get_weekly_cumulative_reach_measurement(
              edp_combination, period
          ).value
          for edp in edps:
            smaller_sum += child_metric_report.get_weekly_cumulative_reach_measurement(
                edp, period
            ).value
            larger_sum += child_metric_report.get_weekly_cumulative_reach_measurement(
                edp_combination, period
            ).value
          if not fuzzy_less_equal(smaller_sum, larger_sum, 2 * (
              1 + len(edps)) * tolerance):
            logging.info(
                f'Overlap constraint check for cumulative measurements across '
                f'metric {parent_metric}/{child_metric} is not satisfied for '
                f'{edp_combination}. At the {period}-th period, the smaller sum'
                f' {smaller_sum} is larger than the larger sum {larger_sum}.'
            )
            is_consistent = False

      # Verifies whole campaign measurements.
      common_whole_campaign_edp_combinations = \
        parent_metric_report.get_whole_campaign_reach_edp_combinations().intersection(
            child_metric_report.get_whole_campaign_reach_edp_combinations())
      for edp_combination in common_whole_campaign_edp_combinations:
        if len(edp_combination) <= 1:
          continue

        edps: list[FrozenSet[str]] = get_edps_from_edp_combination(
            edp_combination, common_whole_campaign_edp_combinations
        )
        if len(edps) != len(edp_combination):
          continue
        # Let X = {X_i} be the child whole campaign measurements and Y = {Y_i}
        # be the parent whole campaign measurements. As X_i is a subset of Y_i
        # for i in [0, k], we verify:
        # |X_1| + ... + |X_k| - |X_1 U ... U X_k| <=
        # |Y_1| + ... + |Y_k| - |Y_1 U ... U Y_k|.
        smaller_sum = - child_metric_report.get_whole_campaign_reach_measurement(
            edp_combination
        ).value
        larger_sum = - parent_metric_report.get_whole_campaign_reach_measurement(
            edp_combination
        ).value
        for edp in edps:
          smaller_sum += child_metric_report.get_whole_campaign_reach_measurement(
              edp).value
          larger_sum += parent_metric_report.get_whole_campaign_reach_measurement(
              edp).value

        if not fuzzy_less_equal(smaller_sum, larger_sum, 2 * (
            1 + len(edps)) * tolerance):
          logging.info(
              f'Overlap constraint check for whole campaign measurements across'
              f' metric {parent_metric}/{child_metric} is not satisfied for '
              f'{edp_combination}. The smaller sum {smaller_sum} is larger than'
              f' the larger sum {larger_sum}.'
          )
          is_consistent = False

  return is_consistent


def get_sorted_list(lst):
  sorted_list = []
  for item in lst:
    if isinstance(item, list):
      sorted_list.append(tuple(sorted(get_sorted_list(item))))
    else:
      sorted_list.append(item)
  return sorted(sorted_list)


def ordered_sets_to_sorted_list(ordered_sets: list[OrderedSets]):
  ordered_sets_list = []
  for ordered_pair in ordered_sets:
    ordered_sets_list.append(
        [list(ordered_pair.larger_set), list(ordered_pair.smaller_set)])
  return get_sorted_list(ordered_sets_list)

