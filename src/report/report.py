import random

import numpy as np

from src.noiseninja.noised_measurements import SetMeasurementsSpec, Measurement
from src.noiseninja.solver import Solver


class MetricReport:
    """Represents a metric sub-report view (e.g. MRC, AMI, etc)
     within a report.
     """
    __reach_time_series_by_edp: list[list[Measurement]]

    __total_campaign_reach_time_series: list[Measurement]

    def __init__(self, total_campaign_reach_time_series: list[Measurement],
                 reach_time_series_by_edp: list[list[Measurement]]):

        for series in reach_time_series_by_edp:
            if len(series) != len(total_campaign_reach_time_series):
                raise ValueError(
                    'all time series must have the same length {1: d} vs {2: d}'
                    .format(len(series), len(total_campaign_reach_time_series)))

        self.__total_campaign_reach_time_series = (
            total_campaign_reach_time_series)
        self.__reach_time_series_by_edp = reach_time_series_by_edp

    def sample_with_noise(self):
        """
        :return: a new MetricReport where measurements have been resampled
        according to their mean and variance.
        """
        return MetricReport(
            total_campaign_reach_time_series=list(
                MetricReport.__sample_with_noise(measurement) for measurement in
                self.__total_campaign_reach_time_series),
            reach_time_series_by_edp=list(
                list(MetricReport.__sample_with_noise(measurement)
                     for measurement in series)
                for series in self.__reach_time_series_by_edp))

    def get_edp_measurement(self, edp: int, period: int):
        return self.__reach_time_series_by_edp[edp][period]

    def get_total_reach_measurement(self, period: int):
        return self.__total_campaign_reach_time_series[period]

    def get_num_edps(self):
        return len(self.__reach_time_series_by_edp)

    def get_number_of_periods(self):
        return len(self.__total_campaign_reach_time_series)

    @staticmethod
    def __sample_with_noise(measurement: Measurement):
        return Measurement(
            measurement.value + random.gauss(0, measurement.sigma),
            measurement.sigma)


class Report:
    """
    Represents a full report, consisting of multiple MetricReports,
    which may have set relationships between each other.
    """
    __metric_reports: dict[str, MetricReport]
    __metric_subsets_by_parent: dict[str, list[str]]
    __metric_index: dict[str, int]

    def __init__(self, metric_reports: dict[str, MetricReport],
                 metric_subsets_by_parent: dict[str, list[str]]):
        """
        Args:
            metric_reports: a dictionary mapping metric types to a MetricReport
            metric_subsets_by_parent: a dictionary containing subset
                relationship between the metrics. .e.g. ami >= [custom, mrc]
        """
        self.__metric_reports = metric_reports
        self.__metric_subsets_by_parent = metric_subsets_by_parent

        # all metrics in the set relationships must have a corresponding report.
        for parent in metric_subsets_by_parent.keys():
            if not (parent in metric_reports):
                raise ValueError('key {1} does not have a corresponding report'
                                 .format(parent))
            for child in metric_subsets_by_parent[parent]:
                if not (child in metric_reports):
                    raise ValueError(
                        'key {1} does not have a corresponding report'
                        .format(child))

        self.__metric_index = {}
        for index, metric in enumerate(metric_reports.keys()):
            self.__metric_index[metric] = index

        self.__num_edps = next(iter(metric_reports.values())).get_num_edps()
        self.__num_periods = next(
            iter(metric_reports.values())).get_number_of_periods()

        num_vars_per_period = (self.__num_edps + 1) * len(metric_reports.keys())
        self.__num_vars = self.__num_periods * num_vars_per_period

    def get_metric_report(self, metric: str) -> MetricReport:
        return self.__metric_reports[metric]

    def get_metrics(self) -> set[str]:
        return set(self.__metric_reports.keys())

    def get_corrected_report(self) -> 'Report':
        """Returns a corrected, consistent report.
        Note all measurements in the corrected report are set to have 0 variance
        """
        spec = self.__to_set_measurement_spec()
        solution = Solver.solve(spec)
        return Report(
            {metric: self.__metric_report_from_solution(metric, solution)
             for
             metric in self.__metric_reports}, self.__metric_subsets_by_parent)

    def sample_with_noise(self) -> 'Report':
        """Returns a new report sampled according to the mean and variance of
        all metrics in this report. Useful to bootstrap sample reports.
        """
        return Report(
            {i: self.__metric_reports[i].sample_with_noise()
             for i in self.__metric_reports},
            self.__metric_subsets_by_parent)

    def to_array(self) -> np.array:
        """Returns an array representation of all the mean measurement values
        in this report
        """
        array = np.zeros(self.__num_vars)
        for metric in self.__metric_reports:
            for period in range(0, self.__num_periods):
                for edp in range(0, self.__num_edps):
                    array.put(self.__get_var_index(period,
                                                   self.__metric_index[metric],
                                                   edp),
                              self.__metric_reports[metric].get_edp_measurement(
                                  edp, period).value)
                array.put(
                    self.__get_var_index(period, self.__metric_index[metric],
                                         self.__num_edps),
                    self.__metric_reports[
                        metric].get_total_reach_measurement(period).value)
        return array

    def __to_set_measurement_spec(self):
        spec = SetMeasurementsSpec()
        self.__add_measurements_to_spec(spec)
        self.__add_set_relations_to_spec(spec)
        return spec

    def __add_set_relations_to_spec(self, spec):
        # sum of subsets >= union for each period
        for period in range(0, self.__num_periods):
            for metric in range(0, len(self.__metric_index.keys())):
                spec.add_cover(
                    children=list(
                        self.__get_var_index(period, metric, edp) for edp in
                        range(0, self.__num_edps)),
                    parent=self.__get_var_index(period, metric,
                                                self.__num_edps))

            # subset <= union
            for metric in range(0, len(self.__metric_index.keys())):
                for edp in range(0, self.__num_edps):
                    spec.add_subset_relation(
                        child_set_id=self.__get_var_index(period, metric, edp),
                        parent_set_id=self.__get_var_index(period, metric,
                                                           self.__num_edps))

            # metric1>=metric#2
            for parent_metric in self.__metric_subsets_by_parent:
                for child_metric in (
                        self.__metric_subsets_by_parent[parent_metric]):
                    for edp in range(0, self.__num_edps + 1):
                        spec.add_subset_relation(
                            child_set_id=self.__get_var_index(
                                period, self.__metric_index[child_metric], edp),
                            parent_set_id=self.__get_var_index(
                                period,
                                self.__metric_index[parent_metric], edp))

            if period < self.__num_periods - 1:
                # period1 <= period2
                for metric in range(0, len(self.__metric_index.keys())):
                    for edp in range(0, self.__num_edps + 1):
                        spec.add_subset_relation(
                            child_set_id=self.__get_var_index(
                                period, metric, edp),
                            parent_set_id=self.__get_var_index(
                                period + 1, metric, edp))

    def __add_measurements_to_spec(self, spec):
        for metric in self.__metric_reports:
            for period in range(0, self.__num_periods):
                spec.add_measurement(
                    self.__get_var_index(period, self.__metric_index[metric],
                                         self.__num_edps),
                    self.__metric_reports[metric].get_total_reach_measurement(
                        period))
                for edp in range(0, self.__num_edps):
                    spec.add_measurement(
                        self.__get_var_index(period,
                                             self.__metric_index[metric], edp),
                        self.__metric_reports[metric].get_edp_measurement(
                            edp, period))

    def __get_var_index(self, period: int, metric: int, edp: int):
        return (metric * (self.__num_edps + 1) * self.__num_periods
                + edp * self.__num_periods + period)

    def __metric_report_from_solution(self, metric, solution):
        return MetricReport(
            total_campaign_reach_time_series=list(
                Measurement(
                    solution[self.__get_var_index(period,
                                                  self.__metric_index[
                                                      metric],
                                                  self.__num_edps)],
                    0)
                for period in range(0, self.__num_periods)),
            reach_time_series_by_edp=list(list(
                Measurement(solution[self.__get_var_index(period,
                                                          self.__metric_index[
                                                              metric], j)],
                            0)
                for period in range(0, self.__num_periods))
                                          for j in
                                          range(0, self.__num_edps)))
