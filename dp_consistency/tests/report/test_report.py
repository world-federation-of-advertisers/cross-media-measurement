from unittest import TestCase

from src.noiseninja.noised_measurements import Measurement
from src.report.report import Report, MetricReport

EXPECTED_PRECISION = 4


class TestReport(TestCase):

    def test_get_corrected_single_metric_report(self):
        ami = 'ami'
        report = Report(
            {ami: MetricReport(
                total_campaign_reach_time_series=[Measurement(50, 1)],
                reach_time_series_by_edp=[
                    [Measurement(48, 0)], [Measurement(1, 1)]])}, {})

        corrected = report.get_corrected_report()

        expected = Report(
            {ami: MetricReport(
                total_campaign_reach_time_series=[Measurement(49.5, 1)],
                reach_time_series_by_edp=[
                    [Measurement(48, 0)], [Measurement(1.5, 1)]])}, {})

        self.__assertReportsAlmostEqual(expected, corrected,
                                        corrected.to_array())

    def test_can_correct_time_series(self):
        ami = 'ami'
        report = Report({ami: MetricReport(
            total_campaign_reach_time_series=[Measurement(0.00, 1),
                                              Measurement(3.30, 1),
                                              Measurement(0.00, 1)],
            reach_time_series_by_edp=[
                [Measurement(0.00, 1),
                 Measurement(3.30, 1),
                 Measurement(0.00, 1)]])}, {})

        corrected = report.get_corrected_report()

        expected = Report({ami: MetricReport(
            total_campaign_reach_time_series=[Measurement(0.00, 1),
                                              Measurement(1.65, 1),
                                              Measurement(1.65, 1)],
            reach_time_series_by_edp=[
                [Measurement(0.00, 1),
                 Measurement(1.65, 1),
                 Measurement(1.65, 1)]])}, {})

        self.__assertReportsAlmostEqual(expected, corrected,
                                        corrected.to_array())

    def test_can_correct_related_metrics(self):
        ami = 'ami'
        mrc = 'mrc'
        report = Report(
            {ami: MetricReport(
                total_campaign_reach_time_series=[Measurement(51, 1)],
                reach_time_series_by_edp=[
                    [Measurement(50, 1)]]),

                mrc: MetricReport(
                    total_campaign_reach_time_series=[Measurement(52, 1)],
                    reach_time_series_by_edp=[
                        [Measurement(51, 1)]])},
            # AMI is a parent of MRC
            {ami: [mrc]})

        corrected = report.get_corrected_report()

        expected = Report(
            {ami: MetricReport(
                total_campaign_reach_time_series=[Measurement(51, 1)],
                reach_time_series_by_edp=[
                    [Measurement(51, 1)]]),

                mrc: MetricReport(
                    total_campaign_reach_time_series=[Measurement(51, 1)],
                    reach_time_series_by_edp=[
                        [Measurement(51, 1)]])},
            # AMI is a parent of MRC
            {ami: [mrc]})

        self.__assertReportsAlmostEqual(expected, corrected,
                                        corrected.to_array())

    def __assertMeasurementAlmostEquals(self, expected: Measurement,
                                        actual: Measurement, msg):
        if expected.sigma == 0:
            self.assertAlmostEqual(expected.value, actual.value, msg=msg)
        else:
            self.assertAlmostEqual(expected.value, actual.value,
                                   places=EXPECTED_PRECISION, msg=msg)

    def __assertMetricReportsAlmostEqual(self, expected: MetricReport,
                                         actual: MetricReport, msg):
        self.assertEqual(expected.get_num_edps(), actual.get_num_edps())
        self.assertEqual(expected.get_number_of_periods(),
                         actual.get_number_of_periods())
        for period in range(0, expected.get_number_of_periods()):
            self.__assertMeasurementAlmostEquals(
                expected.get_total_reach_measurement(period),
                actual.get_total_reach_measurement(period), msg)
            for edp in range(0, expected.get_num_edps()):
                self.__assertMeasurementAlmostEquals(
                    expected.get_edp_measurement(edp, period),
                    actual.get_edp_measurement(edp, period), msg)

    def __assertReportsAlmostEqual(self, expected: Report, actual: Report, msg):
        self.assertEqual(expected.get_metrics(), actual.get_metrics())
        for metric in expected.get_metrics():
            self.__assertMetricReportsAlmostEqual(
                expected.get_metric_report(metric),
                actual.get_metric_report(metric), msg)
