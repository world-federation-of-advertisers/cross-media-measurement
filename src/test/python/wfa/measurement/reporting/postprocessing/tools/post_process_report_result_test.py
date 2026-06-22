# Copyright 2025 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from unittest.mock import MagicMock
from unittest.mock import patch
from unittest.mock import ANY

from src.main.python.wfa.measurement.reporting.postprocessing.tools.post_process_report_result import (
    PostProcessReportResult, )
from src.main.python.wfa.measurement.reporting.postprocessing.tools.post_process_report_result import compute_basic_metric_set
from wfa.measurement.internal.reporting.postprocessing import (
    report_post_processor_result_pb2, )

from google.protobuf import text_format
from wfa.measurement.internal.reporting.v2 import report_result_pb2
from wfa.measurement.internal.reporting.v2 import report_results_service_pb2
from wfa.measurement.internal.reporting.v2 import report_results_service_pb2_grpc
from wfa.measurement.internal.reporting.v2 import reporting_set_pb2
from wfa.measurement.internal.reporting.v2 import reporting_sets_service_pb2
from wfa.measurement.internal.reporting.v2 import reporting_sets_service_pb2_grpc
from wfa.measurement.internal.reporting.v2 import result_group_pb2

BasicMetricSet = result_group_pb2.ResultGroup.MetricSet.BasicMetricSet
ReportingSet = reporting_set_pb2.ReportingSet


class PostProcessReportResultTest(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.mock_report_results_stub = MagicMock()
        self.mock_reporting_sets_stub = MagicMock()
        self.cmms_measurement_consumer_id = "abcd"
        self.external_report_result_id = 123456

        with open(
                'src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_result.textproto',
                'r') as file:
            list_response_textproto = file.read()
            self.mock_list_reporing_set_results_response = text_format.Parse(
                list_response_textproto,
                report_results_service_pb2.ListReportingSetResultsResponse(),
            )

        with open(
                'src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_reporting_sets.textproto',
                'r') as files:
            reporting_sets_textproto = files.read()
            self.mock_batch_get_reporting_set_response = text_format.Parse(
                reporting_sets_textproto,
                reporting_sets_service_pb2.BatchGetReportingSetsResponse(),
            )

    def test_compute_basic_metric_set_no_population_raise_error(self):
        with self.assertRaisesRegex(ValueError,
                                    "Population must be a positive number."):
            compute_basic_metric_set(
                reach=50,
                frequency_values=[20, 10, 15, 5, 0],
                impressions=200,
                population=0,
            )

    def test_compute_basic_metric_set_no_metric(self):
        basic_metric_set = compute_basic_metric_set(reach=None,
                                                    impressions=None,
                                                    frequency_values=None,
                                                    population=100)
        expected_basic_metric_set = BasicMetricSet()
        self.assertEqual(basic_metric_set, expected_basic_metric_set)

    def test_compute_basic_metric_set_reach_only(self):
        basic_metric_set = compute_basic_metric_set(reach=500,
                                                    frequency_values=None,
                                                    impressions=None,
                                                    population=1000)
        self.assertEqual(basic_metric_set.reach, 500)
        self.assertEqual(basic_metric_set.percent_reach, 50.0)
        self.assertEqual(basic_metric_set.impressions, 0)
        self.assertEqual(basic_metric_set.grps, 0.0)
        self.assertEqual(basic_metric_set.average_frequency, 0.0)
        self.assertEqual(len(basic_metric_set.k_plus_reach), 0)
        self.assertEqual(len(basic_metric_set.percent_k_plus_reach), 0)

    def test_compute_basic_metric_set_impressions_only(self):
        basic_metric_set = compute_basic_metric_set(
            reach=None,
            frequency_values=None,
            impressions=200,
            population=100,
        )

        self.assertEqual(basic_metric_set.reach, 0)
        self.assertEqual(basic_metric_set.percent_reach, 0.0)
        self.assertEqual(basic_metric_set.impressions, 200)
        self.assertEqual(basic_metric_set.grps, 200.0)
        self.assertEqual(basic_metric_set.average_frequency, 0.0)
        self.assertEqual(len(basic_metric_set.k_plus_reach), 0)
        self.assertEqual(len(basic_metric_set.percent_k_plus_reach), 0)

    def test_compute_basic_metric_set_frequency_only(self):
        basic_metric_set = compute_basic_metric_set(
            reach=None,
            frequency_values=[10, 20, 15, 5, 0],
            impressions=None,
            population=100,
        )

        self.assertEqual(basic_metric_set.reach, 0)
        self.assertEqual(basic_metric_set.percent_reach, 0.0)
        self.assertEqual(basic_metric_set.impressions, 0)
        self.assertEqual(basic_metric_set.grps, 0.0)
        self.assertEqual(basic_metric_set.average_frequency, 0.0)
        self.assertEqual(list(basic_metric_set.k_plus_reach),
                         [50, 40, 20, 5, 0])
        self.assertEqual(list(basic_metric_set.percent_k_plus_reach),
                         [50.0, 40.0, 20.0, 5.0, 0.0])

    def test_compute_basic_metric_set_without_reach(self):
        basic_metric_set = compute_basic_metric_set(
            reach=None,
            frequency_values=[10, 20, 15, 5, 0],
            impressions=200,
            population=100,
        )

        self.assertEqual(basic_metric_set.reach, 0)
        self.assertEqual(basic_metric_set.percent_reach, 0.0)
        self.assertEqual(basic_metric_set.impressions, 200)
        self.assertEqual(basic_metric_set.grps, 200.0)
        self.assertEqual(basic_metric_set.average_frequency, 0.0)
        self.assertEqual(list(basic_metric_set.k_plus_reach),
                         [50, 40, 20, 5, 0])
        self.assertEqual(list(basic_metric_set.percent_k_plus_reach),
                         [50.0, 40.0, 20.0, 5.0, 0.0])

    def test_compute_basic_metric_set_without_frequency(self):
        basic_metric_set = compute_basic_metric_set(
            reach=50,
            frequency_values=None,
            impressions=200,
            population=100,
        )

        self.assertEqual(basic_metric_set.reach, 50)
        self.assertEqual(basic_metric_set.percent_reach, 50.0)
        self.assertEqual(basic_metric_set.impressions, 200)
        self.assertEqual(basic_metric_set.grps, 200.0)
        self.assertEqual(basic_metric_set.average_frequency, 4.0)
        self.assertEqual(len(basic_metric_set.k_plus_reach), 0)
        self.assertEqual(len(basic_metric_set.percent_k_plus_reach), 0)

    def test_compute_basic_metric_set_without_impressions(self):
        basic_metric_set = compute_basic_metric_set(
            reach=50,
            frequency_values=[10, 20, 15, 5, 0],
            impressions=None,
            population=100,
        )

        self.assertEqual(basic_metric_set.reach, 50)
        self.assertEqual(basic_metric_set.percent_reach, 50.0)
        self.assertEqual(basic_metric_set.impressions, 0)
        self.assertEqual(basic_metric_set.average_frequency, 0.0)
        self.assertEqual(basic_metric_set.grps, 0.0)
        self.assertEqual(list(basic_metric_set.k_plus_reach),
                         [50, 40, 20, 5, 0])
        self.assertEqual(list(basic_metric_set.percent_k_plus_reach),
                         [50.0, 40.0, 20.0, 5.0, 0.0])

    def test_compute_basic_metric_set_all_metrics(self):
        basic_metric_set = compute_basic_metric_set(
            reach=50,
            frequency_values=[10, 20, 15, 5, 0],
            impressions=200,
            population=100,
        )

        self.assertEqual(basic_metric_set.reach, 50)
        self.assertEqual(basic_metric_set.percent_reach, 50.0)
        self.assertEqual(basic_metric_set.impressions, 200)
        self.assertEqual(basic_metric_set.grps, 200.0)
        self.assertEqual(basic_metric_set.average_frequency, 4.0)
        self.assertEqual(list(basic_metric_set.k_plus_reach),
                         [50, 40, 20, 5, 0])
        self.assertEqual(list(basic_metric_set.percent_k_plus_reach),
                         [50.0, 40.0, 20.0, 5.0, 0.0])

    def test_post_process_report_result_success(self):
        # Configures the mock stubs to return the data from the textproto files.
        self.mock_report_results_stub.ListReportingSetResults.return_value = (
            self.mock_list_reporing_set_results_response)
        self.mock_reporting_sets_stub.BatchGetReportingSets.return_value = (
            self.mock_batch_get_reporting_set_response)

        report_result_processor = PostProcessReportResult(
            self.mock_report_results_stub, self.mock_reporting_sets_stub)

        add_processed_result_value_request = report_result_processor.process(
            self.cmms_measurement_consumer_id, self.external_report_result_id,
            [])

        # Verifies the gRPC stubs were called correctly.
        self.mock_report_results_stub.ListReportingSetResults.assert_called_once_with(
            report_results_service_pb2.ListReportingSetResultsRequest(
                cmms_measurement_consumer_id=self.cmms_measurement_consumer_id,
                external_report_result_id=self.external_report_result_id,
                view=report_result_pb2.ReportingSetResultView.
                REPORTING_SET_RESULT_VIEW_UNPROCESSED,
            ))

        self.mock_reporting_sets_stub.BatchGetReportingSets.assert_called_once(
        )
        batch_get_request = self.mock_reporting_sets_stub.BatchGetReportingSets.call_args[
            0][0]
        self.assertEqual(batch_get_request.cmms_measurement_consumer_id,
                         self.cmms_measurement_consumer_id)
        self.assertCountEqual(
            batch_get_request.external_reporting_set_ids,
            [
                'reporting_set_id_edp1',
                'reporting_set_id_edp2',
                'reporting_set_id_edp3',
                'reporting_set_id_edp1_edp2',
                'reporting_set_id_edp1_edp2_edp3',
            ],
        )

        # Verifies the AddProcessedResultValuesRequest.
        self.assertIsNotNone(add_processed_result_value_request)
        self.assertEqual(
            add_processed_result_value_request.cmms_measurement_consumer_id,
            self.cmms_measurement_consumer_id,
        )
        self.assertEqual(
            add_processed_result_value_request.external_report_result_id,
            self.external_report_result_id,
        )

        # Verifies that there are 25 reporting set results in the sample data.
        self.assertEqual(
            len(add_processed_result_value_request.reporting_set_results), 25)

        # Verifies the set result with exteral_reporting_set_result_id = 25.
        # This set result has noise added.
        processed_set_result_25 = add_processed_result_value_request.reporting_set_results[
            25]
        self.assertEqual(len(processed_set_result_25.reporting_window_results),
                         1)

        window_result = processed_set_result_25.reporting_window_results[0]

        # Verifies the window key.
        self.assertEqual(window_result.key.non_cumulative_start.year, 2025)
        self.assertEqual(window_result.key.non_cumulative_start.month, 10)
        self.assertEqual(window_result.key.non_cumulative_start.day, 1)
        self.assertEqual(window_result.key.end.year, 2025)
        self.assertEqual(window_result.key.end.month, 10)
        self.assertEqual(window_result.key.end.day, 15)

        # Verifies the cumulative results.
        cumulative_results = window_result.value.cumulative_results
        self.assertEqual(cumulative_results.reach, 19021120)
        self.assertAlmostEqual(cumulative_results.percent_reach, 34.5838547)
        self.assertAlmostEqual(cumulative_results.average_frequency,
                               1.88917184)
        self.assertEqual(cumulative_results.impressions, 35934165)
        self.assertAlmostEqual(cumulative_results.grps, 65.3348465)
        expected_k_plus_reach = [
            19021120,
            9200728,
            4291494,
            1837838,
            611971,
        ]
        self.assertCountEqual(cumulative_results.k_plus_reach,
                              expected_k_plus_reach)
        expected_percent_k_plus_reach = [
            34.5838547,
            16.7285957,
            7.80271626,
            3.34152365,
            1.11267459,
        ]
        self.assertEqual(len(cumulative_results.percent_k_plus_reach),
                         len(expected_percent_k_plus_reach))
        for actual, expected in zip(cumulative_results.percent_k_plus_reach,
                                    expected_percent_k_plus_reach):
            self.assertAlmostEqual(actual, expected)

        # Verifies the set result with exteral_reporting_set_result_id = 22.
        # This set result does not have noise added and has multiple reporting
        # windows.
        processed_set_result_22 = add_processed_result_value_request.reporting_set_results[
            22]
        self.assertEqual(len(processed_set_result_22.reporting_window_results),
                         2)

        # Sorts windows by end date to ensure consistent order for assertions.
        sorted_windows = sorted(
            processed_set_result_22.reporting_window_results,
            key=lambda x: x.key.end.day)

        # Verifies the first window (ending 2025-10-08).
        window_1 = sorted_windows[0]
        self.assertEqual(window_1.key.end.day, 8)

        # Verifies the first cumulative results for window 1.
        self.assertEqual(window_1.value.cumulative_results.reach, 800000)
        self.assertAlmostEqual(window_1.value.cumulative_results.percent_reach,
                               1.45454546)

        # Verifies the non-cumulative results for window 1.
        non_cumulative_1 = window_1.value.non_cumulative_results
        self.assertEqual(non_cumulative_1.reach, 800000)
        self.assertAlmostEqual(non_cumulative_1.percent_reach, 1.45454546)
        self.assertAlmostEqual(non_cumulative_1.average_frequency, 1.83870876)
        self.assertEqual(non_cumulative_1.impressions, 1470967)
        self.assertAlmostEqual(non_cumulative_1.grps, 2.67448545)

        expected_k_plus_reach_1 = [800000, 387097, 180645, 77419, 25806]
        self.assertCountEqual(non_cumulative_1.k_plus_reach,
                              expected_k_plus_reach_1)

        expected_percent_k_plus_reach_1 = [
            1.4545455, 0.703812718, 0.328445464, 0.140761822, 0.04692
        ]
        self.assertEqual(len(non_cumulative_1.percent_k_plus_reach),
                         len(expected_percent_k_plus_reach_1))
        for actual, expected in zip(non_cumulative_1.percent_k_plus_reach,
                                    expected_percent_k_plus_reach_1):
            self.assertAlmostEqual(actual, expected)

        # Verifies the second window (ending 2025-10-15).
        window_2 = sorted_windows[1]
        self.assertEqual(window_2.key.end.day, 15)

        # Verifies the cumulative results for window 2.
        self.assertEqual(window_2.value.cumulative_results.reach, 1000000)
        self.assertAlmostEqual(window_2.value.cumulative_results.percent_reach,
                               1.81818187)

        # Verifies the non-cumulative results for window 2.
        non_cumulative_2 = window_2.value.non_cumulative_results
        self.assertEqual(non_cumulative_2.reach, 202952)
        self.assertAlmostEqual(non_cumulative_2.percent_reach, 0.369003648)
        self.assertAlmostEqual(non_cumulative_2.average_frequency, 1.83870566)
        self.assertEqual(non_cumulative_2.impressions, 373169)
        self.assertAlmostEqual(non_cumulative_2.grps, 0.678489078)

        expected_k_plus_reach_2 = [202952, 98203, 45828, 19640, 6546]
        self.assertCountEqual(non_cumulative_2.k_plus_reach,
                              expected_k_plus_reach_2)

        expected_percent_k_plus_reach_2 = [
            0.369003624, 0.178550914, 0.0833236352, 0.0357090905, 0.0119018182
        ]
        self.assertEqual(len(non_cumulative_2.percent_k_plus_reach),
                         len(expected_percent_k_plus_reach_2))
        for actual, expected in zip(non_cumulative_2.percent_k_plus_reach,
                                    expected_percent_k_plus_reach_2):
            self.assertAlmostEqual(actual, expected)

    @patch('src.main.python.wfa.measurement.reporting.postprocessing.tools.post_process_report_result.ReportSummaryV2Processor')
    def test_post_process_report_result_with_exempted_edps_passed_to_processor(self, mock_processor_class):
        # Configures the mock stubs to return the data from the textproto files.
        self.mock_report_results_stub.ListReportingSetResults.return_value = (
            self.mock_list_reporing_set_results_response)
        self.mock_reporting_sets_stub.BatchGetReportingSets.return_value = (
            self.mock_batch_get_reporting_set_response)

        # Mock the processor instance and its process() return value.
        mock_processor_instance = MagicMock()
        mock_processor_class.return_value = mock_processor_instance

        mock_result = MagicMock()
        mock_result.status.status_code = report_post_processor_result_pb2.ReportPostProcessorStatus.SOLUTION_FOUND_WITH_HIGHS
        mock_result.updated_measurements = {}
        mock_processor_instance.process.return_value = mock_result

        report_result_processor = PostProcessReportResult(
            self.mock_report_results_stub, self.mock_reporting_sets_stub)
        exempted_edps = ['dataProviders/edp1']
        report_result_processor.process(
            self.cmms_measurement_consumer_id, self.external_report_result_id,
            exempted_edps)

        # Verify that ReportSummaryV2Processor was instantiated with the exempted reporting set ids
        mock_processor_class.assert_called_with(
            ANY,
            ['reporting_set_id_edp1']
        )

    def test_get_ami_mrc_exempted_reporting_set_id(self):
        self.mock_reporting_sets_stub.BatchGetReportingSets.return_value = (
            self.mock_batch_get_reporting_set_response)

        report_result_processor = PostProcessReportResult(
            self.mock_report_results_stub, self.mock_reporting_sets_stub)

        # Case 1: Empty inputs
        self.assertEqual(
            report_result_processor._get_ami_mrc_exempted_reporting_set_id(
                self.cmms_measurement_consumer_id, [], ['dataProviders/edp1']),
            [],
        )
        self.assertEqual(
            report_result_processor._get_ami_mrc_exempted_reporting_set_id(
                self.cmms_measurement_consumer_id, ['reporting_set_id_edp1'], []),
            [],
        )

        # Case 2: No match
        exempted_ids = report_result_processor._get_ami_mrc_exempted_reporting_set_id(
            self.cmms_measurement_consumer_id,
            [
                'reporting_set_id_edp1',
                'reporting_set_id_edp2',
                'reporting_set_id_edp1_edp2',
            ],
            ['dataProviders/edp5'],
        )
        self.assertEqual(
            exempted_ids,
            [],
        )

        # Case 3: Match with EDP name
        exempted_ids = report_result_processor._get_ami_mrc_exempted_reporting_set_id(
            self.cmms_measurement_consumer_id,
            [
                'reporting_set_id_edp1',
                'reporting_set_id_edp2',
                'reporting_set_id_edp1_edp2',
            ],
            ['dataProviders/edp1'],
        )
        self.assertCountEqual(
            exempted_ids,
            ['reporting_set_id_edp1'],
        )

    def _make_reporting_set_result(
        self,
        external_reporting_set_result_id: int,
        external_reporting_set_id: str,
        selector: str,
    ) -> report_result_pb2.ReportingSetResult:
        rsr = report_result_pb2.ReportingSetResult(
            cmms_measurement_consumer_id=self.cmms_measurement_consumer_id,
            external_report_result_id=self.external_report_result_id,
            external_reporting_set_result_id=external_reporting_set_result_id,
        )
        rsr.dimension.external_reporting_set_id = external_reporting_set_id
        rsr.dimension.venn_diagram_region_type = (
            report_result_pb2.ReportingSetResult.Dimension.UNION)
        rsr.dimension.custom = True
        if selector == 'total':
            rsr.dimension.metric_frequency_spec.total = True
        elif selector == 'weekly':
            from google.type import dayofweek_pb2
            rsr.dimension.metric_frequency_spec.weekly = (
                dayofweek_pb2.DayOfWeek.MONDAY)
        return rsr

    def _set_window_reach(
        self,
        request: report_results_service_pb2.AddProcessedResultValuesRequest,
        external_reporting_set_result_id: int,
        end_day: int,
        reach: int,
        k_plus_reach: list[int] | None = None,
    ) -> None:
        processed = request.reporting_set_results[
            external_reporting_set_result_id]
        entry = processed.reporting_window_results.add()
        entry.key.end.year = 2025
        entry.key.end.month = 10
        entry.key.end.day = end_day
        entry.value.cumulative_results.reach = reach
        if k_plus_reach is not None:
            entry.value.cumulative_results.k_plus_reach.extend(k_plus_reach)

    def test_reconcile_snaps_both_sides_to_min_reach(self):
        """Both whole_campaign and last_weekly_cumulative get the smaller of
        the two reach values. Picking the min avoids re-breaking the per-RSR
        identity sum(k_plus_reach) <= impressions on the side that would have
        been snapped upward (Issue #4049 Rule 4)."""
        processor = PostProcessReportResult(self.mock_report_results_stub,
                                            self.mock_reporting_sets_stub)
        reporting_set_results = [
            self._make_reporting_set_result(1, 'reporting_set_id_edp1',
                                            'total'),
            self._make_reporting_set_result(2, 'reporting_set_id_edp1',
                                            'weekly'),
        ]
        request = (
            report_results_service_pb2.AddProcessedResultValuesRequest())
        # whole_campaign rounds to one value, last weekly cumulative rounds to
        # another -- this is the cross-RSR drift the reconciler fixes.
        self._set_window_reach(
            request, 1, end_day=15, reach=1003,
            k_plus_reach=[1003, 500, 250, 100, 25])
        self._set_window_reach(request, 2, end_day=8, reach=600)
        self._set_window_reach(
            request, 2, end_day=15, reach=1000,
            k_plus_reach=[1000, 500, 250, 100, 25])

        processor._reconcile_cross_window_identities(
            request, reporting_set_results)

        whole_camp = request.reporting_set_results[
            1].reporting_window_results[0]
        last_weekly = next(w for w in request.reporting_set_results[
            2].reporting_window_results if w.key.end.day == 15)
        # Both sides snapped to min(1003, 1000) = 1000.
        self.assertEqual(whole_camp.value.cumulative_results.reach, 1000)
        self.assertEqual(whole_camp.value.cumulative_results.k_plus_reach[0],
                         1000)
        self.assertEqual(last_weekly.value.cumulative_results.reach, 1000)
        self.assertEqual(
            last_weekly.value.cumulative_results.k_plus_reach[0], 1000)

    def test_reconcile_snaps_down_when_whole_campaign_is_smaller(self):
        """When whole_campaign.reach < last_weekly.reach, last_weekly is
        snapped down (rather than whole_campaign snapped up). This direction
        prevents re-breaking the impressions identity for last_weekly."""
        processor = PostProcessReportResult(self.mock_report_results_stub,
                                            self.mock_reporting_sets_stub)
        reporting_set_results = [
            self._make_reporting_set_result(1, 'reporting_set_id_edp1',
                                            'total'),
            self._make_reporting_set_result(2, 'reporting_set_id_edp1',
                                            'weekly'),
        ]
        request = (
            report_results_service_pb2.AddProcessedResultValuesRequest())
        self._set_window_reach(
            request, 1, end_day=15, reach=1000,
            k_plus_reach=[1000, 500, 250, 100, 25])
        self._set_window_reach(
            request, 2, end_day=15, reach=1003,
            k_plus_reach=[1003, 500, 250, 100, 25])

        processor._reconcile_cross_window_identities(
            request, reporting_set_results)

        whole_camp = request.reporting_set_results[
            1].reporting_window_results[0]
        last_weekly = request.reporting_set_results[
            2].reporting_window_results[0]
        # Both sides snapped to min(1000, 1003) = 1000.
        self.assertEqual(whole_camp.value.cumulative_results.reach, 1000)
        self.assertEqual(last_weekly.value.cumulative_results.reach, 1000)
        self.assertEqual(
            last_weekly.value.cumulative_results.k_plus_reach[0], 1000)

    def test_reconcile_picks_latest_weekly_window_by_end_date(self):
        processor = PostProcessReportResult(self.mock_report_results_stub,
                                            self.mock_reporting_sets_stub)
        reporting_set_results = [
            self._make_reporting_set_result(1, 'reporting_set_id_edp1',
                                            'total'),
            self._make_reporting_set_result(2, 'reporting_set_id_edp1',
                                            'weekly'),
        ]
        request = (
            report_results_service_pb2.AddProcessedResultValuesRequest())
        self._set_window_reach(request, 1, end_day=15, reach=9999)
        # Add weekly windows in non-sorted order to verify date-max selection.
        self._set_window_reach(request, 2, end_day=15, reach=1234)
        self._set_window_reach(request, 2, end_day=1, reach=100)
        self._set_window_reach(request, 2, end_day=8, reach=500)

        processor._reconcile_cross_window_identities(
            request, reporting_set_results)

        whole_camp = request.reporting_set_results[
            1].reporting_window_results[0]
        # Snap to min(9999, 1234) = 1234. The end_day=15 weekly is the latest,
        # not end_day=1 or end_day=8.
        self.assertEqual(whole_camp.value.cumulative_results.reach, 1234)
        # Earlier weekly windows are unchanged.
        for w in request.reporting_set_results[2].reporting_window_results:
            if w.key.end.day == 1:
                self.assertEqual(w.value.cumulative_results.reach, 100)
            elif w.key.end.day == 8:
                self.assertEqual(w.value.cumulative_results.reach, 500)
            elif w.key.end.day == 15:
                self.assertEqual(w.value.cumulative_results.reach, 1234)

    def test_reconcile_skips_when_no_matching_weekly(self):
        processor = PostProcessReportResult(self.mock_report_results_stub,
                                            self.mock_reporting_sets_stub)
        reporting_set_results = [
            self._make_reporting_set_result(1, 'reporting_set_id_edp1',
                                            'total'),
        ]
        request = (
            report_results_service_pb2.AddProcessedResultValuesRequest())
        self._set_window_reach(request, 1, end_day=15, reach=1003)

        processor._reconcile_cross_window_identities(
            request, reporting_set_results)

        unchanged = request.reporting_set_results[1].reporting_window_results[
            0]
        self.assertEqual(unchanged.value.cumulative_results.reach, 1003)

    def test_reconcile_skips_when_dimensions_differ(self):
        processor = PostProcessReportResult(self.mock_report_results_stub,
                                            self.mock_reporting_sets_stub)
        reporting_set_results = [
            self._make_reporting_set_result(1, 'reporting_set_id_edp1',
                                            'total'),
            self._make_reporting_set_result(2, 'reporting_set_id_edp2',
                                            'weekly'),
        ]
        request = (
            report_results_service_pb2.AddProcessedResultValuesRequest())
        self._set_window_reach(request, 1, end_day=15, reach=1003)
        self._set_window_reach(request, 2, end_day=15, reach=2000)

        processor._reconcile_cross_window_identities(
            request, reporting_set_results)

        unchanged = request.reporting_set_results[1].reporting_window_results[
            0]
        self.assertEqual(unchanged.value.cumulative_results.reach, 1003)

    def test_reconcile_handles_empty_k_plus_reach(self):
        processor = PostProcessReportResult(self.mock_report_results_stub,
                                            self.mock_reporting_sets_stub)
        reporting_set_results = [
            self._make_reporting_set_result(1, 'reporting_set_id_edp1',
                                            'total'),
            self._make_reporting_set_result(2, 'reporting_set_id_edp1',
                                            'weekly'),
        ]
        request = (
            report_results_service_pb2.AddProcessedResultValuesRequest())
        # No k_plus_reach on either side.
        self._set_window_reach(request, 1, end_day=15, reach=1003)
        self._set_window_reach(request, 2, end_day=15, reach=1000)

        processor._reconcile_cross_window_identities(
            request, reporting_set_results)

        whole_camp = request.reporting_set_results[
            1].reporting_window_results[0]
        last_weekly = request.reporting_set_results[
            2].reporting_window_results[0]
        self.assertEqual(whole_camp.value.cumulative_results.reach, 1000)
        self.assertEqual(last_weekly.value.cumulative_results.reach, 1000)
        # Empty k_plus_reach on both sides remains empty -- no spurious
        # element added.
        self.assertEqual(
            list(whole_camp.value.cumulative_results.k_plus_reach), [])
        self.assertEqual(
            list(last_weekly.value.cumulative_results.k_plus_reach), [])


if __name__ == "__main__":
    unittest.main()
