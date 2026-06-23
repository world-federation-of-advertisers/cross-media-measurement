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

    def test_compute_basic_metric_set_k_plus_reach_zero_matches_reach(self):
        """k_plus_reach[0] is the "1+ reach" and must equal reach.

        When the post-processor solver returns reach and the frequency
        histogram as floats (the realistic case in production), independent
        round() calls on the two derivations can disagree by 1+. Consumers
        rely on k_plus_reach[0] == reach (Issue #4049).
        """
        # Floats chosen so round(reach) != round(sum(freqs)):
        # reach rounds to 2194000; sum(freqs) rounds to 2193997 (matches the
        # real-world example from Origin staging in #4049).
        basic_metric_set = compute_basic_metric_set(
            reach=2194000,
            frequency_values=[
                1222269.4, 299330.4, 183245.4, 85498.4, 45170.4,
                64182.4, 26965.4, 7445.4, 5345.4, 4608.4,
                4874.4, 6342.4, 6253.4, 51051.4, 181420.4,
            ],
            impressions=7313150,
            population=8045553,
        )

        self.assertEqual(basic_metric_set.k_plus_reach[0],
                         basic_metric_set.reach,
                         "k_plus_reach[0] (1+ reach) must equal reach")

    def test_compute_basic_metric_set_k_plus_buckets_sum_le_impressions(self):
        """The weighted sum of frequency-distribution buckets is conceptually
        the impression count. With independent rounding the weighted sum can
        exceed impressions by a small amount, breaking a validation rule
        post-processor consumers rely on (Issue #4049).
        """
        basic_metric_set = compute_basic_metric_set(
            reach=3946000,
            frequency_values=[
                1663825.4, 651217.4, 382519.4, 244746.4, 164390.4,
                124340.4, 105164.4, 74233.4, 30675.4, 37481.4,
                25755.4, 21514.4, 31622.4, 139317.4, 249203.4,
            ],
            impressions=15282723,
            population=8568303,
        )

        # k_plus_reach values are aggregated by frequency bucket k. The
        # impression count equals weighted sum: sum(freq[i] * (i+1)) for
        # buckets [0..max], or equivalently sum(k_plus_reach).
        weighted_sum_of_buckets = sum(basic_metric_set.k_plus_reach)
        self.assertLessEqual(
            weighted_sum_of_buckets, basic_metric_set.impressions,
            "sum of k_plus_reach buckets must not exceed impressions")

    def test_compute_basic_metric_set_k_plus_excess_cascades_across_buckets(
            self):
        """When the rounding residue exceeds the last k_plus_reach bucket, the
        snap cascades into the next-highest bucket until the impressions
        identity is satisfied. Without cascading, the violation would persist.
        """
        # frequency_values picked so the highest bucket can absorb only part of
        # the residue: k_plus_reach[-1] = 1 (= frequency_values[-1]) but
        # excess after capping impressions is 5, so 4 must cascade into the
        # next bucket.
        basic_metric_set = compute_basic_metric_set(
            reach=166,
            frequency_values=[100, 50, 10, 5, 1],
            impressions=250,
            population=1000,
        )

        weighted_sum_of_buckets = sum(basic_metric_set.k_plus_reach)
        self.assertLessEqual(
            weighted_sum_of_buckets, basic_metric_set.impressions,
            "sum of k_plus_reach buckets must not exceed impressions")
        # k_plus_reach[-1] absorbs as much as it can; remainder cascades.
        self.assertEqual(basic_metric_set.k_plus_reach[-1], 0)
        self.assertEqual(basic_metric_set.k_plus_reach[-2], 2)
        # Lower buckets are untouched.
        self.assertEqual(basic_metric_set.k_plus_reach[0], 166)
        self.assertEqual(basic_metric_set.k_plus_reach[1], 66)
        self.assertEqual(basic_metric_set.k_plus_reach[2], 16)

    def test_compute_basic_metric_set_k_plus_excess_preserves_reach_identity(
            self):
        """The cascade stops at index 1 so k_plus_reach[0] == reach holds
        even when the residue would otherwise consume the whole histogram.
        Identity #1 takes precedence over identity #2 in this (unrealistic)
        case; the residual is small in practice because large corrections
        are rejected upstream by the 7-sigma threshold.
        """
        with self.assertLogs(level='WARNING') as cm:
            basic_metric_set = compute_basic_metric_set(
                reach=10,
                frequency_values=[10, 5, 3, 2, 1],
                impressions=1,
                population=1000,
            )

        # All higher buckets pushed to zero, but k_plus_reach[0] is preserved.
        self.assertEqual(basic_metric_set.k_plus_reach[0], 10)
        for bucket in basic_metric_set.k_plus_reach[1:]:
            self.assertEqual(bucket, 0)
        # Warning surfaces the unresolved residue so operators notice the
        # upstream data inconsistency (reach > impressions).
        self.assertTrue(
            any('Could not fully reconcile' in m for m in cm.output),
            f"expected reconciliation warning, got: {cm.output}")

    def test_compute_basic_metric_set_single_bucket_excess_logs_warning(self):
        """When the histogram has a single bucket, the cascade has no
        higher-frequency bucket to absorb residue into. Identity 1
        (k_plus_reach[0] == reach) is preserved and the warning surfaces
        the unresolved residue (Issue #4049).
        """
        with self.assertLogs(level='WARNING') as cm:
            basic_metric_set = compute_basic_metric_set(
                reach=10,
                frequency_values=[10],
                impressions=5,
                population=1000,
            )

        self.assertEqual(list(basic_metric_set.k_plus_reach), [10])
        self.assertTrue(
            any('Could not fully reconcile' in m for m in cm.output),
            f"expected reconciliation warning, got: {cm.output}")

    def test_compute_basic_metric_set_excess_warning_with_none_reach(self):
        """When reach is None and the histogram's weighted sum exceeds
        impressions, the cascade exhausts higher-frequency buckets and the
        warning must format cleanly -- not TypeError on '%d % None'. With
        reach=None there is no rule 1 to preserve; the warning still
        signals upstream inconsistency to operators (Issue #4049).
        """
        with self.assertLogs(level='WARNING') as cm:
            basic_metric_set = compute_basic_metric_set(
                reach=None,
                frequency_values=[100],
                impressions=5,
                population=1000,
            )

        self.assertEqual(list(basic_metric_set.k_plus_reach), [100])
        self.assertTrue(
            any('Could not fully reconcile' in m for m in cm.output),
            f"expected reconciliation warning, got: {cm.output}")
        self.assertTrue(
            any('reach=None' in m for m in cm.output),
            f"warning should render reach=None, got: {cm.output}")

    def test_compute_basic_metric_set_zero_reach_zeros_k_plus_reach(self):
        """reach=0 (rather than None) means "nobody was reached"; the
        snap overwrites k_plus_reach[0] with 0 and the forward-clamp
        cascades that down through every bucket. Pin the behavior so a
        future change to the snaps truthiness check (e.g. `if reach:`
        instead of `if reach is not None:`) is caught.
        """
        basic_metric_set = compute_basic_metric_set(
            reach=0,
            frequency_values=[3, 5, 2],
            impressions=20,
            population=1000,
        )

        self.assertEqual(basic_metric_set.reach, 0)
        self.assertEqual(list(basic_metric_set.k_plus_reach), [0, 0, 0])
        self.assertEqual(list(basic_metric_set.percent_k_plus_reach),
                         [0.0, 0.0, 0.0])

    def test_compute_basic_metric_set_k_plus_reach_is_non_increasing(self):
        # Without the clamp: k_plus_reach[0]=6 (overwritten from reach) but
        # k_plus_reach[1]=round(5+2)=7 -- "2+ reach > 1+ reach", nonsense.
        basic_metric_set = compute_basic_metric_set(
            reach=6,
            frequency_values=[3, 5, 2],
            impressions=20,
            population=1000,
        )

        k_plus_reach = list(basic_metric_set.k_plus_reach)
        self.assertEqual(k_plus_reach[0], basic_metric_set.reach,
                         "k_plus_reach[0] must equal reach")
        for i in range(1, len(k_plus_reach)):
            self.assertLessEqual(
                k_plus_reach[i], k_plus_reach[i - 1],
                f"k_plus_reach must be non-increasing; "
                f"bucket {i}={k_plus_reach[i]} exceeds "
                f"bucket {i-1}={k_plus_reach[i - 1]}")

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

        # Verifies that there are 20 reporting set results in the sample data.
        self.assertEqual(
            len(add_processed_result_value_request.reporting_set_results), 20)

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
        mock_result.large_corrections = []
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

    def test_post_process_report_result_raises_on_large_corrections(self):
        # Loads a real fixture whose noisy values force the solver to apply
        # corrections that exceed 7 sigma (e.g. reach going from 16.6M to 0).
        with open(
                'src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_result_with_large_corrections.textproto',
                'r') as file:
            large_correction_response = text_format.Parse(
                file.read(),
                report_results_service_pb2.ListReportingSetResultsResponse(),
            )
        self.mock_report_results_stub.ListReportingSetResults.return_value = (
            large_correction_response)
        self.mock_reporting_sets_stub.BatchGetReportingSets.return_value = (
            self.mock_batch_get_reporting_set_response)

        report_result_processor = PostProcessReportResult(
            self.mock_report_results_stub, self.mock_reporting_sets_stub)

        with self.assertRaisesRegex(ValueError, 'large corrections'):
            report_result_processor.process(
                self.cmms_measurement_consumer_id,
                self.external_report_result_id, [])

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


if __name__ == "__main__":
    unittest.main()
