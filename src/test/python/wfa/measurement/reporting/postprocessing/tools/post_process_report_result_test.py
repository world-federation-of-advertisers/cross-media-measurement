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

    def _make_reporting_set_result(
        self,
        external_reporting_set_result_id: int,
        external_reporting_set_id: str,
        selector: str,
        population_size: int = 1_000_000,
    ) -> report_result_pb2.ReportingSetResult:
        rsr = report_result_pb2.ReportingSetResult(
            cmms_measurement_consumer_id=self.cmms_measurement_consumer_id,
            external_report_result_id=self.external_report_result_id,
            external_reporting_set_result_id=external_reporting_set_result_id,
            population_size=population_size,
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

    def test_reconcile_skips_when_total_side_has_zero_reach(self):
        """When the total-selector RSR has cumulative_results.reach == 0
        (e.g. the spec did not request whole-campaign cumulative reach but
        only requested impressions / GRPs / etc., leaving reach at the
        proto default), the cross-window snap must NOT pull the weekly
        side down to 0 -- that would corrupt a previously-valid reach
        measurement plus its derived k_plus_reach / percent_reach /
        average_frequency. Skip the dimension instead.
        """
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
        # Total side has no cumulative reach (reach left at proto default).
        # In practice this happens when the spec only requested
        # whole-campaign impressions / GRPs, so the cumulative_results
        # message exists (because impressions is in it) but reach is 0.
        self._set_window_reach(request, 1, end_day=15, reach=0,
                               k_plus_reach=[])
        request.reporting_set_results[1].reporting_window_results[
            0].value.cumulative_results.impressions = 9999
        # Weekly side has a real cumulative reach we must not zero.
        self._set_window_reach(request, 2, end_day=15, reach=1000,
                               k_plus_reach=[1000, 500, 200])

        processor._reconcile_cross_window_identities(
            request, reporting_set_results)

        last_weekly = request.reporting_set_results[
            2].reporting_window_results[0]
        # Weekly side untouched: reach and k_plus_reach preserved.
        self.assertEqual(last_weekly.value.cumulative_results.reach, 1000)
        self.assertEqual(
            list(last_weekly.value.cumulative_results.k_plus_reach),
            [1000, 500, 200])
        # Total side also untouched.
        whole = request.reporting_set_results[1].reporting_window_results[0]
        self.assertEqual(whole.value.cumulative_results.reach, 0)
        self.assertEqual(whole.value.cumulative_results.impressions, 9999)

    def test_reconcile_skips_when_weekly_side_has_zero_reach(self):
        """Mirror of the total-side guard: a weekly RSR whose last window
        has reach=0 must not pull whole_campaign down to 0."""
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
        self._set_window_reach(request, 1, end_day=15, reach=1003,
                               k_plus_reach=[1003, 500, 250])
        # Weekly last window has reach=0; do not zero out the total side.
        self._set_window_reach(request, 2, end_day=15, reach=0,
                               k_plus_reach=[])
        request.reporting_set_results[2].reporting_window_results[
            0].value.cumulative_results.impressions = 7777

        processor._reconcile_cross_window_identities(
            request, reporting_set_results)

        whole = request.reporting_set_results[1].reporting_window_results[0]
        self.assertEqual(whole.value.cumulative_results.reach, 1003)
        self.assertEqual(
            list(whole.value.cumulative_results.k_plus_reach),
            [1003, 500, 250])

    def test_reconcile_raises_when_multiple_rsrs_share_dim_and_selector(self):
        """When two RSRs share the dim key AND the same selector kind (e.g.
        two weekly cadences for the same dimension), the post-processor
        cannot disambiguate which cadence's solver run each RSR's
        processed values come from -- one is already shipping values that
        do not correspond to its own measurements (Issue #4056). PR #4057
        rejects the only known input shape that produces this; reaching
        this branch is a bug, and the reconciler raises rather than
        silently shipping inconsistent data."""
        processor = PostProcessReportResult(self.mock_report_results_stub,
                                            self.mock_reporting_sets_stub)
        reporting_set_results = [
            self._make_reporting_set_result(1, 'reporting_set_id_edp1',
                                            'total'),
            self._make_reporting_set_result(2, 'reporting_set_id_edp1',
                                            'weekly'),
            self._make_reporting_set_result(3, 'reporting_set_id_edp1',
                                            'weekly'),
        ]
        request = (
            report_results_service_pb2.AddProcessedResultValuesRequest())
        self._set_window_reach(request, 1, end_day=15, reach=1003)
        self._set_window_reach(request, 2, end_day=15, reach=1000)
        self._set_window_reach(request, 3, end_day=15, reach=999)

        with self.assertRaisesRegex(
                ValueError,
                r"share dimension key.*selector=weekly.*ids 2 and 3"):
            processor._reconcile_cross_window_identities(
                request, reporting_set_results)

    def test_reconcile_skips_silently_when_total_has_no_windows(self):
        """A 'total' RSR with zero reporting windows means the
        request-builder had nothing to write for this RSR. Nothing to
        reconcile -- skip silently, do not warn, do not raise. Also do
        not mutate the weekly side, which still has real data."""
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
        # Touch the total entry to create it in the map, but add no
        # reporting windows.
        _ = request.reporting_set_results[1]
        self._set_window_reach(request, 2, end_day=15, reach=1000,
                               k_plus_reach=[1000, 500, 250])

        processor._reconcile_cross_window_identities(
            request, reporting_set_results)

        # Weekly side untouched.
        weekly = request.reporting_set_results[2].reporting_window_results[0]
        self.assertEqual(weekly.value.cumulative_results.reach, 1000)
        self.assertEqual(
            list(weekly.value.cumulative_results.k_plus_reach),
            [1000, 500, 250])

    def test_reconcile_raises_when_total_has_multiple_windows(self):
        """A 'total' selector covers the full reporting interval, so
        _group_results_by_window must produce exactly one window key.
        More than one means that helper itself misbehaved -- a real
        bug, not a legitimate config. Fail rather than silently shipping
        un-reconciled data."""
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
        # Two windows on a total-selector RSR -- impossible by data model.
        self._set_window_reach(request, 1, end_day=15, reach=1003)
        self._set_window_reach(request, 1, end_day=22, reach=1010)
        self._set_window_reach(request, 2, end_day=15, reach=1000)

        with self.assertRaisesRegex(
                ValueError,
                r"Total-selector ReportingSetResult 1 has 2 reporting "
                r"windows"):
            processor._reconcile_cross_window_identities(
                request, reporting_set_results)

    def test_reconcile_runs_rule1_sweep_when_reaches_agree(self):
        """The Rule 1 sweep runs unconditionally, so a pre-existing
        cumulative non-decreasing violation in the weekly series gets
        fixed even when whole_campaign.reach already equals
        last_weekly.reach (no cross-window snap fires). The earlier
        version of this PR early-returned in the agree case and would
        ship the violation unchanged."""
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
        # whole_campaign and last_weekly agree at 1000 -- no cross-
        # window snap. But week 1 has reach=1100 > week 2's 1000,
        # which violates Rule 1 (cumulative non-decreasing).
        self._set_window_reach(request, 1, end_day=15, reach=1000,
                               k_plus_reach=[1000, 400, 200])
        self._set_window_reach(request, 2, end_day=8, reach=1100,
                               k_plus_reach=[1100, 500, 300])
        self._set_window_reach(request, 2, end_day=15, reach=1000,
                               k_plus_reach=[1000, 400, 200])

        processor._reconcile_cross_window_identities(
            request, reporting_set_results)

        weekly = request.reporting_set_results[2].reporting_window_results
        by_day = {w.key.end.day: w.value.cumulative_results for w in weekly}
        # Earlier week clamped down to its successor's 1000.
        self.assertEqual(by_day[8].reach, 1000)
        self.assertEqual(by_day[15].reach, 1000)
        # last_weekly untouched (no cross-window snap fired).
        self.assertEqual(
            list(by_day[15].k_plus_reach), [1000, 400, 200])
        # whole_campaign untouched too.
        whole = request.reporting_set_results[1].reporting_window_results[0]
        self.assertEqual(whole.value.cumulative_results.reach, 1000)
        self.assertEqual(
            list(whole.value.cumulative_results.k_plus_reach),
            [1000, 400, 200])

    def test_dim_key_raises_on_unknown_iqf_variant(self):
        """`_dimension_key_excluding_metric_frequency_spec` raises when
        the IQF oneof is in an unrecognized state (either unset, or a
        future variant added to the proto without updating this code).
        Failing loud forces a proto-evolution contributor to update the
        dim-key function; the alternative -- silently bucketing the new
        variant into a generic 'none' key -- would collide it with any
        unset-IQF RSR (or with itself across distinct values) and
        mis-reconcile the cross-window snap (Issue #4049)."""
        processor = PostProcessReportResult(self.mock_report_results_stub,
                                            self.mock_reporting_sets_stub)
        rsr = self._make_reporting_set_result(
            1, 'reporting_set_id_edp1', 'total')
        # Simulate the "unknown variant" path by clearing the oneof.
        # This mirrors what a future proto evolution would look like
        # before this function is updated to recognize the new variant.
        rsr.dimension.ClearField('impression_qualification_filter')

        with self.assertRaisesRegex(
                ValueError,
                r"Unknown impression_qualification_filter oneof variant"):
            processor._dimension_key_excluding_metric_frequency_spec(rsr)

    def test_reconcile_snap_down_preserves_cumulative_monotonicity(self):
        """When the cross-window snap pulls last_weekly down to match
        whole_campaign, the Rule 1 backward sweep must also clamp earlier
        weeks that were >= the original last_weekly. Otherwise the snap-down
        would *introduce* a Rule 1 (cumulative non-decreasing) violation on
        a previously-clean report. See Issue #4049."""
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
        # whole_campaign rounds lower than last weekly cumulative; an earlier
        # weekly equals last_weekly pre-snap.
        self._set_window_reach(request, 1, end_day=15, reach=1000)
        self._set_window_reach(request, 2, end_day=8, reach=1001)
        self._set_window_reach(request, 2, end_day=15, reach=1001)

        processor._reconcile_cross_window_identities(
            request, reporting_set_results)

        weekly = request.reporting_set_results[2].reporting_window_results
        earlier = next(w for w in weekly if w.key.end.day == 8)
        last = next(w for w in weekly if w.key.end.day == 15)
        # Last week snapped down to whole_campaign's value, and the backward
        # sweep clamped the earlier week down to match -- Rule 1 holds.
        self.assertEqual(last.value.cumulative_results.reach, 1000)
        self.assertEqual(earlier.value.cumulative_results.reach, 1000)
        self.assertLessEqual(earlier.value.cumulative_results.reach,
                             last.value.cumulative_results.reach)

    def test_reconcile_preserves_per_bms_invariants_after_snap_down(self):
        """Composition test: PR #4053's per-BMS invariants (k_plus_reach[0]
        == reach and sum(k_plus_reach) <= impressions) must still hold on
        both sides after this PR's cross-window snap-down runs. The
        snap-down direction was chosen specifically to preserve these; pin
        it down. Builds each side via the real compute_basic_metric_set so
        the test exercises the actual PR #4053 + PR #4054 composition."""
        processor = PostProcessReportResult(self.mock_report_results_stub,
                                            self.mock_reporting_sets_stub)
        reporting_set_results = [
            self._make_reporting_set_result(1, 'reporting_set_id_edp1',
                                            'total'),
            self._make_reporting_set_result(2, 'reporting_set_id_edp1',
                                            'weekly'),
        ]
        # Float inputs picked so whole_campaign rounds to 2194000 and
        # last_weekly rounds to 2194001 -- the exact 1-unit drift this PR
        # fixes. impressions intentionally below sum(round(freqs)) so
        # PR #4053's cascade fires too, leaving sum(k_plus_reach) ==
        # impressions exactly (the worst case for PR #4054's snap-down --
        # any further drop to k_plus_reach[0] must NOT push the sum below
        # the actual histogram tail).
        whole_camp_bms = compute_basic_metric_set(
            reach=2194000,
            frequency_values=[
                1222269.4, 299330.4, 183245.4, 85498.4, 45170.4,
                64182.4, 26965.4, 7445.4, 5345.4, 4608.4,
                4874.4, 6342.4, 6253.4, 51051.4, 181420.4,
            ],
            impressions=7313150,
            population=8045553,
        )
        last_weekly_bms = compute_basic_metric_set(
            reach=2194001,
            frequency_values=[
                1222270.4, 299330.4, 183245.4, 85498.4, 45170.4,
                64182.4, 26965.4, 7445.4, 5345.4, 4608.4,
                4874.4, 6342.4, 6253.4, 51051.4, 181420.4,
            ],
            impressions=7313151,
            population=8045553,
        )

        request = (
            report_results_service_pb2.AddProcessedResultValuesRequest())
        for rsr_id, bms in ((1, whole_camp_bms), (2, last_weekly_bms)):
            processed = request.reporting_set_results[rsr_id]
            entry = processed.reporting_window_results.add()
            entry.key.end.year = 2025
            entry.key.end.month = 10
            entry.key.end.day = 15
            entry.value.cumulative_results.CopyFrom(bms)

        # Sanity check: per-BMS invariants hold pre-snap on both sides, and
        # the cross-window drift exists.
        for rsr_id in (1, 2):
            bms = request.reporting_set_results[
                rsr_id].reporting_window_results[0].value.cumulative_results
            self.assertEqual(bms.k_plus_reach[0], bms.reach)
            self.assertLessEqual(sum(bms.k_plus_reach), bms.impressions)
        self.assertNotEqual(
            request.reporting_set_results[1].reporting_window_results[0]
            .value.cumulative_results.reach,
            request.reporting_set_results[2].reporting_window_results[0]
            .value.cumulative_results.reach)

        processor._reconcile_cross_window_identities(
            request, reporting_set_results)

        # Per-BMS invariants STILL hold on both sides after the snap-down.
        # Snapping reach DOWN by 1 and k_plus_reach[0] DOWN by 1 keeps both
        # equal (Rule 3) and reduces sum(k_plus_reach) by 1, which can only
        # help Rule 4 (sum <= impressions). A snap-UP would have done the
        # opposite -- this is the justification for min() in the PR.
        for rsr_id in (1, 2):
            bms = request.reporting_set_results[
                rsr_id].reporting_window_results[0].value.cumulative_results
            self.assertEqual(
                bms.k_plus_reach[0], bms.reach,
                f"RSR {rsr_id}: PR #4053 Rule 3 broken after snap-down")
            self.assertLessEqual(
                sum(bms.k_plus_reach), bms.impressions,
                f"RSR {rsr_id}: PR #4053 Rule 4 broken after snap-down")
        # PR #4054 invariant: reaches now agree.
        self.assertEqual(
            request.reporting_set_results[1].reporting_window_results[0]
            .value.cumulative_results.reach,
            request.reporting_set_results[2].reporting_window_results[0]
            .value.cumulative_results.reach)
        # Pin that the snap touched k_plus_reach[0] on the side that was
        # higher, not just the scalar reach field. Without this, a future
        # refactor that forgot to mutate k_plus_reach[0] would still pass
        # the Rule 3 / Rule 4 assertions above (they only check the new
        # value is consistent, not that anything changed).
        last_weekly_after = (request.reporting_set_results[2]
                             .reporting_window_results[0]
                             .value.cumulative_results)
        self.assertEqual(
            last_weekly_after.k_plus_reach[0], 2194000,
            "snap-down must mutate k_plus_reach[0] (was 2194001 pre-snap)")


    def test_reconcile_snap_down_preserves_k_plus_reach_monotonicity(self):
        """When snap-down lowers k_plus_reach[0] on a side where
        k_plus_reach[1] was already equal to k_plus_reach[0] (e.g. the
        frequency=1 bucket rounded to 0), naive overwrite would leave
        k_plus_reach[1] > k_plus_reach[0] -- nonsense and a regression of
        PR #4053's non-increasing invariant. The snap helper must re-clamp
        forward (Issue #4049).
        """
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
        # Both sides have k_plus_reach[0] == k_plus_reach[1] (the worst case
        # for snap-down): freq=1 bucket rounded to 0. After PR #4053's
        # forward-clamp, this state is reachable in real data whenever the
        # rounded 1-frequency bucket is zero.
        self._set_window_reach(
            request, 1, end_day=15, reach=11,
            k_plus_reach=[11, 11, 7, 2])
        self._set_window_reach(
            request, 2, end_day=15, reach=10,
            k_plus_reach=[10, 10, 7, 2])

        processor._reconcile_cross_window_identities(
            request, reporting_set_results)

        # Both sides snapped to 10. The whole_campaign side's k_plus_reach[1]
        # was 11; after lowering k_plus_reach[0] to 10 we MUST re-clamp it,
        # otherwise k_plus_reach[1]=11 > k_plus_reach[0]=10.
        for rsr_id in (1, 2):
            cumulative = (
                request.reporting_set_results[rsr_id]
                .reporting_window_results[0].value.cumulative_results)
            self.assertEqual(cumulative.reach, 10)
            self.assertEqual(cumulative.k_plus_reach[0], 10)
            for i in range(1, len(cumulative.k_plus_reach)):
                self.assertLessEqual(
                    cumulative.k_plus_reach[i],
                    cumulative.k_plus_reach[i - 1],
                    f"RSR {rsr_id}: k_plus_reach must be non-increasing; "
                    f"bucket {i}={cumulative.k_plus_reach[i]} exceeds "
                    f"bucket {i-1}={cumulative.k_plus_reach[i - 1]}")


    def test_snap_cumulative_reach_clamps_non_monotone_input(self):
        """`_snap_cumulative_reach` must forward-clamp every bucket, not
        stop at the first locally-non-increasing pair. The orchestrator
        today only calls it on inputs already shaped by
        `compute_basic_metric_set` (which enforces monotonicity), so the
        old early-break form happened to work. Pin the general-purpose
        contract the docstring promises: a non-monotone input is fully
        clamped, not partially.
        """
        bms = BasicMetricSet()
        bms.reach = 100
        # Deliberately non-monotone: bucket 2 (=20) > bucket 1 (=5).
        # Old code: after `k[0]=8`, loop hits i=1 with 5<=8, breaks,
        # leaves [8, 5, 20, 3] -- a Rule 3 violation between buckets 1
        # and 2 untouched.
        bms.k_plus_reach.extend([100, 5, 20, 3])
        bms.impressions = 200

        PostProcessReportResult._snap_cumulative_reach(
            bms, snapped_reach=8, population=1000)

        self.assertEqual(bms.reach, 8)
        self.assertEqual(list(bms.k_plus_reach), [8, 5, 5, 3])
        for i in range(1, len(bms.k_plus_reach)):
            self.assertLessEqual(
                bms.k_plus_reach[i], bms.k_plus_reach[i - 1],
                f"k_plus_reach must be non-increasing; "
                f"bucket {i}={bms.k_plus_reach[i]} exceeds "
                f"bucket {i-1}={bms.k_plus_reach[i - 1]}")


    def test_reconcile_snap_down_recomputes_derived_fields(self):
        """After snap-down, percent_reach / percent_k_plus_reach[0] /
        average_frequency must equal the value compute_basic_metric_set
        would have produced for the snapped reach -- otherwise the two
        derivation paths drift apart (the bug class that motivated this
        whole PR series). Pin it explicitly so any future mutation in
        _snap_cumulative_reach that forgets to recompute is caught."""
        processor = PostProcessReportResult(self.mock_report_results_stub,
                                            self.mock_reporting_sets_stub)
        population = 8_000_000
        reporting_set_results = [
            self._make_reporting_set_result(1, 'reporting_set_id_edp1',
                                            'total',
                                            population_size=population),
            self._make_reporting_set_result(2, 'reporting_set_id_edp1',
                                            'weekly',
                                            population_size=population),
        ]
        request = (
            report_results_service_pb2.AddProcessedResultValuesRequest())
        # Build a realistic BasicMetricSet on each side via the construction
        # path so derived fields start out internally consistent. Then
        # snap-down should keep them consistent.
        # Deliberately large reach gap between whole and weekly so the
        # drift between stale (pre-snap) and fresh (post-snap) derived fields
        # is large enough to detect against float32 proto precision. Real
        # rounding drift is sub-unit; this test pins behavior, not realism.
        whole_bms = compute_basic_metric_set(
            reach=1_000_000,
            frequency_values=[500_000.0, 250_000.0, 125_000.0],
            impressions=3_000_000,
            population=population,
        )
        weekly_bms = compute_basic_metric_set(
            reach=2_000_000,
            frequency_values=[1_000_000.0, 500_000.0, 250_000.0],
            impressions=6_000_000,
            population=population,
        )
        for rsr_id, bms in ((1, whole_bms), (2, weekly_bms)):
            entry = request.reporting_set_results[
                rsr_id].reporting_window_results.add()
            entry.key.end.year = 2025
            entry.key.end.month = 10
            entry.key.end.day = 15
            entry.value.cumulative_results.CopyFrom(bms)

        processor._reconcile_cross_window_identities(
            request, reporting_set_results)

        snapped_reach = 1_000_000
        for rsr_id in (1, 2):
            bms = (request.reporting_set_results[rsr_id]
                   .reporting_window_results[0].value.cumulative_results)
            self.assertEqual(bms.reach, snapped_reach)
            self.assertEqual(bms.k_plus_reach[0], snapped_reach)
            # Derived fields must match what compute_basic_metric_set would
            # have produced for snapped_reach -- not the pre-snap reach.
            self.assertAlmostEqual(
                bms.percent_reach, snapped_reach / population * 100,
                places=4,
                msg=f"RSR {rsr_id}: percent_reach drifted after snap-down")
            self.assertAlmostEqual(
                bms.percent_k_plus_reach[0],
                snapped_reach / population * 100, places=4,
                msg=f"RSR {rsr_id}: percent_k_plus_reach[0] drifted")
            self.assertAlmostEqual(
                bms.average_frequency, bms.impressions / snapped_reach,
                places=4,
                msg=f"RSR {rsr_id}: average_frequency drifted")
            self.assertAlmostEqual(
                bms.grps, bms.impressions / population * 100, places=4,
                msg=f"RSR {rsr_id}: grps drifted")


    def test_reconcile_snap_down_rule_1_cascade(self):
        """All earlier weekly windows that exceed the snapped last_weekly
        get clamped down (cascade), and the clamp keeps per-BMS invariants
        (Issue #4049 Rules 3 and 4) intact on every touched window.

        Concretely: weeks 1, 2, 3, 4 with reaches [800, 1100, 1100, 1100],
        whole_campaign=1000. After snap: last_weekly drops to 1000; weeks 2
        and 3 cascade down to 1000; week 1 (already below 1000) is
        untouched."""
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
        # whole_campaign at 1000; last week at 1100 forces snap-down to 1000.
        # Weeks 2 and 3 are also at 1100 and must cascade. Week 1 at 800
        # stays put. Each weekly window carries a non-trivial k_plus_reach
        # and impressions so we can pin Rules 3 and 4 after clamp.
        self._set_window_reach(request, 1, end_day=29, reach=1000,
                               k_plus_reach=[1000, 400, 200, 50])
        self._set_window_reach(request, 2, end_day=1, reach=800,
                               k_plus_reach=[800, 200, 50, 10])
        self._set_window_reach(request, 2, end_day=8, reach=1100,
                               k_plus_reach=[1100, 500, 300, 100])
        self._set_window_reach(request, 2, end_day=15, reach=1100,
                               k_plus_reach=[1100, 500, 300, 100])
        self._set_window_reach(request, 2, end_day=22, reach=1100,
                               k_plus_reach=[1100, 500, 300, 100])
        # Last weekly. Picked so impressions == sum(k_plus_reach), the worst
        # case for Rule 4 after clamp (any further drop on k_plus_reach[0]
        # must not push the sum above impressions).
        self._set_window_reach(request, 2, end_day=29, reach=1100,
                               k_plus_reach=[1100, 500, 300, 100])
        for w in request.reporting_set_results[2].reporting_window_results:
            w.value.cumulative_results.impressions = sum(
                w.value.cumulative_results.k_plus_reach)

        processor._reconcile_cross_window_identities(
            request, reporting_set_results)

        weekly = request.reporting_set_results[2].reporting_window_results
        by_day = {w.key.end.day: w.value.cumulative_results for w in weekly}

        # Snap-down + cascade.
        self.assertEqual(by_day[1].reach, 800,
                         "earlier week below snapped reach is untouched")
        self.assertEqual(by_day[8].reach, 1000, "cascade clamps week 2")
        self.assertEqual(by_day[15].reach, 1000, "cascade clamps week 3")
        self.assertEqual(by_day[22].reach, 1000, "cascade clamps week 4")
        self.assertEqual(by_day[29].reach, 1000, "last weekly snapped")

        # Rule 1: cumulative non-decreasing across all weekly windows.
        sorted_days = sorted(by_day)
        for prev_day, cur_day in zip(sorted_days, sorted_days[1:]):
            self.assertLessEqual(
                by_day[prev_day].reach, by_day[cur_day].reach,
                f"Rule 1 violation: week ending day {prev_day} reach "
                f"{by_day[prev_day].reach} > week ending day {cur_day} "
                f"reach {by_day[cur_day].reach}")

        # Per-BMS invariants survive the clamp on every touched window.
        for day, bms in by_day.items():
            self.assertEqual(bms.k_plus_reach[0], bms.reach,
                             f"Rule 3 broken on week ending day {day}")
            self.assertLessEqual(
                sum(bms.k_plus_reach), bms.impressions,
                f"Rule 4 broken on week ending day {day}")
            for i in range(1, len(bms.k_plus_reach)):
                self.assertLessEqual(
                    bms.k_plus_reach[i], bms.k_plus_reach[i - 1],
                    f"k_plus_reach non-increasing broken on day {day}")


if __name__ == "__main__":
    unittest.main()
