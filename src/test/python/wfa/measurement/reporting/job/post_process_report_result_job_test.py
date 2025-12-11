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

from absl import logging
import unittest
from unittest import mock
import grpc

from wfa.measurement.internal.reporting.v2 import basic_report_pb2
from wfa.measurement.internal.reporting.v2 import basic_reports_service_pb2
from wfa.measurement.internal.reporting.v2 import report_results_service_pb2
from job import post_process_report_result_job
from tools import post_process_report_result

BasicReport = basic_report_pb2.BasicReport


class PostProcessReportResultJobTest(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.mock_report_results_stub = mock.MagicMock()
        self.mock_reporting_sets_stub = mock.MagicMock()
        self.mock_basic_reports_stub = mock.MagicMock()

        self.mock_post_processor = mock.MagicMock(
            spec=post_process_report_result.PostProcessReportResult)

        # Mocks the gRPC channels and stubs.
        self.patches = [
            mock.patch(
                "wfa.measurement.internal.reporting.v2.report_results_service_pb2_grpc.ReportResultsStub",
                return_value=self.mock_report_results_stub,
            ),
            mock.patch(
                "wfa.measurement.internal.reporting.v2.reporting_sets_service_pb2_grpc.ReportingSetsStub",
                return_value=self.mock_reporting_sets_stub,
            ),
            mock.patch(
                "wfa.measurement.internal.reporting.v2.basic_reports_service_pb2_grpc.BasicReportsStub",
                return_value=self.mock_basic_reports_stub,
            ),
            mock.patch(
                "tools.post_process_report_result.PostProcessReportResult",
                return_value=self.mock_post_processor,
            ),
        ]
        for patch in self.patches:
            patch.start()

        mock_channel = mock.create_autospec(grpc.Channel)
        self.job = post_process_report_result_job.PostProcessReportResultJob(
            mock_channel)

    def tearDown(self):
        for patch in self.patches:
            patch.stop()
        super().tearDown()

    def test_execute_unprocessed_reports_successfully(self):
        # Sets up mock objects.
        mock_report1 = BasicReport(
            cmms_measurement_consumer_id="mc_id_1",
            external_report_result_id=101,
        )
        mock_report2 = BasicReport(
            cmms_measurement_consumer_id="mc_id_2",
            external_report_result_id=102,
        )
        self.mock_basic_reports_stub.ListBasicReports.side_effect = [
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[mock_report1],
                next_page_token=basic_reports_service_pb2.
                ListBasicReportsPageToken(),
            ),
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[mock_report2], ),
        ]

        mock_request = (
            report_results_service_pb2.AddProcessedResultValuesRequest())
        self.mock_post_processor.process.return_value = mock_request

        # Executes the job.
        result = self.job.execute()

        # Verifies the expected behavior.
        self.assertTrue(result)
        self.assertEqual(
            self.mock_basic_reports_stub.ListBasicReports.call_count, 2)
        self.assertEqual(self.mock_post_processor.process.call_count, 2)
        self.mock_post_processor.process.assert_any_call("mc_id_1", 101)
        self.mock_post_processor.process.assert_any_call("mc_id_2", 102)

        self.assertEqual(
            self.mock_report_results_stub.AddProcessedResultValues.call_count,
            2)
        self.mock_report_results_stub.AddProcessedResultValues.assert_called_with(
            mock_request)

    def test_execute_no_unprocessed_reports(self):
        # Sets up mock objects.
        self.mock_basic_reports_stub.ListBasicReports.return_value = (
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[]))

        # Executes the job.
        result = self.job.execute()

        # Verifies the expected behavior.
        self.assertTrue(result)
        self.mock_basic_reports_stub.ListBasicReports.assert_called_once()
        self.mock_post_processor.process.assert_not_called()
        self.mock_report_results_stub.AddProcessedResultValues.assert_not_called(
        )

    @mock.patch.object(logging, "warning", autospec=True)
    def test_execute_with_failure(self, mock_logging):
        # Sets up mock objects.
        mock_report1 = BasicReport(
            external_basic_report_id="basic_report_1",
            cmms_measurement_consumer_id="mc_id_1",
            external_report_result_id=101,
        )
        mock_report2 = BasicReport(
            external_basic_report_id="basic_report_2",
            cmms_measurement_consumer_id="mc_id_2",
            external_report_result_id=102,
        )
        self.mock_basic_reports_stub.ListBasicReports.return_value = (
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[mock_report1, mock_report2]))

        mock_request = (
            report_results_service_pb2.AddProcessedResultValuesRequest())

        # The first call fails, the second succeeds.
        self.mock_post_processor.process.side_effect = [
            Exception("Error processing report"),
            mock_request,
        ]

        # Executes the job.
        result = self.job.execute()

        # Verifies the expected behavior.
        self.assertFalse(result)
        self.assertEqual(self.mock_post_processor.process.call_count, 2)

        # Verifies that the successful report was still updated.
        self.mock_report_results_stub.AddProcessedResultValues.assert_called_once_with(
            mock_request)
        self.mock_basic_reports_stub.FailBasicReport.assert_called_once_with(
            basic_reports_service_pb2.FailBasicReportRequest(
                cmms_measurement_consumer_id="mc_id_1",
                external_basic_report_id="basic_report_1",
            ))
        # Verifies that the exception was logged.
        mock_logging.assert_called_once_with(
            "Failed to process  basic report basic_report_1 for measurement consumer mc_id_1."
        )


if __name__ == "__main__":
    unittest.main()
