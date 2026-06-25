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
from google.rpc import error_details_pb2
from google.rpc import status_pb2
from google.protobuf import any_pb2
from job import post_process_report_result_job
from tools import post_process_report_result

BasicReport = basic_report_pb2.BasicReport


class PostProcessReportResultJobTest(unittest.TestCase):

    @staticmethod
    def _make_rpc_error_with_error_info(
        code: grpc.StatusCode,
        reason: str,
        metadata: dict,
        details_text: str = "",
        domain: str = "internal.reporting.halo-cmm.org",
    ) -> grpc.RpcError:
        """Builds a grpc.RpcError that carries a google.rpc.Status with an
        ErrorInfo in its trailing metadata, matching the wire format the
        Reporting server emits.
        """
        error_info = error_details_pb2.ErrorInfo(
            domain=domain,
            reason=reason,
            metadata=metadata,
        )
        any_detail = any_pb2.Any()
        any_detail.Pack(error_info)
        status = status_pb2.Status(
            code=code.value[0],
            message=details_text,
            details=[any_detail],
        )
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda c=code: c
        rpc_error.details = lambda d=details_text: d
        rpc_error.trailing_metadata = lambda b=status.SerializeToString(): (
            ("grpc-status-details-bin", b),
        )
        return rpc_error

    def setUp(self):
        super().setUp()
        self.mock_report_results_stub = mock.MagicMock()
        self.mock_reporting_sets_stub = mock.MagicMock()
        self.mock_basic_reports_stub = mock.MagicMock()

        self.mock_post_processor = mock.MagicMock(
            spec=post_process_report_result.PostProcessReportResult
        )

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
            mock_channel
        )

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
                next_page_token=basic_reports_service_pb2.ListBasicReportsPageToken(),
            ),
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[mock_report2],
            ),
        ]

        mock_request = (
            report_results_service_pb2.AddProcessedResultValuesRequest()
        )
        self.mock_post_processor.process.return_value = mock_request

        # Executes the job.
        result = self.job.execute()

        # Verifies the expected behavior.
        self.assertTrue(result)
        self.assertEqual(
            self.mock_basic_reports_stub.ListBasicReports.call_count, 2
        )
        self.assertEqual(self.mock_post_processor.process.call_count, 2)
        self.mock_post_processor.process.assert_any_call("mc_id_1", 101, [])
        self.mock_post_processor.process.assert_any_call("mc_id_2", 102, [])

        self.assertEqual(
            self.mock_report_results_stub.AddProcessedResultValues.call_count, 2
        )
        self.mock_report_results_stub.AddProcessedResultValues.assert_called_with(
            mock_request
        )

    def test_execute_with_exemption(self):
        # Sets up mock objects.
        mock_channel = mock.create_autospec(grpc.Channel)
        job_with_exemption = (
            post_process_report_result_job.PostProcessReportResultJob(
                mock_channel,
                ami_mrc_exempted_edps=[
                    "dataProviders/edp1",
                    "dataProviders/edp2",
                ],
            )
        )
        mock_report = BasicReport(
            cmms_measurement_consumer_id="mc_id_1",
            external_report_result_id=101,
        )
        self.mock_basic_reports_stub.ListBasicReports.return_value = (
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[mock_report]
            )
        )

        mock_request = (
            report_results_service_pb2.AddProcessedResultValuesRequest()
        )
        self.mock_post_processor.process.return_value = mock_request

        # Executes the job.
        result = job_with_exemption.execute()

        # Verifies the expected behavior.
        self.assertTrue(result)
        self.mock_post_processor.process.assert_called_once_with(
            "mc_id_1",
            101,
            ["dataProviders/edp1", "dataProviders/edp2"],
        )

    def test_execute_no_unprocessed_reports(self):
        # Sets up mock objects.
        self.mock_basic_reports_stub.ListBasicReports.return_value = (
            basic_reports_service_pb2.ListBasicReportsResponse(basic_reports=[])
        )

        # Executes the job.
        result = self.job.execute()

        # Verifies the expected behavior.
        self.assertTrue(result)
        self.mock_basic_reports_stub.ListBasicReports.assert_called_once()
        self.mock_post_processor.process.assert_not_called()
        self.mock_report_results_stub.AddProcessedResultValues.assert_not_called()

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
                basic_reports=[mock_report1, mock_report2]
            )
        )

        mock_request = (
            report_results_service_pb2.AddProcessedResultValuesRequest()
        )

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
            mock_request
        )
        self.mock_basic_reports_stub.FailBasicReport.assert_called_once_with(
            basic_reports_service_pb2.FailBasicReportRequest(
                cmms_measurement_consumer_id="mc_id_1",
                external_basic_report_id="basic_report_1",
            )
        )
        # Verifies that the exception was logged.
        mock_logging.assert_called_once_with(
            "Failed to process BasicReport %s for MeasurementConsumer %s",
            "basic_report_1",
            "mc_id_1",
            exc_info=True,
        )

    @mock.patch.object(logging, "info", autospec=True)
    def test_execute_skips_basic_report_already_processed(self, mock_logging):
        """Regression test: if AddProcessedResultValues fails because the
        BasicReport state is no longer UNPROCESSED_RESULTS_READY (e.g. it was
        already advanced to SUCCEEDED by another writer between the list call
        and the add call), the job should NOT mark the BasicReport as FAILED.
        That would corrupt a BasicReport that already has valid processed
        results. Instead, the job should skip it and continue.
        """
        mock_report = BasicReport(
            external_basic_report_id="basic_report_already_done",
            cmms_measurement_consumer_id="mc_id_1",
            external_report_result_id=101,
        )
        self.mock_basic_reports_stub.ListBasicReports.return_value = (
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[mock_report]
            )
        )

        mock_request = (
            report_results_service_pb2.AddProcessedResultValuesRequest()
        )
        self.mock_post_processor.process.return_value = mock_request

        # Server emits FAILED_PRECONDITION with ErrorInfo whose
        # basicReportState says the BR is already past
        # UNPROCESSED_RESULTS_READY.
        rpc_error = self._make_rpc_error_with_error_info(
            code=grpc.StatusCode.FAILED_PRECONDITION,
            reason="BASIC_REPORT_STATE_INVALID",
            metadata={"basicReportState": "SUCCEEDED"},
            details_text=(
                "BasicReport with external key (mc_id_1,"
                " basic_report_already_done) is in state SUCCEEDED which is"
                " invalid for the operation"
            ),
        )
        self.mock_report_results_stub.AddProcessedResultValues.side_effect = (
            rpc_error
        )

        result = self.job.execute()

        # Job should succeed overall (the BR was already done by someone else).
        self.assertTrue(result)
        # The BasicReport must NOT be marked FAILED.
        self.mock_basic_reports_stub.FailBasicReport.assert_not_called()

    def test_execute_fails_basic_report_on_other_failed_precondition(self):
        """When AddProcessedResultValues returns FAILED_PRECONDITION for a
        cause OTHER than the BasicReport state having advanced (e.g. missing
        ReportingSetResult/ReportingWindowResult -- real data integrity
        error), the BasicReport state is still UNPROCESSED_RESULTS_READY. The
        job must NOT silently skip; it should mark the BasicReport FAILED so
        the issue is surfaced.
        """
        mock_report = BasicReport(
            external_basic_report_id="basic_report_missing_data",
            cmms_measurement_consumer_id="mc_id_1",
            external_report_result_id=101,
        )
        self.mock_basic_reports_stub.ListBasicReports.return_value = (
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[mock_report]
            )
        )

        mock_request = (
            report_results_service_pb2.AddProcessedResultValuesRequest()
        )
        self.mock_post_processor.process.return_value = mock_request

        # Server emits FAILED_PRECONDITION with a DIFFERENT reason -- this
        # is a real data-integrity error, not "already advanced", and the
        # BasicReport must be marked FAILED.
        rpc_error = self._make_rpc_error_with_error_info(
            code=grpc.StatusCode.FAILED_PRECONDITION,
            reason="REPORTING_SET_RESULT_NOT_FOUND",
            metadata={},
            details_text="ReportingSetResult with external id (...) not found",
        )
        self.mock_report_results_stub.AddProcessedResultValues.side_effect = (
            rpc_error
        )

        result = self.job.execute()

        # Job should surface the failure.
        self.assertFalse(result)
        # The BasicReport must be marked FAILED so the integrity issue is
        # visible rather than silently retried forever.
        self.mock_basic_reports_stub.FailBasicReport.assert_called_once_with(
            basic_reports_service_pb2.FailBasicReportRequest(
                cmms_measurement_consumer_id="mc_id_1",
                external_basic_report_id="basic_report_missing_data",
            )
        )

    def test_execute_fails_basic_report_when_state_reason_comes_from_other_domain(
        self,
    ):
        """When AddProcessedResultValues returns FAILED_PRECONDITION with an
        ErrorInfo whose reason text matches BASIC_REPORT_STATE_INVALID but the
        domain is NOT the reporting server's, the BasicReport must NOT be
        silently skipped: the reason name is only meaningful within its own
        domain. Treat as a real failure -- mark FAILED.
        """
        mock_report = BasicReport(
            external_basic_report_id="basic_report_wrong_domain",
            cmms_measurement_consumer_id="mc_id_1",
            external_report_result_id=101,
        )
        self.mock_basic_reports_stub.ListBasicReports.return_value = (
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[mock_report]
            )
        )

        mock_request = (
            report_results_service_pb2.AddProcessedResultValuesRequest()
        )
        self.mock_post_processor.process.return_value = mock_request

        rpc_error = self._make_rpc_error_with_error_info(
            code=grpc.StatusCode.FAILED_PRECONDITION,
            reason="BASIC_REPORT_STATE_INVALID",
            metadata={"basicReportState": "SUCCEEDED"},
            details_text="lookalike error from some other subsystem",
            domain="other.example.com",
        )
        self.mock_report_results_stub.AddProcessedResultValues.side_effect = (
            rpc_error
        )

        result = self.job.execute()

        self.assertFalse(result)
        self.mock_basic_reports_stub.FailBasicReport.assert_called_once_with(
            basic_reports_service_pb2.FailBasicReportRequest(
                cmms_measurement_consumer_id="mc_id_1",
                external_basic_report_id="basic_report_wrong_domain",
            )
        )

    def test_execute_does_not_fail_on_transient_grpc_error(self):
        """If AddProcessedResultValues fails with a transient gRPC error such
        as UNAVAILABLE, the BasicReport should be left in
        UNPROCESSED_RESULTS_READY for the next tick to retry; it must not be
        marked FAILED.
        """
        mock_report = BasicReport(
            external_basic_report_id="basic_report_transient",
            cmms_measurement_consumer_id="mc_id_1",
            external_report_result_id=101,
        )
        self.mock_basic_reports_stub.ListBasicReports.return_value = (
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[mock_report]
            )
        )

        mock_request = (
            report_results_service_pb2.AddProcessedResultValuesRequest()
        )
        self.mock_post_processor.process.return_value = mock_request

        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.UNAVAILABLE
        rpc_error.details = lambda: "connection reset"
        self.mock_report_results_stub.AddProcessedResultValues.side_effect = (
            rpc_error
        )

        result = self.job.execute()

        self.assertFalse(result)
        # Transient failure should not fail the BasicReport.
        self.mock_basic_reports_stub.FailBasicReport.assert_not_called()


if __name__ == "__main__":
    unittest.main()
