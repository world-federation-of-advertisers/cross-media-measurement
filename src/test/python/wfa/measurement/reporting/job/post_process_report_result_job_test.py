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
import logging as stdlib_logging
import unittest
from unittest import mock
import grpc

from wfa.measurement.internal.reporting.v2 import basic_report_pb2
from wfa.measurement.internal.reporting.v2 import basic_reports_service_pb2
from wfa.measurement.internal.reporting.v2 import report_results_service_pb2
from google.rpc import error_details_pb2
from google.rpc import status_pb2
from google.protobuf import any_pb2
from noiseninja import noise_correction_value_logging
from job import post_process_report_result_job
from tools import post_process_report_result
from tools.potential_direct_result_minimum_thresholds import (
    PotentialDirectResultMinimumThresholds,
)

BasicReport = basic_report_pb2.BasicReport


class PostProcessReportResultJobTest(unittest.TestCase):

    def test_report_trace_filter_adds_basic_report_and_report_names(self):
        basic_report = BasicReport(
            cmms_measurement_consumer_id="mc_id_1",
            external_basic_report_id="basic_report_1",
            external_report_id="report_1",
        )
        record = stdlib_logging.LogRecord(
            name="test",
            level=stdlib_logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="xmm.lifecycle.stage=noise_correction xmm.outcome=started",
            args=(),
            exc_info=None,
        )

        with post_process_report_result_job._report_trace_logging_context(
            basic_report
        ):
            post_process_report_result_job._ReportTraceFilter().filter(record)

        self.assertIn(
            "xmm.basic_report.name=measurementConsumers/mc_id_1/"
            "basicReports/basic_report_1",
            record.getMessage(),
        )
        self.assertIn(
            "xmm.report.name=measurementConsumers/mc_id_1/reports/report_1",
            record.getMessage(),
        )
        self.assertIn("xmm.lifecycle.stage=noise_correction", record.getMessage())

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
        started_patches = [patch.start() for patch in self.patches]
        self.mock_post_processor_class = started_patches[-1]

        mock_channel = mock.create_autospec(grpc.Channel)
        self.job = post_process_report_result_job.PostProcessReportResultJob(
            mock_channel,
            log_noise_correction_values=False,
        )

    def tearDown(self):
        for patch in self.patches:
            patch.stop()
        super().tearDown()

    def test_value_logging_context_is_enabled_only_during_processing(self):
        mock_channel = mock.create_autospec(grpc.Channel)
        job = post_process_report_result_job.PostProcessReportResultJob(
            mock_channel,
            log_noise_correction_values=True,
        )
        basic_report = BasicReport(
            cmms_measurement_consumer_id="mc_id_1",
            external_basic_report_id="basic_report_1",
            external_report_result_id=101,
        )

        def process(*unused_args):
            self.assertTrue(noise_correction_value_logging.is_enabled())
            return None

        self.mock_post_processor.process.side_effect = process

        self.assertTrue(job._process_basic_report(basic_report))
        self.assertFalse(noise_correction_value_logging.is_enabled())

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
                log_noise_correction_values=False,
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

    def test_potential_direct_thresholds_are_passed_to_post_processor(self):
        mock_channel = mock.create_autospec(grpc.Channel)
        thresholds = PotentialDirectResultMinimumThresholds(
            min_users=100,
            min_impressions=1000,
        )

        post_process_report_result_job.PostProcessReportResultJob(
            mock_channel,
            log_noise_correction_values=False,
            potential_direct_result_minimum_thresholds=thresholds,
            potential_direct_thresholding_edps=["dataProviders/edp1"],
        )

        self.mock_post_processor_class.assert_called_with(
            self.mock_report_results_stub,
            self.mock_reporting_sets_stub,
            potential_direct_result_minimum_thresholds=thresholds,
            potential_direct_thresholding_edps=["dataProviders/edp1"],
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

    @mock.patch.object(logging, "info", autospec=True)
    def test_no_update_logs_noise_correction_succeeded(self, mock_logging):
        mock_report = BasicReport(
            external_basic_report_id="basic_report_1",
            cmms_measurement_consumer_id="mc_id_1",
            external_report_result_id=101,
        )
        self.mock_basic_reports_stub.ListBasicReports.return_value = (
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[mock_report]
            )
        )
        self.mock_post_processor.process.return_value = None

        result = self.job.execute()

        self.assertTrue(result)
        self.assertTrue(
            any(
                call.args[0]
                == "xmm.lifecycle.stage=noise_correction "
                "xmm.outcome=succeeded "
                "xmm.operation.result=no_update_required"
                for call in mock_logging.call_args_list
            )
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
            "xmm.lifecycle.stage=noise_correction xmm.outcome=failed "
            "%s "
            "Failed to process BasicReport %s for MeasurementConsumer %s",
            "xmm.error.type=Exception",
            "basic_report_1",
            "mc_id_1",
            exc_info=True,
        )

    def _noise_correction_failure(self):
        failed_result = post_process_report_result.ReportPostProcessorResult(
            status=post_process_report_result.ReportPostProcessorStatus(
                status_code=(
                    post_process_report_result.ReportPostProcessorStatus.SOLUTION_NOT_FOUND
                ),
                primal_equality_residual=3.5,
                primal_inequality_residual=4.5,
            )
        )
        failed_summary = failed_result.pre_correction_report_summary_v2
        failed_summary.cmms_measurement_consumer_id = "mc_id_1"
        failed_summary.external_report_result_id = 101
        set_result = failed_summary.report_summary_set_results.add(
            external_reporting_set_result_id=123456789,
            impression_filter="ami",
            set_operation="union",
            data_providers=["reporting-set-1"],
        )
        set_result.whole_campaign_result.reach.value = 918273645
        set_result.whole_campaign_result.reach.standard_deviation = 12.5
        set_result.whole_campaign_result.reach.metric = "synthetic-reach"

        successful_result = post_process_report_result.ReportPostProcessorResult(
            status=post_process_report_result.ReportPostProcessorStatus(
                status_code=(
                    post_process_report_result.ReportPostProcessorStatus.SOLUTION_FOUND_WITH_HIGHS
                )
            )
        )
        successful_summary = successful_result.pre_correction_report_summary_v2
        successful_summary.cmms_measurement_consumer_id = "mc_id_1"
        successful_summary.external_report_result_id = 101

        context = post_process_report_result.ReportingSetDiagnosticContext(
            external_reporting_set_id="reporting-set-1",
            external_reporting_set_result_id=123456789,
            primitive_reporting_set_ids=("primitive-reporting-set-1",),
            data_provider_resource_names=("dataProviders/provider-1",),
            window_end_dates=("2026-09-30",),
        )
        measurement_context = (
            post_process_report_result.MeasurementDiagnosticContext(
                external_reporting_set_result_id=123456789,
                impression_qualification_filter_type="EXTERNAL",
                external_impression_qualification_filter_id="ami",
                venn_diagram_region_type="UNION",
                source_reporting_set_type="COMPOSITE",
                source_reporting_set_root_operation="DIFFERENCE",
                metric_frequency="WEEKLY",
                window_type="NON_CUMULATIVE",
                is_cumulative=False,
                measurement_type="REACH_AND_FREQUENCY",
                data_provider_count=1,
                window_end_date="2026-09-30",
            )
        )
        return post_process_report_result.NoiseCorrectionFailure([
            post_process_report_result.ReportSummaryProcessingResult(
                report_summary_index=0,
                result=failed_result,
                reporting_set_contexts=(context,),
                measurement_contexts=(measurement_context,),
            ),
            post_process_report_result.ReportSummaryProcessingResult(
                report_summary_index=1,
                result=successful_result,
                reporting_set_contexts=(context,),
                measurement_contexts=(measurement_context,),
            ),
        ])

    def test_noise_correction_failure_logs_every_summary_without_values(self):
        basic_report = BasicReport(
            external_basic_report_id="basic_report_1",
            external_report_id="report_1",
            cmms_measurement_consumer_id="mc_id_1",
            external_report_result_id=101,
        )
        self.mock_basic_reports_stub.ListBasicReports.return_value = (
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[basic_report]
            )
        )
        self.mock_post_processor.process.side_effect = (
            self._noise_correction_failure()
        )

        with self.assertLogs(level="WARNING") as captured:
            result = self.job.execute()

        logs = "\n".join(captured.output)
        self.assertFalse(result)
        self.assertEqual(logs.count("report_failed=true"), 2)
        self.assertIn("summary_failed=true", logs)
        self.assertIn("summary_failed=false", logs)
        self.assertIn("measurement_values_logging_enabled=false", logs)
        self.assertIn("status=SOLUTION_NOT_FOUND", logs)
        self.assertIn("primal_equality_residual=3.5", logs)
        self.assertNotIn("918273645", logs)
        self.assertNotIn("12.5", logs)
        self.assertNotIn("primitive-reporting-set-1", logs)
        self.assertNotIn("dataProviders/provider-1", logs)
        self.assertNotIn("2026-09-30", logs)
        self.assertNotIn("REACH_AND_FREQUENCY", logs)
        self.assertNotIn("NON_CUMULATIVE", logs)
        self.assertNotIn("DIFFERENCE", logs)
        self.assertNotIn('"policy"', logs)
        self.mock_basic_reports_stub.FailBasicReport.assert_called_once()

    def test_noise_correction_failure_logs_values_when_enabled(self):
        mock_channel = mock.create_autospec(grpc.Channel)
        job = post_process_report_result_job.PostProcessReportResultJob(
            mock_channel,
            log_noise_correction_values=True,
        )
        basic_report = BasicReport(
            external_basic_report_id="basic_report_1",
            cmms_measurement_consumer_id="mc_id_1",
            external_report_result_id=101,
        )
        self.mock_basic_reports_stub.ListBasicReports.return_value = (
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[basic_report]
            )
        )
        self.mock_post_processor.process.side_effect = (
            self._noise_correction_failure()
        )

        with self.assertLogs(level="WARNING") as captured:
            result = job.execute()

        logs = "\n".join(captured.output)
        self.assertFalse(result)
        self.assertEqual(logs.count("report_failed=true"), 2)
        self.assertIn("measurement_values_logging_enabled=true", logs)
        self.assertIn("pre_correction_report_summary_v2", logs)
        self.assertIn("918273645", logs)
        self.assertIn("12.5", logs)
        self.assertIn("reporting-set-1", logs)
        self.assertIn("primitive-reporting-set-1", logs)
        self.assertIn("dataProviders/provider-1", logs)
        self.assertIn("2026-09-30", logs)
        self.assertIn("REACH_AND_FREQUENCY", logs)
        self.assertIn("NON_CUMULATIVE", logs)
        self.assertIn("DIFFERENCE", logs)
        self.assertIn('"impression_qualification_filter_type":"EXTERNAL"', logs)
        self.assertIn(
            '"external_impression_qualification_filter_id":"ami"', logs
        )
        self.assertNotIn('"policy"', logs)
        self.assertNotIn("UNIQUE", logs)
        self.assertNotIn("INCREMENTAL", logs)

    @mock.patch.object(logging, "info", autospec=True)
    def test_fail_basic_report_logs_started_and_succeeded(self, mock_info):
        mock_report = BasicReport(
            external_basic_report_id="basic_report_1",
            cmms_measurement_consumer_id="mc_id_1",
        )

        result = self.job._fail_basic_report(mock_report)

        self.assertTrue(result)
        mock_info.assert_has_calls(
            [
                mock.call(
                    "xmm.lifecycle.stage=basic_report_failure_writeback "
                    "xmm.outcome=started Marking BasicReport %s for "
                    "MeasurementConsumer %s as FAILED",
                    "basic_report_1",
                    "mc_id_1",
                ),
                mock.call(
                    "xmm.lifecycle.stage=basic_report_failure_writeback "
                    "xmm.outcome=succeeded Marked BasicReport %s for "
                    "MeasurementConsumer %s as FAILED",
                    "basic_report_1",
                    "mc_id_1",
                ),
            ]
        )

    @mock.patch.object(logging, "error", autospec=True)
    def test_fail_basic_report_failure_is_structured(self, mock_error):
        mock_report = BasicReport(
            external_basic_report_id="basic_report_1",
            cmms_measurement_consumer_id="mc_id_1",
            external_report_result_id=101,
        )
        self.mock_basic_reports_stub.ListBasicReports.return_value = (
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[mock_report]
            )
        )
        self.mock_post_processor.process.side_effect = ValueError("bad input")
        self.mock_basic_reports_stub.FailBasicReport.side_effect = RuntimeError(
            "write failed"
        )

        result = self.job.execute()

        self.assertFalse(result)
        self.assertTrue(
            any(
                "xmm.lifecycle.stage=basic_report_failure_writeback "
                "xmm.outcome=failed %s" in call.args[0]
                and call.args[1] == "xmm.error.type=RuntimeError"
                for call in mock_error.call_args_list
            )
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
        self.assertTrue(
            any(
                "xmm.outcome=succeeded "
                "xmm.operation.result=already_completed" in call.args[0]
                for call in mock_logging.call_args_list
            )
        )

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

    @mock.patch.object(logging, "warning", autospec=True)
    def test_execute_does_not_fail_on_transient_grpc_error(
        self, mock_warning
    ):
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
        self.assertTrue(
            any(
                "%s" in call.args[0]
                and call.args[1]
                == "xmm.error.type=RpcError xmm.error.code=grpc.UNAVAILABLE"
                for call in mock_warning.call_args_list
            )
        )

    @mock.patch.object(logging, "warning", autospec=True)
    def test_execute_fails_basic_report_on_permanent_grpc_error(
        self, mock_warning
    ):
        mock_report = BasicReport(
            external_basic_report_id="basic_report_invalid",
            cmms_measurement_consumer_id="mc_id_1",
            external_report_result_id=101,
        )
        self.mock_basic_reports_stub.ListBasicReports.return_value = (
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[mock_report]
            )
        )
        request = report_results_service_pb2.AddProcessedResultValuesRequest()
        self.mock_post_processor.process.return_value = request
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.INVALID_ARGUMENT
        rpc_error.details = lambda: "invalid result"
        self.mock_report_results_stub.AddProcessedResultValues.side_effect = (
            rpc_error
        )

        result = self.job.execute()

        self.assertFalse(result)
        self.mock_basic_reports_stub.FailBasicReport.assert_called_once_with(
            basic_reports_service_pb2.FailBasicReportRequest(
                cmms_measurement_consumer_id="mc_id_1",
                external_basic_report_id="basic_report_invalid",
            )
        )
        self.assertTrue(
            any(
                "Permanent gRPC failure" in call.args[0]
                and call.args[2] == "INVALID_ARGUMENT"
                for call in mock_warning.call_args_list
            )
        )

    @mock.patch.object(logging, "warning", autospec=True)
    def test_execute_structures_non_grpc_writeback_failure(
        self, mock_warning
    ):
        mock_report = BasicReport(
            external_basic_report_id="basic_report_writeback",
            cmms_measurement_consumer_id="mc_id_1",
            external_report_result_id=101,
        )
        self.mock_basic_reports_stub.ListBasicReports.return_value = (
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[mock_report]
            )
        )
        request = report_results_service_pb2.AddProcessedResultValuesRequest()
        self.mock_post_processor.process.return_value = request
        self.mock_report_results_stub.AddProcessedResultValues.side_effect = (
            ValueError("serialization failed")
        )

        result = self.job.execute()

        self.assertFalse(result)
        self.mock_basic_reports_stub.FailBasicReport.assert_called_once()
        self.assertTrue(
            any(
                "xmm.lifecycle.stage=processed_result_writeback "
                "xmm.outcome=failed %s" in call.args[0]
                and call.args[1] == "xmm.error.type=ValueError"
                for call in mock_warning.call_args_list
            )
        )

    @mock.patch.object(logging, "warning", autospec=True)
    def test_execute_records_wrapped_grpc_error_code(self, mock_warning):
        mock_report = BasicReport(
            external_basic_report_id="basic_report_wrapped_error",
            cmms_measurement_consumer_id="mc_id_1",
            external_report_result_id=101,
        )
        self.mock_basic_reports_stub.ListBasicReports.return_value = (
            basic_reports_service_pb2.ListBasicReportsResponse(
                basic_reports=[mock_report]
            )
        )
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.PERMISSION_DENIED
        try:
            raise RuntimeError("post-processing failed") from rpc_error
        except RuntimeError as error:
            wrapped_error = error
        self.mock_post_processor.process.side_effect = wrapped_error

        result = self.job.execute()

        self.assertFalse(result)
        self.assertTrue(
            any(
                call.args[1]
                == "xmm.error.type=RuntimeError "
                "xmm.error.code=grpc.PERMISSION_DENIED"
                for call in mock_warning.call_args_list
            )
        )


if __name__ == "__main__":
    unittest.main()
