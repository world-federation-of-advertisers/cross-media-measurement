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
"""A job for fetching, correcting, and updating a report."""

from absl import logging
from typing import Iterable
import grpc

from wfa.measurement.internal.reporting.v2 import basic_report_pb2
from wfa.measurement.internal.reporting.v2 import basic_reports_service_pb2
from wfa.measurement.internal.reporting.v2 import basic_reports_service_pb2_grpc
from wfa.measurement.internal.reporting.v2 import report_results_service_pb2_grpc
from wfa.measurement.internal.reporting.v2 import reporting_sets_service_pb2_grpc
from tools import post_process_report_result

_MAX_PAGE_SIZE = 50

class PostProcessReportResultJob:
    """A job for fetching, correcting, and updating a report."""

    def __init__(
        self,
        internal_reporting_channel: grpc.Channel,
        ami_mrc_exempted_edps: Iterable[str] | None = None,
    ):
        """Initializes the job with the necessary gRPC stubs.

        Args:
            internal_reporting_channel: A gRPC channel to the internal reporting
                server.
            ami_mrc_exempted_edps: The list of EDPs resource name for which the
                AMI >= MRC consistency checks are disabled.
        """
        self._report_results_stub = (
            report_results_service_pb2_grpc.ReportResultsStub(
                internal_reporting_channel))
        self._reporting_sets_stub = (
            reporting_sets_service_pb2_grpc.ReportingSetsStub(
                internal_reporting_channel))
        self._basic_reports_stub = (
            basic_reports_service_pb2_grpc.BasicReportsStub(
                internal_reporting_channel))
        self._post_processor = (
            post_process_report_result.PostProcessReportResult(
                self._report_results_stub, self._reporting_sets_stub
            )
        )
        self._ami_mrc_exempted_edps = ami_mrc_exempted_edps or []

    def _process_basic_report(
            self, basic_report: basic_report_pb2.BasicReport) -> bool:
        """Processes a single basic report.

        This method calls the post-processor to correct the report results. If
        successful, it updates the report with the processed values. If an
        error occurs, it marks the report as FAILED.

        Args:
            basic_report: The basic_report_pb2.BasicReport to process.

        Returns:
            True if the report was processed successfully, False otherwise.
        """
        succeeded = True

        try:
            logging.info(
                "Processing report %s", basic_report.external_report_result_id
            )
            add_processed_result_values_request = self._post_processor.process(
                basic_report.cmms_measurement_consumer_id,
                basic_report.external_report_result_id,
                self._ami_mrc_exempted_edps,
            )
        except Exception:
            # The post-processor itself (solver, data parsing) failed. This
            # BasicReport cannot be processed; mark it FAILED so downstream
            # consumers don't wait forever.
            logging.warning(
                "Failed to process BasicReport %s for MeasurementConsumer %s",
                basic_report.external_basic_report_id,
                basic_report.cmms_measurement_consumer_id,
                exc_info=True,
            )
            self._basic_reports_stub.FailBasicReport(
                basic_reports_service_pb2.FailBasicReportRequest(
                    cmms_measurement_consumer_id=basic_report.
                    cmms_measurement_consumer_id,
                    external_basic_report_id=basic_report.
                    external_basic_report_id,
                ))
            return False

        if not add_processed_result_values_request:
            return succeeded

        logging.info(
            "Updating ReportResult %s",
            basic_report.external_report_result_id,
        )
        try:
            self._report_results_stub.AddProcessedResultValues(
                add_processed_result_values_request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                # FAILED_PRECONDITION can mean (a) the BasicReport state is no
                # longer UNPROCESSED_RESULTS_READY -- typically because another
                # writer already advanced it -- in which case the work is
                # done and we should skip silently, or (b) a real data
                # integrity error such as a missing ReportingSetResult /
                # ReportingWindowResult, which we must not swallow. Disambig-
                # uate by reading the current BasicReport state.
                if self._is_basic_report_past_unprocessed_results_ready(
                        basic_report):
                    logging.info(
                        "Skipping BasicReport %s for MeasurementConsumer %s:"
                        " already advanced past UNPROCESSED_RESULTS_READY (%s)",
                        basic_report.external_basic_report_id,
                        basic_report.cmms_measurement_consumer_id,
                        e.details(),
                    )
                    return True
                # State precondition was NOT the cause -- fall through to
                # treat as a real failure (e.g. missing ReportingSetResult).
                logging.warning(
                    "AddProcessedResultValues failed for BasicReport %s,"
                    " MeasurementConsumer %s with FAILED_PRECONDITION but"
                    " state has not advanced; marking FAILED",
                    basic_report.external_basic_report_id,
                    basic_report.cmms_measurement_consumer_id,
                    exc_info=True,
                )
                self._basic_reports_stub.FailBasicReport(
                    basic_reports_service_pb2.FailBasicReportRequest(
                        cmms_measurement_consumer_id=basic_report.
                        cmms_measurement_consumer_id,
                        external_basic_report_id=basic_report.
                        external_basic_report_id,
                    ))
                return False
            # Any other gRPC error (UNAVAILABLE, DEADLINE_EXCEEDED, etc.) is
            # treated as transient. Leave the BasicReport in
            # UNPROCESSED_RESULTS_READY so the next tick can retry; do not
            # mark it FAILED.
            logging.warning(
                "Transient failure updating ReportResult for BasicReport %s,"
                " MeasurementConsumer %s; will retry next tick",
                basic_report.external_basic_report_id,
                basic_report.cmms_measurement_consumer_id,
                exc_info=True,
            )
            return False

        return succeeded

    _STATES_PAST_UNPROCESSED_RESULTS_READY = frozenset({
        basic_report_pb2.BasicReport.State.SUCCEEDED,
        basic_report_pb2.BasicReport.State.FAILED,
    })

    def _is_basic_report_past_unprocessed_results_ready(
            self, basic_report: basic_report_pb2.BasicReport) -> bool:
        """Returns True if the BasicReport's current state is SUCCEEDED or
        FAILED, i.e. it has already moved past UNPROCESSED_RESULTS_READY and
        no further work from this job is appropriate.

        On any error reading the state (network failure, NOT_FOUND, etc.),
        returns False so the caller falls through to its normal error path.
        """
        try:
            current = self._basic_reports_stub.GetBasicReport(
                basic_reports_service_pb2.GetBasicReportRequest(
                    cmms_measurement_consumer_id=basic_report.
                    cmms_measurement_consumer_id,
                    external_basic_report_id=basic_report.
                    external_basic_report_id,
                ))
        except grpc.RpcError:
            logging.warning(
                "Failed to read BasicReport %s state for MeasurementConsumer"
                " %s while disambiguating FAILED_PRECONDITION",
                basic_report.external_basic_report_id,
                basic_report.cmms_measurement_consumer_id,
                exc_info=True,
            )
            return False
        return (current.state in
                self._STATES_PAST_UNPROCESSED_RESULTS_READY)

    def execute(self) -> bool:
        """Runs the post-processing job.

        This method lists all basic reports in the UNPROCESSED_RESULTS_READY
        state, iterates through them, and attempts to process each one. It
        handles pagination to ensure all reports are processed.

        Returns:
            True if all reports were processed successfully, False otherwise.
        """
        job_succeeded = True

        # Initial requests.
        basic_reports_request = basic_reports_service_pb2.ListBasicReportsRequest(
            page_size=_MAX_PAGE_SIZE,
            filter=basic_reports_service_pb2.ListBasicReportsRequest.Filter(
                state=basic_report_pb2.BasicReport.State.UNPROCESSED_RESULTS_READY
            ),
        )

        while True:
            response = self._basic_reports_stub.ListBasicReports(
                basic_reports_request)

            # Processes basic report in this page.
            for report in response.basic_reports:
                if not self._process_basic_report(report):
                    job_succeeded = False

            # Gets the request for the next page or breaks when there is no
            # more basic reports to process.
            if response.HasField("next_page_token"):
                basic_reports_request.page_token.CopyFrom(
                    response.next_page_token)
            else:
                break

        return job_succeeded
