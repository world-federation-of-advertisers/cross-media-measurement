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
from typing import Iterable, Optional
import grpc
from grpc_status import rpc_status

from google.rpc import error_details_pb2
from wfa.measurement.internal.reporting.v2 import basic_report_pb2
from wfa.measurement.internal.reporting.v2 import basic_reports_service_pb2
from wfa.measurement.internal.reporting.v2 import basic_reports_service_pb2_grpc
from wfa.measurement.internal.reporting.v2 import report_results_service_pb2_grpc
from wfa.measurement.internal.reporting.v2 import reporting_sets_service_pb2_grpc
from tools import post_process_report_result

_MAX_PAGE_SIZE = 50

# Domain, reason, and metadata key emitted by the internal reporting server
# when the operation's precondition on BasicReport state fails. See
# org.wfanet.measurement.reporting.service.internal.Errors in the Kotlin
# source.
_INTERNAL_REPORTING_ERROR_DOMAIN = "internal.reporting.halo-cmm.org"
_BASIC_REPORT_STATE_INVALID_REASON = "BASIC_REPORT_STATE_INVALID"
_BASIC_REPORT_STATE_METADATA_KEY = "basicReportState"
_STATES_PAST_UNPROCESSED = frozenset({
    basic_report_pb2.BasicReport.State.Name(
        basic_report_pb2.BasicReport.State.SUCCEEDED
    ),
    basic_report_pb2.BasicReport.State.Name(
        basic_report_pb2.BasicReport.State.FAILED
    ),
})


def _extract_error_info(
    rpc_error: grpc.RpcError,
) -> Optional[error_details_pb2.ErrorInfo]:
    """Returns the first ErrorInfo attached to [rpc_error]'s status details,
    or None if the call did not carry a google.rpc.Status with an ErrorInfo.

    Uses grpcio-status' rpc_status.from_call to parse the
    grpc-status-details-bin trailer; see google.rpc.status.proto and
    google.rpc.error_details.proto for the wire format.
    """
    try:
        status = rpc_status.from_call(rpc_error)
    except (ValueError, TypeError):
        # rpc_status.from_call raises ValueError if the trailer is present but
        # the gRPC status code does not match what's encoded inside, and may
        # raise TypeError for malformed inputs. Either way the trailer is not
        # usable; surface as "no ErrorInfo available" rather than letting the
        # caller treat the error as a non-state-precondition one (which would
        # FAIL the BasicReport).
        logging.warning(
            "Failed to parse google.rpc.Status from grpc-status-details-bin"
            " trailer",
            exc_info=True,
        )
        return None
    if status is None:
        return None
    for detail in status.details:
        if not detail.Is(error_details_pb2.ErrorInfo.DESCRIPTOR):
            continue
        error_info = error_details_pb2.ErrorInfo()
        detail.Unpack(error_info)
        return error_info
    return None


def _basic_report_state_past_unprocessed(
    rpc_error: grpc.RpcError,
) -> Optional[str]:
    """If [rpc_error] carries a BASIC_REPORT_STATE_INVALID ErrorInfo whose
    basicReportState metadata indicates a state past
    UNPROCESSED_RESULTS_READY (SUCCEEDED or FAILED), returns that state name.
    Otherwise returns None.
    """
    error_info = _extract_error_info(rpc_error)
    if error_info is None:
        return None
    if error_info.domain != _INTERNAL_REPORTING_ERROR_DOMAIN:
        return None
    if error_info.reason != _BASIC_REPORT_STATE_INVALID_REASON:
        return None
    state = error_info.metadata.get(_BASIC_REPORT_STATE_METADATA_KEY)
    if state in _STATES_PAST_UNPROCESSED:
        return state
    return None


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
                internal_reporting_channel
            )
        )
        self._reporting_sets_stub = (
            reporting_sets_service_pb2_grpc.ReportingSetsStub(
                internal_reporting_channel
            )
        )
        self._basic_reports_stub = (
            basic_reports_service_pb2_grpc.BasicReportsStub(
                internal_reporting_channel
            )
        )
        self._post_processor = (
            post_process_report_result.PostProcessReportResult(
                self._report_results_stub, self._reporting_sets_stub
            )
        )
        self._ami_mrc_exempted_edps = ami_mrc_exempted_edps or []

    def _process_basic_report(
        self, basic_report: basic_report_pb2.BasicReport
    ) -> bool:
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
                    cmms_measurement_consumer_id=basic_report.cmms_measurement_consumer_id,
                    external_basic_report_id=basic_report.external_basic_report_id,
                )
            )
            return False

        if not add_processed_result_values_request:
            return succeeded

        logging.info(
            "Updating ReportResult %s",
            basic_report.external_report_result_id,
        )
        try:
            self._report_results_stub.AddProcessedResultValues(
                add_processed_result_values_request
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                # FAILED_PRECONDITION can mean (a) the BasicReport state is no
                # longer UNPROCESSED_RESULTS_READY -- typically because another
                # writer already advanced it -- in which case the work is
                # done and we should skip silently, or (b) a real data
                # integrity error such as a missing ReportingSetResult /
                # ReportingWindowResult, which we must not swallow. The server
                # attaches a google.rpc.ErrorInfo with reason and a
                # basicReportState metadata field that disambiguates.
                advanced_state = _basic_report_state_past_unprocessed(e)
                if advanced_state is not None:
                    logging.info(
                        "Skipping BasicReport %s for MeasurementConsumer %s:"
                        " already advanced past UNPROCESSED_RESULTS_READY"
                        " (now %s)",
                        basic_report.external_basic_report_id,
                        basic_report.cmms_measurement_consumer_id,
                        advanced_state,
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
                        cmms_measurement_consumer_id=basic_report.cmms_measurement_consumer_id,
                        external_basic_report_id=basic_report.external_basic_report_id,
                    )
                )
                return False
            # Any other gRPC error (UNAVAILABLE, DEADLINE_EXCEEDED, etc.) is
            # treated as transient. Leave the BasicReport in
            # UNPROCESSED_RESULTS_READY so the next tick can retry; do not
            # mark it FAILED.
            logging.warning(
                "Transient failure (%s) updating ReportResult for BasicReport"
                " %s, MeasurementConsumer %s; will retry next tick",
                e.code().name,
                basic_report.external_basic_report_id,
                basic_report.cmms_measurement_consumer_id,
                exc_info=True,
            )
            return False

        return succeeded

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
                basic_reports_request
            )

            # Processes basic report in this page.
            for report in response.basic_reports:
                if not self._process_basic_report(report):
                    job_succeeded = False

            # Gets the request for the next page or breaks when there is no
            # more basic reports to process.
            if response.HasField("next_page_token"):
                basic_reports_request.page_token.CopyFrom(
                    response.next_page_token
                )
            else:
                break

        return job_succeeded
