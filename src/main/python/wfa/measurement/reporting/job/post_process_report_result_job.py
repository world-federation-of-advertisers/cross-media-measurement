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
    ):
        """Initializes the job with the necessary gRPC stubs.

        Args:
            internal_reporting_channel: A gRPC channel to the internal reporting
                server.
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
        self._post_processor = post_process_report_result.PostProcessReportResult(
            self._report_results_stub, self._reporting_sets_stub)

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
                f"Processing report: {basic_report.external_report_result_id}")
            add_processed_result_values_request = self._post_processor.process(
                basic_report.cmms_measurement_consumer_id,
                basic_report.external_report_result_id,
            )
            if add_processed_result_values_request:
                logging.info(
                    f"Updating report: {basic_report.external_report_result_id}"
                )
                self._report_results_stub.AddProcessedResultValues(
                    add_processed_result_values_request)
        except Exception:
            logging.warning(
                f"Failed to process  basic report {basic_report.external_basic_report_id}"
                f" for measurement consumer {basic_report.cmms_measurement_consumer_id}."
            )
            self._basic_reports_stub.FailBasicReport(
                basic_reports_service_pb2.FailBasicReportRequest(
                    cmms_measurement_consumer_id=basic_report.
                    cmms_measurement_consumer_id,
                    external_basic_report_id=basic_report.
                    external_basic_report_id,
                ))
            succeeded = False

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
                state=basic_report_pb2.BasicReport.State.
                UNPROCESSED_RESULTS_READY))

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
