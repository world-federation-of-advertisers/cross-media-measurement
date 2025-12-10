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

    def _list_unprocessed_basic_reports(
            self
        ) -> Iterable[basic_report_pb2.BasicReport]:
        """Lists all basic reports with status UNPROCESSED_RESULTS_READY."""
        request = basic_reports_service_pb2.ListBasicReportsRequest(
            page_size=_MAX_PAGE_SIZE,
            filter=basic_reports_service_pb2.ListBasicReportsRequest.Filter(
                state=basic_report_pb2.BasicReport.State.
                UNPROCESSED_RESULTS_READY))

        all_reports = []
        while True:
            response = self._basic_reports_stub.ListBasicReports(request)
            all_reports.extend(response.basic_reports)
            if not response.HasField("next_page_token"):
                break
            request.page_token.CopyFrom(response.next_page_token)

        return all_reports

    def execute(self) -> bool:
        """Runs the post-processing job.

        Returns:
            True if all reports were processed successfully, False otherwise.
        """
        job_succeeded = True
        basic_reports = self._list_unprocessed_basic_reports()
        for report in basic_reports:
            try:
                logging.info(
                    f"Processing report: {report.external_report_result_id}")
                request = self._post_processor.process(
                    report.cmms_measurement_consumer_id,
                    report.external_report_result_id,
                )
                if request:
                    logging.info(
                        f"Updating report: {report.external_report_result_id}")
                    self._report_results_stub.AddProcessedResultValues(request)
            except Exception:
                logging.warning(
                    f"Failed to process  basic report {report.external_basic_report_id}"
                    f" for measurement consumer {report.cmms_measurement_consumer_id}."
                )
                self._basic_reports_stub.FailBasicReport(
                    basic_reports_service_pb2.FailBasicReportRequest(
                        cmms_measurement_consumer_id=report.cmms_measurement_consumer_id,
                        external_basic_report_id=report.external_basic_report_id,
                    )
                )
                job_succeeded = False
        return job_succeeded
