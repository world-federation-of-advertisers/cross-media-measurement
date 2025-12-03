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
import grpc

from wfa.measurement.internal.reporting.v2 import basic_report_pb2
from wfa.measurement.internal.reporting.v2 import basic_reports_service_pb2
from wfa.measurement.internal.reporting.v2 import basic_reports_service_pb2_grpc
from wfa.measurement.internal.reporting.v2 import report_results_service_pb2_grpc
from wfa.measurement.internal.reporting.v2 import reporting_sets_service_pb2_grpc
from tools import (
    post_process_report_result, )


class PostProcessReportResultJob:
    """A job for fetching, correcting, and updating a report."""

    def __init__(
        self,
        report_results_channel: grpc.Channel,
        reporting_sets_channel: grpc.Channel,
        basic_reports_channel: grpc.Channel,
    ):
        self._report_results_stub = (report_results_service_pb2_grpc.
                                     ReportResultsStub(report_results_channel))
        self._reporting_sets_stub = (reporting_sets_service_pb2_grpc.
                                     ReportingSetsStub(reporting_sets_channel))
        self._basic_reports_stub = (basic_reports_service_pb2_grpc.
                                    BasicReportsStub(basic_reports_channel))
        self._post_processor = post_process_report_result.PostProcessReportResult(
            self._report_results_stub, self._reporting_sets_stub)

    def _list_unprocessed_basic_reports(self):
        """Lists all basic reports with status UNPROCESSED_RESULTS_READY."""
        request = basic_reports_service_pb2.ListBasicReportsRequest(
            filter=basic_reports_service_pb2.ListBasicReportsRequest.Filter(
                state=basic_report_pb2.BasicReport.State.
                UNPROCESSED_RESULTS_READY))
        response = self._basic_reports_stub.ListBasicReports(request)
        return response.basic_reports

    def execute(self):
        """Runs the post-processing job."""
        basic_reports = self._list_unprocessed_basic_reports()
        for report in basic_reports:
            logging.info(
                f"Processing report: {report.external_report_result_id}")
            request = self._post_processor.process(
                report.cmms_measurement_consumer_id,
                report.external_report_result_id,
            )
            if request:
                self._report_results_stub.AddProcessedResultValues(request)
