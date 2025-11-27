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

from src.main.python.wfa.measurement.reporting.postprocessing.tools.post_process_report_result import (
    PostProcessReportResult, )
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

    def test_process_success(self):
        # Configures the mock stubs to return the data from the textproto files.
        self.mock_report_results_stub.ListReportingSetResults.return_value = (
            self.mock_list_reporing_set_results_response)
        self.mock_reporting_sets_stub.BatchGetReportingSets.return_value = (
            self.mock_batch_get_reporting_set_response)

        report_result_processor = PostProcessReportResult(
            self.mock_report_results_stub, self.mock_reporting_sets_stub)

        add_denoised_request = report_result_processor.process(
            self.cmms_measurement_consumer_id, self.external_report_result_id)

        # Verify the gRPC stubs were called correctly.
        self.mock_report_results_stub.ListReportingSetResults.assert_called_once_with(
            report_results_service_pb2.ListReportingSetResultsRequest(
                cmms_measurement_consumer_id=self.cmms_measurement_consumer_id,
                external_report_result_id=self.external_report_result_id,
                view=report_result_pb2.ReportingSetResultView.
                REPORTING_SET_RESULT_VIEW_NOISY,
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
                'edp1',
                'edp2',
                'edp3',
                'edp1_edp2',
                'edp1_edp2_edp3',
            ],
        )

        # Verify the AddDenoisedResultValuesRequest.
        self.assertIsNotNone(add_denoised_request)
        self.assertEqual(
            add_denoised_request.cmms_measurement_consumer_id,
            self.cmms_measurement_consumer_id,
        )
        self.assertEqual(
            add_denoised_request.external_report_result_id,
            self.external_report_result_id,
        )

        print(add_denoised_request.reporting_set_results)

        # There are 25 reporting set results in the sample data.
        self.assertEqual(len(add_denoised_request.reporting_set_results), 25)


if __name__ == "__main__":
    unittest.main()
