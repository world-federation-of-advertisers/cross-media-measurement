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

from src.main.proto.wfa.measurement.internal.reporting.postprocessing import (
    report_post_processor_result_pb2,
)
from src.main.python.wfa.measurement.reporting.postprocessing.tools import (
    post_process_report_result,
)
from wfa.measurement.internal.reporting.v2 import (
    report_results_service_pb2,
)
from wfa.measurement.internal.reporting.v2 import (
    reporting_sets_service_pb2,
)


class PostProcessReportResultTest(unittest.TestCase):

    def setUp(self):
        self.mock_report_results_stub = MagicMock()
        self.mock_reporting_sets_stub = MagicMock()
        self.cmms_measurement_consumer_id = "test-mc-id"
        self.external_report_result_id = 12345

    @patch(
        "src.main.python.wfa.measurement.reporting.postprocessing.tools."
        "post_process_report_result.ReportSummaryV2Processor"
    )
    def test_process_success(self, mock_processor):
        # Arrange
        # Mock the GetNoisyResultValues response
        mock_report_result = (
            report_results_service_pb2.GetNoisyResultValuesResponse()
        )
        mock_report_result.reporting_set_results.add(
            key={"external_reporting_set_id": "rs-1"}
        )
        self.mock_report_results_stub.GetNoisyResultValues.return_value = (
            mock_report_result
        )

        # Mock the BatchGetReportingSets response
        mock_batch_get_response = (
            reporting_sets_service_pb2.BatchGetReportingSetsResponse()
        )
        rs1 = mock_batch_get_response.reporting_sets.add(
            external_reporting_set_id="rs-1"
        )
        rs1.composite.expression.operation = (
            reporting_sets_service_pb2.ReportingSet.SetExpression.Operation.UNION
        )
        rs1.composite.expression.lhs.reporting_set = "rs-2"
        rs1.composite.expression.rhs.reporting_set = "rs-3"

        rs2 = mock_batch_get_response.reporting_sets.add(
            external_reporting_set_id="rs-2"
        )
        rs2.primitive.cmms_event_groups.append("edp1-event-group")

        rs3 = mock_batch_get_response.reporting_sets.add(
            external_reporting_set_id="rs-3"
        )
        rs3.primitive.cmms_event_groups.append("edp2-event-group")

        self.mock_reporting_sets_stub.BatchGetReportingSets.return_value = (
            mock_batch_get_response
        )

        # Mock the ReportSummaryV2Processor
        mock_process_result = (
            report_post_processor_result_pb2.ReportPostProcessorResult(
                status=report_post_processor_result_pb2.ReportPostProcessorStatus(
                    status_code=report_post_processor_result_pb2.ReportPostProcessorStatus.SUCCESS
                ),
                updated_measurements={"reach_metric_1": 1000},
            )
        )
        mock_processor.return_value.process.return_value = mock_process_result

        # Instantiate the class under test
        post_processor = post_process_report_result.PostProcessReportResult(
            self.mock_report_results_stub, self.mock_reporting_sets_stub
        )

        # Act
        post_processor.process(
            self.cmms_measurement_consumer_id, self.external_report_result_id
        )

        # Assert
        # Verify GetNoisyResultValues was called
        self.mock_report_results_stub.GetNoisyResultValues.assert_called_once_with(
            report_results_service_pb2.GetNoisyResultValuesRequest(
                cmms_measurement_consumer_id=self.cmms_measurement_consumer_id,
                external_report_result_id=self.external_report_result_id,
            )
        )

        # Verify BatchGetReportingSets was called
        self.mock_reporting_sets_stub.BatchGetReportingSets.assert_called_once()
        batch_get_request = (
            self.mock_reporting_sets_stub.BatchGetReportingSets.call_args[0][0]
        )
        self.assertEqual(
            batch_get_request.cmms_measurement_consumer_id,
            self.cmms_measurement_consumer_id,
        )
        self.assertCountEqual(
            batch_get_request.external_reporting_set_ids, ["rs-1", "rs-2", "rs-3"]
        )

        # Verify AddDenoisedResultValues was called with the correct data
        self.mock_report_results_stub.AddDenoisedResultValues.assert_called_once()
        add_denoised_request = (
            self.mock_report_results_stub.AddDenoisedResultValues.call_args[0][0]
        )

        self.assertEqual(
            add_denoised_request.cmms_measurement_consumer_id,
            self.cmms_measurement_consumer_id,
        )
        self.assertEqual(
            add_denoised_request.external_report_result_id,
            self.external_report_result_id,
        )

        # This part of the test is more complex due to the conversion logic.
        # We'll do a basic check to ensure it was called. A more detailed
        # test would mock report_conversion and verify the inputs to
        # _create_add_denoised_result_values_requests.
        # For now, we know the mock processor returned a measurement, so the
        # request should have been created and sent.
        self.assertTrue(add_denoised_request.reporting_set_results)


if __name__ == "__main__":
    unittest.main()