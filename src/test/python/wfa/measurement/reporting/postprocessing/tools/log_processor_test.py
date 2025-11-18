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

import tools.log_processor
import unittest
from google.api_core.exceptions import NotFound
from google.protobuf import json_format
from io import StringIO
from wfa.measurement.internal.reporting.postprocessing import report_post_processor_result_pb2
from unittest.mock import MagicMock
from unittest.mock import patch

ReportPostProcessorResult = report_post_processor_result_pb2.ReportPostProcessorResult


class LogProcessorTest(unittest.TestCase):
  def setUp(self):
    # Creates the mock objects.
    self.mock_storage_client = MagicMock()
    self.mock_bucket = MagicMock()
    self.mock_blob = MagicMock()

    # Patches google.cloud.storage.Client
    self.patcher_storage_client = patch(
        'tools.log_processor.storage.Client',
        new=self.mock_storage_client
    )
    self.patcher_storage_client.start()

    # Configures the mock chain: Client() -> .bucket() -> .blob()
    self.mock_storage_client.return_value.bucket.return_value = self.mock_bucket
    self.mock_bucket.blob.return_value = self.mock_blob

    # Patches sys.stdout to capture printed output.
    self.held_stdout = StringIO()
    self.patcher_stdout = patch('sys.stdout', new=self.held_stdout)
    self.patcher_stdout.start()

    # Patches sys.stderr to capture error output.
    self.held_stderr = StringIO()
    self.patcher_stderr = patch('sys.stderr', new=self.held_stderr)
    self.patcher_stderr.start()

    # Patches absl.flags.FLAGS and absl.logging.set_verbosity.
    self.mock_flags = MagicMock()
    self.mock_flags.logtostderr = False
    self.mock_flags.debug = False
    self.mock_flags.bucket_name = None
    self.mock_flags.blob_key = None

    self.patcher_flags = patch('tools.log_processor.FLAGS', new=self.mock_flags)
    self.patcher_flags.start()

    self.patcher_logging_set_verbosity = patch(
        'tools.log_processor.logging.set_verbosity')
    self.mock_logging_set_verbosity = self.patcher_logging_set_verbosity.start()

  # Cleans up patches after each test.
  def tearDown(self):
    self.patcher_storage_client.stop()
    self.patcher_stdout.stop()
    self.patcher_stderr.stop()
    self.patcher_flags.stop()
    self.patcher_logging_set_verbosity.stop()

  def _call_main_with_args(self, bucket_name, blob_key, debug=False):
    # Sets the flag values on the mock FLAGS object.
    self.mock_flags.bucket_name = bucket_name
    self.mock_flags.blob_key = blob_key
    self.mock_flags.debug = debug

    # Calls the main function with FLAGS already populated.
    tools.log_processor.main()

  def test_log_processor_prints_correct_result_on_success(self):
    # Prepares mock data.
    report_post_processor_result_json = \
      """
      {
        "preCorrectionReportSummary": {
            "measurementDetails": [
                {
                    "measurementPolicy": "ami",
                    "setOperation": "cumulative",
                    "isCumulative": true,
                    "dataProviders": [
                        "edp1"
                    ],
                    "measurementResults": [
                        {
                            "reach": {
                                "standardDeviation": 137708.79990420336
                            },
                            "metric": "cumulative/ami/edp1_00"
                        },
                        {
                            "reach": {
                                "standardDeviation": 137708.79990420336
                            },
                            "metric": "cumulative/ami/edp1_01"
                        }
                    ]
                },
                {
                    "measurementPolicy": "ami",
                    "setOperation": "cumulative",
                    "isCumulative": true,
                    "dataProviders": [
                        "edp1",
                        "edp2"
                    ],
                    "leftHandSideTargets": [
                        "edp1",
                        "edp2"
                    ],
                    "measurementResults": [
                        {
                            "reach": {
                                "value": "100",
                                "standardDeviation": 184302.26284602462
                            },
                            "metric": "cumulative/ami/edp1_edp2_00"
                        },
                        {
                            "reach": {
                                "value": "182300",
                                "standardDeviation": 197680.09530394306
                            },
                            "metric": "cumulative/ami/edp1_edp2_01"
                        }
                    ]
                },
                {
                    "measurementPolicy": "ami",
                    "setOperation": "cumulative",
                    "isCumulative": true,
                    "dataProviders": [
                        "edp2"
                    ],
                    "measurementResults": [
                        {
                            "reach": {
                                "value": "168600",
                                "standardDeviation": 137769.39054605988
                            },
                            "metric": "cumulative/ami/edp2_00"
                        },
                        {
                            "reach": {
                                "value": "101700",
                                "standardDeviation": 137745.3515414703
                            },
                            "metric": "cumulative/ami/edp2_01"
                        }
                    ]
                }
            ]
        },
        "updatedMeasurements": {
            "cumulative/ami/edp1_edp2_00": "58116",
            "cumulative/ami/edp1_edp2_01": "97598",
            "cumulative/ami/edp1_00": "39482",
            "cumulative/ami/edp1_01": "39482",
            "cumulative/ami/edp2_00": "58116",
            "cumulative/ami/edp2_01": "58116",
            "reach_and_frequency/ami/edp1_edp2": "97598",
            "reach_and_frequency/ami/edp1": "39482",
            "reach_and_frequency/ami/edp2": "58116"
        },
        "status": {
            "statusCode": "SOLUTION_FOUND_WITH_HIGHS",
            "primalEqualityResidual": 1.4551915228366852e-10,
            "primalInequalityResidual": 1.0294648262315833e-11
        },
        "preCorrectionQuality": {},
        "postCorrectionQuality": {}
      }
      """
    report_post_processor_result = ReportPostProcessorResult()
    json_format.Parse(report_post_processor_result_json,
                      report_post_processor_result,
                      ignore_unknown_fields=False)

    # Serializes the report post processor result to bytes, as this is what
    # download_as_bytes() would return.
    mock_protobuf_binary_data = report_post_processor_result.SerializeToString()

    # Configures the mock GCS blob to return the mock binary data.
    self.mock_blob.download_as_bytes.return_value = mock_protobuf_binary_data

    # Defines expected print output.
    expected_printed_output = str(report_post_processor_result) + "\n"

    # Patches sys.exit to prevent the test from actually exiting.
    with patch('sys.exit') as mock_exit:
      # Calls the main function with provided bucket name and blob key.
      bucket_name = "test-bucket"
      blob_key = "20250525/20250525_report.textproto"
      self._call_main_with_args(
          bucket_name=bucket_name,
          blob_key=blob_key,
          debug=False
      )

      # Verifies GCS client interactions.
      self.mock_storage_client.assert_called_once()
      self.mock_storage_client.return_value.bucket.assert_called_once_with(
          bucket_name)
      self.mock_bucket.blob.assert_called_once_with(blob_key)
      self.mock_blob.download_as_bytes.assert_called_once()

      # Verifies stdout capture.
      captured_output = self.held_stdout.getvalue()
      self.assertEqual(captured_output, expected_printed_output)

      # Verifies no error output.
      self.assertEqual(self.held_stderr.getvalue(), "")

      # Verifies logging set_up was called.
      self.mock_flags.logtostderr = True
      self.mock_logging_set_verbosity.assert_called_once_with(
          tools.log_processor.logging.INFO)

      # Verifies sys.exit was called with success code.
      mock_exit.assert_called_once_with(0)

  def test_log_processor_handles_blob_not_found(self):
    # Configure GCS mock to raise NotFound.
    self.mock_blob.download_as_bytes.side_effect = NotFound("Blob not found.")

    # Patches sys.exit to prevent the test from actually exiting.
    with patch('sys.exit') as mock_exit:
      # Calls the main function with arguments.
      bucket_name = "non-existent-bucket"
      blob_key = "non-existent-blob"
      self._call_main_with_args(
          bucket_name=bucket_name,
          blob_key=blob_key,
          debug=False
      )

      # Verifies error message was printed to stderr.
      expected_error_output = \
        f"Error processing report: The blob key {blob_key} does not exist.\n"
      self.assertEqual(self.held_stderr.getvalue(), expected_error_output)
      self.assertEqual(self.held_stdout.getvalue(), "")

      # Verifies sys.exit was called with error code.
      mock_exit.assert_called_once_with(1)

  def test_log_processor_handles_generic_error(self):
    # Configures GCS mock to raise a generic Exception
    self.mock_blob.download_as_bytes.side_effect = Exception(
        "Error downloading."
    )

    with patch('sys.exit') as mock_exit:
      bucket_name = "error-bucket"
      blob_key = "error-blob"
      self._call_main_with_args(
          bucket_name=bucket_name,
          blob_key=blob_key,
          debug=False
      )

      expected_error_output = f"An error occurred: Error downloading.\n"
      self.assertEqual(self.held_stderr.getvalue(), expected_error_output)
      self.assertEqual(self.held_stdout.getvalue(), "")

      mock_exit.assert_called_once_with(1)


if __name__ == '__main__':
  unittest.main()
