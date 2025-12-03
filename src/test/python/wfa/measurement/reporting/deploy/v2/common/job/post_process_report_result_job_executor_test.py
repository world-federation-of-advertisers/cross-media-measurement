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

import unittest
from unittest import mock
from absl import flags
from absl.testing import flagsaver

from job import post_process_report_result_job_executor
from job import post_process_report_result_job


class PostProcessReportResultJobExecutorTest(unittest.TestCase):

    @flagsaver.flagsaver(
        report_results_target="report_results_target",
        reporting_sets_target="reporting_sets_target",
        basic_reports_target="basic_reports_target",
        tls_cert_file="tls_cert_file",
        tls_key_file="tls_key_file",
        tls_ca_cert_file="tls_ca_cert_file",
    )
    @mock.patch(
        "job.post_process_report_result_job.PostProcessReportResultJob"
    )
    @mock.patch(
        "job.post_process_report_result_job_executor._get_secure_credentials"
    )
    @mock.patch(
        "job.post_process_report_result_job_executor._create_secure_channel"
    )
    def test_main(self, mock_create_channel, mock_get_credentials, mock_job_class):
        # Arrange
        mock_credentials = mock.MagicMock()
        mock_get_credentials.return_value = mock_credentials
        mock_job_instance = mock.MagicMock()
        mock_job_class.return_value = mock_job_instance

        mock_report_results_channel = mock.MagicMock()
        mock_reporting_sets_channel = mock.MagicMock()
        mock_basic_reports_channel = mock.MagicMock()

        def create_channel_side_effect(target, credentials):
            if target == "report_results_target":
                return mock_report_results_channel
            if target == "reporting_sets_target":
                return mock_reporting_sets_channel
            if target == "basic_reports_target":
                return mock_basic_reports_channel
            return mock.MagicMock()

        mock_create_channel.side_effect = create_channel_side_effect

        # Act
        post_process_report_result_job_executor.main(["test_main"])

        # Assert
        mock_get_credentials.assert_called_once()
        self.assertEqual(mock_create_channel.call_count, 3)
        mock_create_channel.assert_any_call("report_results_target", mock_credentials)
        mock_create_channel.assert_any_call("reporting_sets_target", mock_credentials)
        mock_create_channel.assert_any_call("basic_reports_target", mock_credentials)

        mock_job_class.assert_called_once_with(
            mock_report_results_channel,
            mock_reporting_sets_channel,
            mock_basic_reports_channel,
        )
        mock_job_instance.execute.assert_called_once()

    @flagsaver.flagsaver(
        # Missing --report_results_target
        reporting_sets_target="reporting_sets_target",
        basic_reports_target="basic_reports_target",
        tls_cert_file="tls_cert_file",
        tls_key_file="tls_key_file",
        tls_ca_cert_file="tls_ca_cert_file",
    )
    def test_main_raises_error_with_missing_flag(self):
      with self.assertRaises(flags.Error):
          # In a real execution, absl.app.run would catch this and exit.
          # Here, we call main directly to test the flag parsing behavior.
          post_process_report_result_job_executor.main(["test_program"])


if __name__ == "__main__":
    unittest.main()
