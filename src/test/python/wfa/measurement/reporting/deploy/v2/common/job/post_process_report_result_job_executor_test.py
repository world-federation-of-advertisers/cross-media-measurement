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

import os
import unittest
from absl.testing import parameterized
from unittest import mock
from absl import flags
from absl.testing import flagsaver

from job import post_process_report_result_job_executor
from job import post_process_report_result_job


class PostProcessReportResultJobExecutorTest(parameterized.TestCase):

    @flagsaver.flagsaver(
        internal_api_target="internal_api_target",
        tls_cert_file="client_cert",
        tls_key_file="client_key",
        cert_collection_file="root_ca_cert",
        internal_api_cert_host="cert_host",
    )
    @mock.patch("os.path.exists", return_value=True)
    @mock.patch("job.post_process_report_result_job.PostProcessReportResultJob")
    @mock.patch(
        "job.post_process_report_result_job_executor._get_secure_credentials"
    )
    @mock.patch(
        "job.post_process_report_result_job_executor._create_secure_channel"
    )
    def test_post_process_report_result_with_internal_api_cert_host_success(
        self,
        mock_create_channel,
        mock_get_credentials,
        mock_job_class,
        mock_exists,
    ):
        # Sets up mock objects.
        mock_credentials = mock.MagicMock()
        mock_get_credentials.return_value = mock_credentials
        mock_job_instance = mock.MagicMock()
        mock_job_class.return_value = mock_job_instance
        mock_channel_instance = mock.MagicMock(name="secure_channel_instance")
        mock_create_channel.return_value.__enter__.return_value = mock_channel_instance

        # Calls the main function.
        post_process_report_result_job_executor.main(["test_main"])

        # Verifies the expected behavior.
        mock_exists.assert_has_calls([
            mock.call("client_cert"),
            mock.call("client_key"),
            mock.call("root_ca_cert"),
        ])
        mock_get_credentials.assert_called_once_with("client_key",
                                                     "client_cert",
                                                     "root_ca_cert")
        expected_options = [("grpc.ssl_target_name_override", "cert_host")]
        mock_create_channel.assert_called_once_with(
            "internal_api_target", mock_credentials, expected_options
        )

        mock_job_class.assert_called_once_with(mock_channel_instance)
        mock_job_instance.execute.assert_called_once()

    @flagsaver.flagsaver(
        internal_api_target="internal_api_target",
        tls_cert_file="client_cert",
        tls_key_file="client_key",
        cert_collection_file="root_ca_cert",
    )
    @mock.patch("os.path.exists", return_value=True)
    @mock.patch("job.post_process_report_result_job.PostProcessReportResultJob")
    @mock.patch(
        "job.post_process_report_result_job_executor._get_secure_credentials"
    )
    @mock.patch(
        "job.post_process_report_result_job_executor._create_secure_channel"
    )
    def test_post_process_report_result_job_executor_no_internal_api_cert_host_success(
            self, mock_create_channel, mock_get_credentials, mock_job_class,
            mock_exists):
        # Sets up mock objects.
        mock_credentials = mock.MagicMock()
        mock_get_credentials.return_value = mock_credentials
        mock_job_instance = mock.MagicMock()
        mock_job_class.return_value = mock_job_instance
        mock_channel_instance = mock.MagicMock(name="secure_channel_instance")
        mock_create_channel.return_value.__enter__.return_value = mock_channel_instance

        # Calls the main function.
        post_process_report_result_job_executor.main(["test_main"])

        # Verifies the expected behavior.
        mock_exists.assert_has_calls([
            mock.call("client_cert"),
            mock.call("client_key"),
            mock.call("root_ca_cert"),
        ])
        mock_get_credentials.assert_called_once_with("client_key",
                                                     "client_cert",
                                                     "root_ca_cert")
        mock_create_channel.assert_called_once_with(
            "internal_api_target", mock_credentials, []
        )
        mock_job_class.assert_called_once_with(mock_channel_instance)
        mock_job_instance.execute.assert_called_once()

    @parameterized.named_parameters(
        ("missing_internal_api_target_flag", "internal_api_target"),
        ("missing_tls_cert_file_flag", "tls_cert_file"),
        ("missing_tls_key_file_flag", "tls_key_file"),
        ("missing_cert_collection_file_flag", "cert_collection_file"),
    )
    @flagsaver.flagsaver(
        internal_api_target="internal_api_target",
        tls_cert_file="client_cert",
        tls_key_file="client_key",
        cert_collection_file="root_ca_cert",
    )
    def test_post_process_report_result_job_executor_missing_flag_raises_error(
            self, flag_to_omit):
        flags.FLAGS[flag_to_omit].value = None

        with self.assertRaises(flags.Error):
            post_process_report_result_job_executor.main(["test_program"])

    @parameterized.named_parameters(
        ("missing_tls_cert", "tls_cert_file", "client_cert"),
        ("missing_tls_key", "tls_key_file", "client_key"),
        ("missing_cert_collection", "cert_collection_file", "root_ca_cert"),
    )
    @flagsaver.flagsaver(
        internal_api_target="internal_api_target",
        tls_cert_file="client_cert",
        tls_key_file="client_key",
        cert_collection_file="root_ca_cert",
    )
    @mock.patch("os.path.exists")
    def test_post_process_report_result_job_executor_raises_error_if_cert_not_found(
            self, flag_to_make_missing, missing_file_path, mock_exists):
        mock_exists.side_effect = lambda path: path != missing_file_path
        with self.assertRaises(ValueError):
            post_process_report_result_job_executor.main(["test_program"])


if __name__ == "__main__":
    unittest.main()
