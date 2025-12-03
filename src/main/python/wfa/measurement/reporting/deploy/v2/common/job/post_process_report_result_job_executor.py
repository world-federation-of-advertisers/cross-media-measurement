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
"""A one-shot job for fetching, correcting, and updating a report."""

import grpc

from absl import app
from absl import flags
from absl import logging

from job import post_process_report_result_job

_REPORT_RESULTS_TARGET = flags.DEFINE_string(
    "report_results_target",
    None,
    "The target for the ReportResults service.",
    required=True,
)
_REPORTING_SETS_TARGET = flags.DEFINE_string(
    "reporting_sets_target",
    None,
    "The target for the ReportingSets service.",
    required=True,
)
_BASIC_REPORTS_TARGET = flags.DEFINE_string(
    "basic_reports_target",
    None,
    "The target for the BasicReports service.",
    required=True,
)

_TLS_CERT_FILE = flags.DEFINE_string("tls_cert_file",
                                     None,
                                     "The path to the TLS certificate file.",
                                     required=True)
_TLS_KEY_FILE = flags.DEFINE_string("tls_key_file",
                                    None,
                                    "The path to the TLS private key file.",
                                    required=True)
_TLS_CA_CERT_FILE = flags.DEFINE_string(
    "tls_ca_cert_file",
    None,
    "The path to the CA certificate file for server validation.",
    required=True)


def _get_secure_credentials() -> grpc.ChannelCredentials:
    """Creates secure gRPC channel credentials."""
    with open(_TLS_KEY_FILE.value, "rb") as f:
        private_key = f.read()
    with open(_TLS_CERT_FILE.value, "rb") as f:
        certificate_chain = f.read()
    with open(_TLS_CA_CERT_FILE.value, "rb") as f:
        root_certificates = f.read()
    return grpc.ssl_channel_credentials(
        root_certificates=root_certificates,
        private_key=private_key,
        certificate_chain=certificate_chain,
    )


def _create_secure_channel(target: str, credentials) -> grpc.Channel:
    """Creates a secure gRPC channel."""
    return grpc.secure_channel(target, credentials)


def main(argv):
    # The program doesn't take any arguments other than the required flags.
    if len(argv) > 1:
        raise app.UsageError("Too many command-line arguments.")

    # Parses flags.
    flags.FLAGS(argv)

    credentials = _get_secure_credentials()

    report_results_channel = _create_secure_channel(
        _REPORT_RESULTS_TARGET.value, credentials)

    reporting_sets_channel = _create_secure_channel(
        _REPORTING_SETS_TARGET.value, credentials)

    basic_reports_channel = _create_secure_channel(
        _BASIC_REPORTS_TARGET.value, credentials)

    job = post_process_report_result_job.PostProcessReportResultJob(
        report_results_channel, reporting_sets_channel, basic_reports_channel)

    job.execute()


if __name__ == "__main__":
    app.run(main)
