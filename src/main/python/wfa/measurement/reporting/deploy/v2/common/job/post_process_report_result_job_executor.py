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

import os
import grpc

from absl import app
from absl import flags
from absl import logging

from job import post_process_report_result_job

_KINGDOM_INTERNAL_API_TARGET = flags.DEFINE_string(
    "kingdom_internal_api_target",
    None,
    "The target for the Kingdom internal API.",
    required=True,
)

_TLS_CLIENT_CERT_FILE = flags.DEFINE_string(
    "tls_client_cert_file",
    None,
    "The path to the TLS certificate file, used to identify this client.",
    required=True,
)
_TLS_CLIENT_KEY_FILE = flags.DEFINE_string(
    "tls_client_key_file",
    None,
    "The path to the TLS private key file for this client's cert.",
    required=True,
)
_TLS_ROOT_CA_CERT_FILE = flags.DEFINE_string(
    "tls_root_ca_cert_file",
    None,
    "The path to the CA certificate file for validating the server's cert.",
    required=True)


def _get_secure_credentials(
        tls_client_key_path: str, tls_client_cert_path: str,
        tls_root_ca_cert_path: str) -> grpc.ChannelCredentials:
    """Creates secure gRPC channel credentials."""
    try:
        with open(tls_client_key_path, "rb") as f:
            private_key = f.read()
    except IOError as e:
        raise ValueError(
            f"Error reading TLS client key from {tls_client_key_path}") from e

    try:
        with open(tls_client_cert_path, "rb") as f:
            certificate_chain = f.read()
    except IOError as e:
        raise ValueError(
            f"Error reading TLS client cert from {tls_client_cert_path}"
        ) from e

    try:
        with open(tls_root_ca_cert_path, "rb") as f:
            root_certificates = f.read()
    except IOError as e:
        raise ValueError(
            f"Error reading TLS root CA cert from {tls_root_ca_cert_path}"
        ) from e

    return grpc.ssl_channel_credentials(
        root_certificates=root_certificates,
        private_key=private_key,
        certificate_chain=certificate_chain,
    )


def _create_secure_channel(
        target: str, credentials: grpc.ChannelCredentials) -> grpc.Channel:
    """Creates a secure gRPC channel."""
    return grpc.secure_channel(target, credentials)


def main(argv):
    if len(argv) > 1:
        raise app.UsageError("Too many command-line arguments.")

    # Parses flags.
    flags.FLAGS(argv)

    kingdom_internal_api_target = _KINGDOM_INTERNAL_API_TARGET.value
    if not kingdom_internal_api_target:
        raise app.UsageError("kingdom_internal_api_target must be non-empty.")

    tls_client_cert_file = _TLS_CLIENT_CERT_FILE.value
    if not os.path.exists(tls_client_cert_file):
        raise app.UsageError(
            f"TLS client cert file not found at {tls_client_cert_file}")

    tls_client_key_file = _TLS_CLIENT_KEY_FILE.value
    if not os.path.exists(tls_client_key_file):
        raise app.UsageError(
            f"TLS client key file not found at {tls_client_key_file}")

    tls_root_ca_cert_file = _TLS_ROOT_CA_CERT_FILE.value
    if not os.path.exists(tls_root_ca_cert_file):
        raise app.UsageError(
            f"TLS root CA cert file not found at {tls_root_ca_cert_file}")

    credentials = _get_secure_credentials(tls_client_key_file,
                                          tls_client_cert_file,
                                          tls_root_ca_cert_file)

    kingdom_internal_api_channel = None
    try:
        kingdom_internal_api_channel = _create_secure_channel(
            kingdom_internal_api_target, credentials)

        job = post_process_report_result_job.PostProcessReportResultJob(
            kingdom_internal_api_channel)

        job.execute()
    finally:
        if kingdom_internal_api_channel:
            kingdom_internal_api_channel.close()


if __name__ == "__main__":
    app.run(main)
