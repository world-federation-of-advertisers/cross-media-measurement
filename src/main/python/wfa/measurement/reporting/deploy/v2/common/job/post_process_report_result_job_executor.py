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

_INTERNAL_API_TARGET = flags.DEFINE_string(
    "internal_api_target",
    None,
    "The target for the internal reporting API.",
    required=True,
)
flags.DEFINE_alias("internal-api-target", "internal_api_target")
_INTERNAL_API_CERT_HOST = flags.DEFINE_string(
    "internal_api_cert_host",
    None,
    "Expected hostname (DNS-ID) in the internal reporting API server's TLS"
    " certificate. This overrides derivation of the TLS DNS-ID from"
    " internal_api_target.",
    required=False,
)
flags.DEFINE_alias("internal-api-cert-host", "internal_api_cert_host")
_TLS_CERT_FILE = flags.DEFINE_string(
    "tls_cert_file",
    None,
    "The path to the TLS certificate file, used to identify this client.",
    required=True,
)
flags.DEFINE_alias("tls-cert-file", "tls_cert_file")
_TLS_KEY_FILE = flags.DEFINE_string(
    "tls_key_file",
    None,
    "The path to the TLS private key file for this client's cert.",
    required=True,
)
flags.DEFINE_alias("tls-key-file", "tls_key_file")
_CERT_COLLECTION_FILE = flags.DEFINE_string(
    "cert_collection_file",
    None,
    "The path to the certificate collection file for validating the server's cert.",
    required=True)
flags.DEFINE_alias("cert-collection-file", "cert_collection_file")


def _get_secure_credentials(
        tls_key_path: str, tls_cert_path: str,
        cert_collection_path: str) -> grpc.ChannelCredentials:
    """Creates secure gRPC channel credentials."""
    logging.info("Get secure credentials.")
    try:
        with open(tls_key_path, "rb") as f:
            private_key = f.read()
    except IOError as e:
        raise ValueError(
            f"Error reading TLS client key from {tls_key_path}") from e

    try:
        with open(tls_cert_path, "rb") as f:
            certificate_chain = f.read()
    except IOError as e:
        raise ValueError(f"Error reading TLS cert from {tls_cert_path}") from e

    try:
        with open(cert_collection_path, "rb") as f:
            root_certificates = f.read()
    except IOError as e:
        raise ValueError(
            f"Error reading cert collection from {cert_collection_path}"
        ) from e

    return grpc.ssl_channel_credentials(
        root_certificates=root_certificates,
        private_key=private_key,
        certificate_chain=certificate_chain,
    )


def _create_secure_channel(
    target: str,
    credentials: grpc.ChannelCredentials,
    channel_options: list[tuple[str, str]] | None = None,
) -> grpc.Channel:
    """Creates a secure gRPC channel."""
    return grpc.secure_channel(target, credentials, options=channel_options)


def main(argv):
    if len(argv) > 1:
        raise app.UsageError("Too many command-line arguments.")

    # Parses flags.
    flags.FLAGS(argv)

    internal_api_target = _INTERNAL_API_TARGET.value
    if not internal_api_target:
        raise ValueError("internal_api_target must be non-empty.")

    tls_cert_file = _TLS_CERT_FILE.value
    if not os.path.exists(tls_cert_file):
        raise ValueError(f"TLS cert file not found at {tls_cert_file}")

    tls_key_file = _TLS_KEY_FILE.value
    if not os.path.exists(tls_key_file):
        raise ValueError(f"TLS key file not found at {tls_key_file}")

    cert_collection_file = _CERT_COLLECTION_FILE.value
    if not os.path.exists(cert_collection_file):
        raise ValueError(
            f"The cert collection file not found at {cert_collection_file}")

    channel_options = []
    internal_api_cert_host = _INTERNAL_API_CERT_HOST.value
    if internal_api_cert_host:
        channel_options.append(
            ("grpc.ssl_target_name_override", internal_api_cert_host))

    credentials = _get_secure_credentials(tls_key_file, tls_cert_file,
                                          cert_collection_file)

    try:
        with _create_secure_channel(
            internal_api_target,
            credentials,
            channel_options,
        ) as internal_reporting_channel:
            logging.info("Create PostProcessReportResultJob.")
            job = post_process_report_result_job.PostProcessReportResultJob(
                internal_reporting_channel)

            logging.info("Executing PostProcessReportResultJob.")
            job.execute()
            logging.info("Done executing PostProcessReportResultJob.")
    except grpc.RpcError as e:
        logging.error(f"gRPC call failed: {e}")


if __name__ == "__main__":
    app.run(main)
