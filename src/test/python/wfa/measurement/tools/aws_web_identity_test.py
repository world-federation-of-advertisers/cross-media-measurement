# Copyright 2026 The Cross-Media Measurement Authors
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

import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import aws_web_identity


class FakeProcess:
    """Deterministic command waits; no real clock or subprocess is needed."""

    pid = 123456789

    def __init__(self, token_file, results):
        self.token_file = token_file
        self.results = list(results)
        self.observed_tokens = []
        self.observed_modes = []
        self.returncode = None

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        if self.returncode is not None:
            return self.returncode
        self.observed_tokens.append(self.token_file.read_text())
        self.observed_modes.append(stat.S_IMODE(self.token_file.stat().st_mode))
        result = self.results.pop(0) if self.results else 0
        if isinstance(result, BaseException):
            raise result
        self.returncode = result
        return result


class AwsWebIdentityTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.token_file = self.root / "aws-token"
        self.credentials_file = self.root / "google-credentials.json"
        self.google_credentials = {
            "type": "external_account",
            "audience": "test-wif-audience",
            "credential_source": {
                "headers": {"Authorization": "Bearer fake-job-token"}
            },
            "service_account_impersonation_url": (
                "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
                "test@example.iam.gserviceaccount.com:generateAccessToken"
            ),
        }
        self.credentials_file.write_text(json.dumps(self.google_credentials))
        self.tokens = []
        self.mint_count = 0
        self.mint_failure_at = None

    def mint_token(self, command, **kwargs):
        source_file = Path(kwargs["env"]["CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"])
        source = json.loads(source_file.read_text())
        self.assertNotIn("service_account_impersonation_url", source)
        self.assertEqual(
            source["credential_source"], self.google_credentials["credential_source"]
        )
        self.assertEqual(stat.S_IMODE(source_file.stat().st_mode), 0o600)
        self.assertEqual(
            command,
            [
                "gcloud",
                "auth",
                "print-identity-token",
                "--impersonate-service-account=test@example.iam.gserviceaccount.com",
                "--audiences=test-aws-audience",
                "--format=value(id_token)",
            ],
        )
        self.mint_count += 1
        if self.mint_count == self.mint_failure_at:
            raise subprocess.CalledProcessError(7, command)
        token = f"identity-token-{self.mint_count}"
        self.tokens.append(token)
        return subprocess.CompletedProcess(command, 0, stdout=token + "\n")

    def run_main(self, process):
        with (
            mock.patch.dict(
                os.environ,
                {"GOOGLE_APPLICATION_CREDENTIALS": str(self.credentials_file)},
            ),
            mock.patch.object(
                sys,
                "argv",
                [
                    "aws_web_identity",
                    "--audience=test-aws-audience",
                    f"--token-file={self.token_file}",
                    "--",
                    "seed-command",
                ],
            ),
            mock.patch("subprocess.run", side_effect=self.mint_token),
            mock.patch("subprocess.Popen", return_value=process) as start,
            mock.patch("os.killpg") as stop,
            mock.patch("signal.signal"),
        ):
            result = aws_web_identity.main()
        return result, start, stop

    def test_main_renews_private_tokens_until_command_finishes(self):
        timeout = subprocess.TimeoutExpired("seed-command", 1800)
        process = FakeProcess(self.token_file, [timeout, timeout, 0])
        result, start, stop = self.run_main(process)
        self.assertEqual(result, 0)
        self.assertEqual(process.observed_tokens, self.tokens)
        self.assertEqual(process.observed_modes, [0o600] * 3)
        self.assertEqual(len(self.tokens), 3)
        self.assertEqual(
            json.loads(self.credentials_file.read_text()), self.google_credentials
        )
        start.assert_called_once_with(["seed-command"], start_new_session=True)
        stop.assert_not_called()
        self.assertFalse(self.token_file.exists())

    def test_main_does_not_start_command_when_initial_authentication_fails(self):
        self.mint_failure_at = 1
        result, start, stop = self.run_main(FakeProcess(self.token_file, [0]))
        self.assertEqual(result, 1)
        start.assert_not_called()
        stop.assert_not_called()
        self.assertFalse(self.token_file.exists())

    def test_main_stops_command_when_token_renewal_fails(self):
        self.mint_failure_at = 2
        process = FakeProcess(
            self.token_file, [subprocess.TimeoutExpired("seed-command", 1800)]
        )
        result, _, stop = self.run_main(process)
        self.assertEqual(result, 1)
        stop.assert_called_once_with(process.pid, signal.SIGTERM)
        self.assertIsNotNone(process.poll())
        self.assertFalse(self.token_file.exists())

    def test_main_returns_command_failure(self):
        result, _, stop = self.run_main(FakeProcess(self.token_file, [13]))
        self.assertEqual(result, 13)
        stop.assert_not_called()
        self.assertFalse(self.token_file.exists())

    def test_main_stops_command_and_removes_token_on_cancellation(self):
        process = FakeProcess(self.token_file, [SystemExit(143)])
        with self.assertRaises(SystemExit) as raised:
            self.run_main(process)
        self.assertEqual(raised.exception.code, 143)
        self.assertIsNotNone(process.poll())
        self.assertFalse(self.token_file.exists())


if __name__ == "__main__":
    unittest.main()
