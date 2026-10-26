# Copyright 2026 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Execute Terraform's rendered caller with a fake transport; never contact cloud APIs."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

MODULE = Path(__file__).resolve().parent.parent


class CallerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        variables = {
            "project": "example-project",
            "region": "us-central1",
            "name": "egs-job",
            "function_name": "event-group-sync",
            "terraform_service_account": "terraform@example-project.iam.gserviceaccount.com",
            "target": {
                "schedule": "0 2 * * *",
                "data_provider": "dataProviders/example",
                "input_blob_uri": "gs://example-bucket/event_groups.json",
                "caller_image": "example/caller@sha256:" + "a" * 64,
            },
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tfvars.json") as params:
            json.dump(variables, params)
            params.flush()
            output = subprocess.check_output(
                [os.environ.get("TERRAFORM_BINARY", "terraform"), "console", "-var-file=" + params.name],
                input=b"jsonencode(local.caller_script)\n",
                cwd=MODULE,
            )
        cls.script = json.loads(json.loads(output))
        subprocess.run(["/bin/sh", "-n"], input=cls.script.encode(), check=True)

    def invoke(self, status="200", transport_exit="0", metadata_exit="0"):
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            fake_curl = temp / "curl"
            fake_curl.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, sys\n"
                "with open(os.environ['CALL_LOG'], 'a') as log: log.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                "metadata = sys.argv[-1].startswith('http://metadata.google.internal/')\n"
                "print('test-token' if metadata else os.environ['HTTP_STATUS'], end='')\n"
                "sys.exit(int(os.environ['METADATA_EXIT' if metadata else 'TRANSPORT_EXIT']))\n"
            )
            fake_curl.chmod(0o755)
            env = dict(os.environ, PATH=str(temp) + os.pathsep + os.environ['PATH'],
                       CALL_LOG=str(temp / 'calls.jsonl'), HTTP_STATUS=status,
                       TRANSPORT_EXIT=transport_exit, METADATA_EXIT=metadata_exit,
                       EGS_URL='https://example.invalid/egs',
                       INPUT_BLOB_URI='gs://example-bucket/$(touch should-not-exist)',
                       REQUEST_BODY='{"dataProvider":"dataProviders/example"}')
            result = subprocess.run(['/bin/sh', '-c', self.script], cwd=temp, env=env,
                                    capture_output=True, text=True)
            calls = [json.loads(line) for line in (temp / 'calls.jsonl').read_text().splitlines()]
            self.assertFalse((temp / 'should-not-exist').exists())
            return result, calls

    def test_success_sends_authenticated_existing_contract(self):
        result, calls = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 2)
        self.assertIn('audience=https://example.invalid/egs', calls[0])
        self.assertIn('Authorization: Bearer test-token', calls[1])
        self.assertIn('X-DataWatcher-Path: gs://example-bucket/$(touch should-not-exist)', calls[1])
        self.assertIn('{"dataProvider":"dataProviders/example"}', calls[1])
        self.assertEqual(calls[1][-1], 'https://example.invalid/egs')

    def test_busy_function_fails_without_another_request(self):
        result, calls = self.invoke(status='429', transport_exit='22')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(calls), 2)

    def test_acceptance_is_not_sync_completion(self):
        result, calls = self.invoke(status='202')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(calls), 2)

    def test_connection_timeout_fails_without_another_request(self):
        result, calls = self.invoke(transport_exit='28')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(calls), 2)

    def test_identity_failure_never_calls_function(self):
        result, calls = self.invoke(metadata_exit='22')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(calls), 1)


if __name__ == '__main__':
    unittest.main()
