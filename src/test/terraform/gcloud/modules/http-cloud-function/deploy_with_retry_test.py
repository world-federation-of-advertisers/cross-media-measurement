# Copyright 2026 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class DeployWithRetryTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.directory = Path(self.temporary_directory.name)
        self.script = Path(__file__).resolve().parents[6] / "src/main/terraform/gcloud/modules/http-cloud-function/deploy_with_retry.sh"
        command = self.directory / "gcloud"
        command.write_text("""#!/usr/bin/env python3
import json, os, pathlib, sys
directory = pathlib.Path(os.environ['RETRY_TEST_DIRECTORY'])
calls_file = directory / 'calls.json'
calls = json.loads(calls_file.read_text()) if calls_file.exists() else []
outcome = json.loads(os.environ['RETRY_TEST_OUTCOMES'])[len(calls)]
calls.append(sys.argv[1:])
calls_file.write_text(json.dumps(calls))
if outcome == 'success':
    print('Deployment complete')
    sys.exit(0)
if outcome == 'expired':
    print('ERROR: (gcloud.functions.deploy) OperationError: code=4, message=Build failed with status: EXPIRED and message: An unexpected error occurred.', file=sys.stderr)
else:
    print('ERROR: Build failed with status: FAILURE', file=sys.stderr)
sys.exit(17)
""")
        command.chmod(0o755)
        sleeper = self.directory / "sleep"
        sleeper.write_text("#!/usr/bin/env bash\nprintf '%s\n' \"$1\" >> \"$RETRY_TEST_DIRECTORY/sleeps\"\n")
        sleeper.chmod(0o755)

    def run_deploy(self, outcomes):
        environment = dict(os.environ, PATH=str(self.directory) + os.pathsep + os.environ['PATH'],
                           RETRY_TEST_DIRECTORY=str(self.directory), RETRY_TEST_OUTCOMES=json.dumps(outcomes))
        arguments = ['functions', 'deploy', 'test-function', '--set-env-vars=FOO=literal value']
        result = subprocess.run(['bash', str(self.script), *arguments], env=environment, capture_output=True, text=True)
        calls = json.loads((self.directory / 'calls.json').read_text())
        for call in calls:
            self.assertEqual(call, arguments)
        return result, calls

    def test_recovers_expired_build(self):
        result, calls = self.run_deploy(['expired', 'success'])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 2)
        self.assertEqual((self.directory / 'sleeps').read_text().splitlines(), ['30'])

    def test_stops_after_three_attempts(self):
        result, calls = self.run_deploy(['expired', 'expired', 'expired'])
        self.assertEqual(result.returncode, 17)
        self.assertEqual(len(calls), 3)
        self.assertEqual((self.directory / 'sleeps').read_text().splitlines(), ['30', '60'])

    def test_does_not_retry_permanent_failure(self):
        result, calls = self.run_deploy(['failure'])
        self.assertEqual(result.returncode, 17)
        self.assertEqual(len(calls), 1)
        self.assertFalse((self.directory / 'sleeps').exists())

    def test_success_does_not_retry(self):
        result, calls = self.run_deploy(['success'])
        self.assertEqual(result.returncode, 0)
        self.assertEqual(len(calls), 1)
        self.assertFalse((self.directory / 'sleeps').exists())


if __name__ == '__main__':
    unittest.main()
