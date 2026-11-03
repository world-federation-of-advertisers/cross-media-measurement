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
import select
import subprocess
import tempfile
import unittest


class DeployWithRetryTest(unittest.TestCase):

    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.directory = Path(self.temporary_directory.name)
        self.script = (
            Path(__file__).resolve().parents[6]
            / 'src/main/terraform/gcloud/modules/http-cloud-function/deploy_with_retry.sh'
        )
        command = self.directory / 'gcloud'
        command.write_text("""#!/usr/bin/env python3
import json, os, pathlib, sys
directory = pathlib.Path(os.environ['RETRY_TEST_DIRECTORY'])
calls_file = directory / 'calls.json'
calls = json.loads(calls_file.read_text()) if calls_file.exists() else []
deploy_count = sum(call[:2] == ['functions', 'deploy'] for call in calls)
calls.append(sys.argv[1:])
calls_file.write_text(json.dumps(calls))
if sys.argv[1:3] == ['builds', 'describe']:
    status = json.loads(os.environ['RETRY_TEST_BUILD_STATUSES'])[deploy_count - 1]
    if status is None:
        print('ERROR: Could not read Cloud Build status', file=sys.stderr)
        sys.exit(23)
    print(status)
    sys.exit(0)
outcome = json.loads(os.environ['RETRY_TEST_OUTCOMES'])[deploy_count]
if outcome == 'stream':
    print('Deployment started', flush=True)
    sys.stdin.readline()
if outcome in ('success', 'stream'):
    print('Deployment complete')
    sys.exit(0)
if outcome in ('expired', 'no-link'):
    print('ERROR: (gcloud.functions.deploy) OperationError: code=4, message=Build failed with status: EXPIRED and message: An unexpected error occurred.', file=sys.stderr)
else:
    print('ERROR: Build failed with status: FAILURE', file=sys.stderr)
if outcome != 'no-link':
    print('https://console.cloud.google.com/cloud-build/builds;region=us-central1/11111111-1111-4111-8111-111111111111?project=123456789012.', file=sys.stderr)
    print('https://console.cloud.google.com/cloud-build/builds;region=us-central1/11111111-1111-4111-8111-111111111111?project=123456789012.', file=sys.stderr)
if outcome == 'ambiguous':
    print('https://console.cloud.google.com/cloud-build/builds;region=europe-west1/22222222-2222-4222-8222-222222222222?project=other-project.', file=sys.stderr)
sys.exit(17)
""")
        command.chmod(0o755)
        sleeper = self.directory / 'sleep'
        sleeper.write_text(
            '#!/usr/bin/env bash\nprintf \'%s\n\' "$1" >>'
            ' "$RETRY_TEST_DIRECTORY/sleeps"\n'
        )
        sleeper.chmod(0o755)

    def environment(self, outcomes, build_statuses):
        return dict(
            os.environ,
            PATH=str(self.directory) + os.pathsep + os.environ['PATH'],
            RETRY_TEST_DIRECTORY=str(self.directory),
            RETRY_TEST_OUTCOMES=json.dumps(outcomes),
            RETRY_TEST_BUILD_STATUSES=json.dumps(build_statuses),
        )

    def arguments(self):
        return [
            'functions',
            'deploy',
            'test-function',
            '--set-env-vars=FOO=literal value',
        ]

    def run_deploy(self, outcomes, build_statuses):
        arguments = self.arguments()
        result = subprocess.run(
            ['bash', str(self.script), *arguments],
            env=self.environment(outcomes, build_statuses),
            capture_output=True,
            text=True,
            timeout=10,
        )
        calls = json.loads((self.directory / 'calls.json').read_text())
        for call in calls:
            if call[:2] == ['functions', 'deploy']:
                self.assertEqual(call, arguments)
            else:
                self.assertEqual(
                    call,
                    [
                        'builds',
                        'describe',
                        'projects/123456789012/locations/us-central1/builds/11111111-1111-4111-8111-111111111111',
                        '--billing-project=123456789012',
                        '--format=value(status)',
                        '--quiet',
                    ],
                )
        return result, [
            call for call in calls if call[:2] == ['functions', 'deploy']
        ]

    def test_recovers_expired_build(self):
        result, calls = self.run_deploy(['expired', 'success'], ['EXPIRED'])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 2)
        self.assertEqual(
            (self.directory / 'sleeps').read_text().splitlines(), ['30']
        )

    def test_stops_after_three_attempts(self):
        result, calls = self.run_deploy(
            ['expired', 'expired', 'expired'], ['EXPIRED', 'EXPIRED']
        )
        self.assertEqual(result.returncode, 17)
        self.assertEqual(len(calls), 3)
        self.assertEqual(
            (self.directory / 'sleeps').read_text().splitlines(), ['30', '60']
        )

    def test_does_not_retry_permanent_failure(self):
        result, calls = self.run_deploy(['failure'], ['FAILURE'])
        self.assertEqual(result.returncode, 17)
        self.assertEqual(len(calls), 1)
        self.assertFalse((self.directory / 'sleeps').exists())

    def test_success_does_not_retry(self):
        result, calls = self.run_deploy(['success'], [])
        self.assertEqual(result.returncode, 0)
        self.assertEqual(len(calls), 1)
        self.assertFalse((self.directory / 'sleeps').exists())

    def test_recovers_expired_build_without_matching_error_message(self):
        result, calls = self.run_deploy(['failure', 'success'], ['EXPIRED'])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 2)

    def test_does_not_trust_expired_error_message(self):
        result, calls = self.run_deploy(['expired', 'success'], ['FAILURE'])
        self.assertEqual(result.returncode, 17)
        self.assertEqual(len(calls), 1)
        self.assertFalse((self.directory / 'sleeps').exists())

    def test_status_lookup_failure_preserves_deploy_error(self):
        result, calls = self.run_deploy(['expired', 'success'], [None])
        self.assertEqual(result.returncode, 17)
        self.assertEqual(len(calls), 1)
        self.assertIn('Could not read Cloud Build status', result.stderr)
        self.assertFalse((self.directory / 'sleeps').exists())

    def test_does_not_resubmit_build_that_is_still_running(self):
        result, calls = self.run_deploy(['expired', 'success'], ['WORKING'])
        self.assertEqual(result.returncode, 17)
        self.assertEqual(len(calls), 1)
        self.assertFalse((self.directory / 'sleeps').exists())

    def test_missing_build_link_does_not_retry(self):
        result, calls = self.run_deploy(['no-link', 'success'], ['EXPIRED'])
        self.assertEqual(result.returncode, 17)
        self.assertEqual(len(calls), 1)
        self.assertFalse((self.directory / 'sleeps').exists())

    def test_ambiguous_build_links_does_not_retry(self):
        result, calls = self.run_deploy(['ambiguous', 'success'], ['EXPIRED'])
        self.assertEqual(result.returncode, 17)
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            len(json.loads((self.directory / 'calls.json').read_text())), 1
        )
        self.assertFalse((self.directory / 'sleeps').exists())

    def test_does_not_reuse_build_link_from_previous_attempt(self):
        result, calls = self.run_deploy(
            ['expired', 'no-link', 'success'], ['EXPIRED', 'EXPIRED']
        )
        self.assertEqual(result.returncode, 17)
        self.assertEqual(len(calls), 2)
        self.assertEqual(
            (self.directory / 'sleeps').read_text().splitlines(), ['30']
        )

    def test_streams_output_before_deployment_finishes(self):
        process = subprocess.Popen(
            ['bash', str(self.script), *self.arguments()],
            env=self.environment(['stream'], []),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            readable, _, _ = select.select([process.stdout], [], [], 5)
            self.assertTrue(
                readable, 'No output while deployment was waiting for input'
            )
            self.assertEqual(process.stdout.readline(), 'Deployment started\n')
            self.assertIsNone(process.poll())
        finally:
            output, errors = process.communicate(input='continue\n', timeout=5)
        self.assertEqual(process.returncode, 0, errors)
        self.assertIn('Deployment complete', output)


if __name__ == '__main__':
    unittest.main()
