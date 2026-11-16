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
import re
import subprocess
import tempfile
import unittest


class DeadLetterPolicyTest(unittest.TestCase):

    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.directory = Path(self.temporary_directory.name)
        module = (
            Path(__file__).resolve().parents[6]
            / 'src/main/terraform/gcloud/modules/gcs-bucket-cloud-function/main.tf'
        )
        attachment = module.read_text().split(
            'resource "terraform_data" "attach_dead_letter_policy" {', 1
        )[1]
        self.command = re.search(
            r'command = <<-EOT\n(.*?)\n    EOT', attachment, re.DOTALL
        ).group(1)
        gcloud = self.directory / 'gcloud'
        gcloud.write_text("""#!/usr/bin/env python3
import json, os, pathlib, sys
calls_file = pathlib.Path(os.environ['DLQ_TEST_DIRECTORY']) / 'calls.json'
calls = json.loads(calls_file.read_text()) if calls_file.exists() else []
calls.append(sys.argv[1:])
calls_file.write_text(json.dumps(calls))
if sys.argv[1:3] == ['functions', 'describe']:
    print(os.environ['DLQ_TEST_TRIGGER'])
elif sys.argv[1:4] == ['eventarc', 'triggers', 'describe']:
    print(os.environ['DLQ_TEST_SUBSCRIPTION'])
elif sys.argv[1:4] == ['pubsub', 'subscriptions', 'list']:
    # The archive subscription appears first and shares the function prefix.
    print('projects/test/subscriptions/eventarc-us-data-watcher-delete-archived-first')
elif sys.argv[1:4] != ['pubsub', 'subscriptions', 'update']:
    sys.exit(2)
""")
        gcloud.chmod(0o755)

    def run_attachment(self, trigger='projects/test/locations/us-central1/triggers/primary',
                       subscription='projects/test/subscriptions/primary'):
        result = subprocess.run(
            ['bash', '-c', self.command],
            env=dict(
                os.environ,
                PATH=str(self.directory) + os.pathsep + os.environ['PATH'],
                DLQ_TEST_DIRECTORY=str(self.directory),
                DLQ_TEST_TRIGGER=trigger,
                DLQ_TEST_SUBSCRIPTION=subscription,
                FUNCTION_NAME='data-watcher-delete',
                CLOUD_REGION='us-central1',
                DLQ_TOPIC='projects/test/topics/dead-letter',
                MAX_DELIVERY_ATTEMPTS='5',
                MESSAGE_RETENTION='86400s',
            ),
            capture_output=True,
            text=True,
            timeout=10,
        )
        calls = json.loads((self.directory / 'calls.json').read_text())
        return result, calls

    def test_uses_primary_trigger_when_archive_subscription_exists(self):
        result, calls = self.run_attachment()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, [
            ['functions', 'describe', 'data-watcher-delete', '--gen2',
             '--region=us-central1', '--format=value(eventTrigger.trigger)'],
            ['eventarc', 'triggers', 'describe',
             'projects/test/locations/us-central1/triggers/primary',
             '--location=us-central1',
             '--format=value(transport.pubsub.subscription)'],
            ['pubsub', 'subscriptions', 'update',
             'projects/test/subscriptions/primary',
             '--dead-letter-topic=projects/test/topics/dead-letter',
             '--max-delivery-attempts=5',
             '--message-retention-duration=86400s'],
        ])

    def test_missing_primary_trigger_fails_deployment(self):
        result, calls = self.run_attachment(trigger='')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Missing primary trigger', result.stderr)
        self.assertEqual(len(calls), 1)

    def test_missing_primary_subscription_fails_deployment(self):
        result, calls = self.run_attachment(subscription='')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Missing primary trigger subscription', result.stderr)
        self.assertEqual(len(calls), 2)


if __name__ == '__main__':
    unittest.main()
