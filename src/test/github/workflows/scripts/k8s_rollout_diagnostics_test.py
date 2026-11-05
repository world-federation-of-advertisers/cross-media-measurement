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

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[5] / ".github/workflows/scripts/k8s_rollout_diagnostics.py"
FAKE = """#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
with open(os.environ["CALLS"], "a") as out:
    out.write(json.dumps(args) + "\\n")
data = json.loads(Path(os.environ["DATA"]).read_text())
if data.get("fail") in args:
    print("read forbidden", file=sys.stderr)
    sys.exit(4)
if "logs" in args:
    print("schema migration rejected")
else:
    resource = args[args.index("get") + 1]
    print(json.dumps({"items": data[resource]}))
"""


class RolloutDiagnosticsTest(unittest.TestCase):
    def fixture(self):
        return {
            "deployments": [{
                "metadata": {"name": "edpa", "uid": "deployment-1"},
                "spec": {
                    "selector": {"matchLabels": {"app": "edpa-app"}},
                    "template": {"spec": {"containers": [{
                        "env": [{"name": "PASSWORD", "value": "SECRET_FROM_ENV"}]
                    }]}},
                },
                "status": {"conditions": [{"reason": "ProgressDeadlineExceeded", "status": "False"}]},
            }],
            "replicasets": [{"metadata": {"name": "edpa-rs", "uid": "rs-1"}, "status": {}}],
            "pods": [{
                "metadata": {"name": "edpa-pod", "uid": "pod-1"},
                "status": {
                    "phase": "Pending",
                    "initContainerStatuses": [{
                        "name": "update-schema", "restartCount": 2,
                        "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                        "lastState": {"terminated": {"exitCode": 1, "reason": "Error"}},
                    }],
                    "containerStatuses": [{
                        "name": "internal-api", "restartCount": 0,
                        "state": {"waiting": {"reason": "PodInitializing"}},
                    }],
                },
            }],
            "events": [
                {"involvedObject": {"uid": "pod-1"}, "reason": "BackOff", "message": "schema retry"},
                {"involvedObject": {"uid": "rs-1"}, "reason": "FailedCreate", "message": "quota"},
                {"involvedObject": {"uid": "unrelated"}, "message": "UNRELATED_EVENT"},
            ],
        }

    def invoke(self, data):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        fake = root / "kubectl"
        fake.write_text(FAKE)
        fake.chmod(0o755)
        config, calls, summary = root / "data.json", root / "calls.jsonl", root / "summary.md"
        config.write_text(json.dumps(data))
        output = root / "output"
        env = {
            **os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
            "DATA": str(config), "CALLS": str(calls), "GITHUB_STEP_SUMMARY": str(summary),
        }
        result = subprocess.run([
            sys.executable, str(SCRIPT), "--selector=app.kubernetes.io/component=edp-aggregator",
            "--output-dir", str(output),
        ], env=env, capture_output=True, text=True)
        commands = [json.loads(line) for line in calls.read_text().splitlines()]
        return result, output, commands, summary.read_text()

    def test_main_collects_failed_init_and_previous_logs_without_environment_contents(self):
        result, output, commands, summary = self.invoke(self.fixture())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("ProgressDeadlineExceeded", summary)
        self.assertIn("previous exit=1", summary)
        self.assertTrue((output / "edpa-pod-update-schema.log").exists())
        self.assertTrue((output / "edpa-pod-update-schema-previous.log").exists())
        all_output = "".join(path.read_text() for path in output.iterdir())
        self.assertNotIn("SECRET_FROM_ENV", all_output)
        self.assertNotIn("UNRELATED_EVENT", all_output)
        logs = [command for command in commands if "logs" in command]
        self.assertEqual(len(logs), 2)
        self.assertTrue(all("--tail=80" in command and "--limit-bytes=16384" in command for command in logs))
        self.assertTrue(all("internal-api" not in command for command in logs))
        pod_reads = [command for command in commands if "pods" in command]
        self.assertTrue(all("app=edpa-app" in command for command in pod_reads))

    def test_main_preserves_status_when_log_reads_fail(self):
        data = self.fixture()
        data["fail"] = "logs"
        result, output, _, summary = self.invoke(data)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Collection incomplete", summary)
        self.assertTrue(json.loads((output / "errors.json").read_text()))
        self.assertEqual(len(json.loads((output / "status.json").read_text())["pods"]), 1)

    def test_main_collects_replica_set_events_when_no_pod_exists(self):
        data = self.fixture()
        data["pods"] = []
        result, output, commands, _ = self.invoke(data)
        self.assertEqual(result.returncode, 0, result.stderr)
        events = json.loads((output / "status.json").read_text())["events"]
        self.assertEqual([event["reason"] for event in events], ["FailedCreate"])
        self.assertFalse(any("logs" in command for command in commands))

    def test_main_skips_completed_init_container(self):
        data = self.fixture()
        container = data["pods"][0]["status"]["initContainerStatuses"][0]
        container["state"] = {"terminated": {"exitCode": 0, "reason": "Completed"}}
        container["restartCount"] = 0
        result, _, commands, _ = self.invoke(data)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any("logs" in command for command in commands))

    def test_main_refuses_empty_deployment_selector(self):
        data = self.fixture()
        data["deployments"][0]["spec"]["selector"] = {}
        result, _, commands, summary = self.invoke(data)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Collection incomplete", summary)
        self.assertFalse(any("pods" in command for command in commands))

    def test_main_skips_init_container_that_has_not_started(self):
        data = self.fixture()
        data["pods"][0]["status"]["initContainerStatuses"].append({
            "name": "instrumentation", "restartCount": 0,
            "state": {"waiting": {"reason": "PodInitializing"}},
        })
        result, _, commands, _ = self.invoke(data)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any("logs" in command and "instrumentation" in command for command in commands))

    def test_main_records_failed_deployment_read(self):
        data = self.fixture()
        data["fail"] = "deployments"
        result, output, commands, summary = self.invoke(data)
        self.assertEqual(result.returncode, 1)
        self.assertIn("No deployments", summary)
        self.assertTrue(json.loads((output / "errors.json").read_text()))
        self.assertEqual(len(commands), 1)


if __name__ == "__main__":
    unittest.main()
