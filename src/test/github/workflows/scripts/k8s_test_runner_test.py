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
import signal
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[5] / ".github/workflows/scripts/k8s_test_runner.py"
FAKE = r"""#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
with open(os.environ["CALLS"], "a") as out:
    out.write(json.dumps(args) + "\n")
data = json.loads(Path(os.environ["DATA"]).read_text())
if "logs" in args:
    if data.get("fail_logs"):
        print("logs unavailable", file=sys.stderr)
        sys.exit(4)
    print(data.get("logs", "emulator process terminated"))
else:
    count_file = Path(os.environ["ATTEMPTS"])
    count = int(count_file.read_text()) if count_file.exists() else 0
    count_file.write_text(str(count + 1))
    response = data["responses"][min(count, len(data["responses"]) - 1)]
    if response == "error":
        print("API unavailable", file=sys.stderr)
        sys.exit(4)
    print(json.dumps({"kind": "List", "items": response}))
"""
LONG_CHILD = (
    "import os,time; from pathlib import Path; "
    "Path(os.environ['CHILD_PID_FILE']).write_text(str(os.getpid())); "
    "print('CHILD_STARTED',flush=True); time.sleep(30)"
)
SHORT_CHILD = "import time; time.sleep(0.3)"


class KubernetesTestRunnerTest(unittest.TestCase):
    def pod(self, uid, state, restarts):
        return {
            "metadata": {"name": "spanner-emulator", "uid": uid},
            "spec": {"containers": [{"env": [{"value": "SECRET_FROM_ENV"}]}]},
            "status": {
                "phase": "Running", "containerStatuses": [{
                    "name": "spanner-emulator", "restartCount": restarts,
                    "image": "emulator:1.5.52", "state": state,
                }],
            },
        }

    def invoke(self, data, command):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        fake = root / "kubectl"
        fake.write_text(FAKE)
        fake.chmod(0o755)
        config = root / "data.json"
        config.write_text(json.dumps(data))
        output, calls, pid_file = root / "output", root / "calls.jsonl", root / "child.pid"
        env = {
            **os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
            "DATA": str(config), "CALLS": str(calls), "ATTEMPTS": str(root / "attempts"),
            "CHILD_PID_FILE": str(pid_file), "GITHUB_STEP_SUMMARY": str(root / "summary"),
        }
        result = subprocess.run([
            sys.executable, str(SCRIPT), "--pod=spanner-emulator",
            "--output-dir", str(output), "--poll-interval-seconds", str(data.get("interval", 0.01)),
            "--", sys.executable, "-c", command,
        ], env=env, capture_output=True, text=True, timeout=5)
        commands = [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []
        if pid_file.exists():
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pid_file.read_text()), 0)
        return result, output, commands

    def test_preserves_success_while_pod_starts(self):
        pod = self.pod("original", {"running": {}}, 0)
        result, _, commands = self.invoke({"responses": [[], [], [pod]]}, SHORT_CHILD)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertGreaterEqual(sum("get" in command for command in commands), 3)

    def test_preserves_test_failure_exit_code(self):
        pod = self.pod("original", {"running": {}}, 0)
        result, _, _ = self.invoke({"responses": [[pod]]}, "import time; time.sleep(0.1); raise SystemExit(7)")
        self.assertEqual(result.returncode, 7, result.stderr)

    def test_stops_test_on_emulator_exit_and_captures_status_and_bounded_logs(self):
        pod = self.pod("original", {"terminated": {"exitCode": 255, "reason": "Error"}}, 0)
        result, output, commands = self.invoke({"responses": [[pod]], "logs": "é" * 20000}, LONG_CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("CHILD_STARTED", result.stdout)
        self.assertIn("exit=255", result.stderr)
        status = json.loads((output / "status.json").read_text())
        self.assertEqual(status["pods"][0]["status"]["containerStatuses"][0]["image"], "emulator:1.5.52")
        self.assertNotIn("SECRET_FROM_ENV", (output / "status.json").read_text())
        self.assertEqual((output / "spanner-emulator-spanner-emulator.log").stat().st_size, 16384)
        logs = [command for command in commands if "logs" in command]
        self.assertTrue(all("--tail=80" in command and "--limit-bytes=16384" in command for command in logs))

    def test_clean_emulator_exit_is_still_data_loss(self):
        pod = self.pod("original", {"terminated": {"exitCode": 0, "reason": "Completed"}}, 0)
        result, _, _ = self.invoke({"responses": [[pod]]}, LONG_CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("exit=0", result.stderr)

    def test_detects_restart_even_when_container_is_running(self):
        pod = self.pod("original", {"running": {}}, 1)
        pod["status"]["containerStatuses"][0]["lastState"] = {"terminated": {"exitCode": 255}}
        result, output, commands = self.invoke({"responses": [[pod]]}, LONG_CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("restarted", result.stderr)
        self.assertTrue((output / "spanner-emulator-spanner-emulator-previous.log").exists())
        self.assertTrue(any("--previous" in command for command in commands))

    def test_detects_replacement_by_uid(self):
        old = self.pod("original", {"running": {}}, 0)
        new = self.pod("replacement", {"running": {}}, 0)
        result, _, _ = self.invoke({"responses": [[old], [new]]}, LONG_CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("original -> replacement", result.stderr)

    def test_detects_disappearance_after_observation(self):
        pod = self.pod("original", {"running": {}}, 0)
        result, output, _ = self.invoke({"responses": [[pod], []]}, LONG_CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("disappeared", result.stderr)
        self.assertEqual(json.loads((output / "status.json").read_text())["pods"][0]["uid"], "original")

    def test_ignores_successful_init_container_completion(self):
        pod = self.pod("original", {"running": {}}, 0)
        pod["status"]["initContainerStatuses"] = [{"name": "setup", "state": {"terminated": {"exitCode": 0}}}]
        result, _, _ = self.invoke({"responses": [[pod]]}, SHORT_CHILD)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_detects_terminal_phase_before_container_status_is_available(self):
        pod = self.pod("original", {"running": {}}, 0)
        pod["status"] = {"phase": "Failed"}
        result, _, _ = self.invoke({"responses": [[pod]]}, LONG_CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("terminal phase Failed", result.stderr)

    def test_tolerates_a_transient_status_read_failure(self):
        pod = self.pod("original", {"running": {}}, 0)
        result, _, _ = self.invoke({"responses": ["error", [pod]]}, SHORT_CHILD)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_bounds_repeated_status_read_failures(self):
        result, _, commands = self.invoke({"responses": ["error"]}, LONG_CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("after 3 reads", result.stderr)
        self.assertEqual(sum("get" in command for command in commands), 3)

    def test_bounds_malformed_status_responses(self):
        result, _, commands = self.invoke({"responses": [[{"metadata": {}}]]}, LONG_CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("after 3 reads", result.stderr)
        self.assertEqual(sum("get" in command for command in commands), 3)

    def test_cancellation_stops_and_reaps_test_process(self):
        child = (
            "import os,signal,time; from pathlib import Path; "
            "Path(os.environ['CHILD_PID_FILE']).write_text(str(os.getpid())); "
            "os.kill(os.getppid(), signal.SIGTERM); time.sleep(30)"
        )
        pod = self.pod("original", {"running": {}}, 0)
        result, _, _ = self.invoke({"responses": [[pod]]}, child)
        self.assertEqual(result.returncode, 128 + signal.SIGTERM, result.stderr)

    def test_log_read_failure_does_not_hide_emulator_exit(self):
        pod = self.pod("original", {"terminated": {"exitCode": 255}}, 0)
        result, output, _ = self.invoke({"responses": [[pod]], "fail_logs": True}, LONG_CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("exit=255", result.stderr)
        self.assertEqual(len(json.loads((output / "errors.json").read_text())), 1)

    def test_rejects_non_positive_poll_interval(self):
        result, _, commands = self.invoke({"responses": [[]], "interval": 0}, SHORT_CHILD)
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertFalse(commands)


if __name__ == "__main__":
    unittest.main()
