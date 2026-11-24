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

import ctypes
import json
import os
from pathlib import Path
import select
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
if "logs" in args:
    print("Logs belong to rollout diagnostics", file=sys.stderr)
    sys.exit(4)
data = json.loads(Path(os.environ["DATA"]).read_text())
count_file = Path(os.environ["ATTEMPTS"])
count = int(count_file.read_text()) if count_file.exists() else 0
if count == 0:
    with open(os.environ["READY"], "rb") as ready:
        ready.read(1)
count_file.write_text(str(count + 1))
if count + 1 == data.get("finish_after"):
    with open(os.environ["RELEASE"], "wb") as release:
        release.write(b"x")
response = data["responses"][min(count, len(data["responses"]) - 1)]
if response == "error":
    print("API unavailable", file=sys.stderr)
    sys.exit(4)
print(json.dumps({"kind": "List", "items": response}))
"""
CHILD = r"""
import os, signal
from pathlib import Path
Path(os.environ['CHILD_PID_FILE']).write_text(str(os.getpid()))
print('CHILD_STARTED', flush=True)
with open(os.environ['READY'], 'wb') as ready:
    ready.write(b'x')
if os.environ['CANCEL'] == 'true':
    os.kill(os.getppid(), signal.SIGTERM)
with open(os.environ['RELEASE'], 'rb') as release:
    release.read(1)
raise SystemExit(int(os.environ['CHILD_EXIT']))
"""
# Exercise the real CLI with a fake cleanup clock; subprocess timeouts remain guards.
CLOCK_RUNNER = r"""
import runpy, sys
from unittest.mock import patch
clock = 0
def monotonic():
    return clock
def advance(seconds):
    global clock
    clock += seconds
sys.argv = sys.argv[1:]
with patch('time.monotonic', monotonic), patch('time.sleep', advance):
    runpy.run_path(sys.argv[0], run_name='__main__')
"""
GRANDCHILD = r"""
import os, signal
from pathlib import Path
signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path(os.environ['CHILD_PID_FILE']).write_text(str(os.getpid()))
with open(os.environ['READY'], 'wb') as ready:
    ready.write(b'x')
with open(os.environ['RELEASE'], 'rb') as release:
    release.read(1)
"""


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
        if data.get("output_is_file"):
            output.write_text("occupied")
        ready, release = root / "ready", root / "release"
        os.mkfifo(ready)
        os.mkfifo(release)
        env = {
            **os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
            "DATA": str(config), "CALLS": str(calls), "ATTEMPTS": str(root / "attempts"),
            "CHILD_PID_FILE": str(pid_file), "GITHUB_STEP_SUMMARY": str(root / "summary"),
            "READY": str(ready), "RELEASE": str(release),
            "CANCEL": str(data.get("cancel", False)).lower(),
            "CHILD_EXIT": str(data.get("child_exit", 0)),
        }
        result = subprocess.run([
            sys.executable, "-c", CLOCK_RUNNER, str(SCRIPT), "--pod=spanner-emulator",
            "--output-dir", str(output), "--poll-interval-seconds", str(data.get("interval", 0.01)),
            "--", sys.executable, "-c", command,
        ], env=env, capture_output=True, text=True, timeout=5)
        commands = [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []
        if pid_file.exists():
            pid = int(pid_file.read_text())
            try:
                descriptor = os.pidfd_open(pid)
            except ProcessLookupError:
                pass
            else:
                try:
                    exited = bool(select.select([descriptor], [], [], 5)[0])
                    if not exited:
                        os.kill(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
                    self.assertTrue(exited, "Test descendant survived the runner")
                finally:
                    os.close(descriptor)
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)
        return result, output, commands

    def test_preserves_success_while_pod_starts(self):
        pod = self.pod("original", {"running": {}}, 0)
        result, _, commands = self.invoke({"responses": [[], [], [pod]], "finish_after": 3}, CHILD)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertGreaterEqual(sum("get" in command for command in commands), 3)

    def test_preserves_test_failure_exit_code(self):
        pod = self.pod("original", {"running": {}}, 0)
        result, _, _ = self.invoke({"responses": [[pod]], "finish_after": 1, "child_exit": 7}, CHILD)
        self.assertEqual(result.returncode, 7, result.stderr)

    def test_stops_test_on_emulator_exit_and_retains_status_without_fetching_logs(self):
        pod = self.pod("original", {"terminated": {"exitCode": 255, "reason": "Error"}}, 0)
        result, output, commands = self.invoke({"responses": [[pod]]}, CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("CHILD_STARTED", result.stdout)
        self.assertIn("exit=255", result.stderr)
        status = json.loads((output / "status.json").read_text())
        self.assertEqual(status["pods"][0]["status"]["containerStatuses"][0]["image"], "emulator:1.5.52")
        self.assertNotIn("SECRET_FROM_ENV", (output / "status.json").read_text())
        self.assertFalse(any("logs" in command for command in commands))

    def test_clean_emulator_exit_is_still_data_loss(self):
        pod = self.pod("original", {"terminated": {"exitCode": 0, "reason": "Completed"}}, 0)
        result, _, _ = self.invoke({"responses": [[pod]]}, CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("exit=0", result.stderr)

    def test_detects_restart_even_when_container_is_running(self):
        pod = self.pod("original", {"running": {}}, 1)
        pod["status"]["containerStatuses"][0]["lastState"] = {"terminated": {"exitCode": 255}}
        result, output, _ = self.invoke({"responses": [[pod]]}, CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("restarted", result.stderr)
        self.assertEqual(json.loads((output / "status.json").read_text())["pods"][0]["status"], pod["status"])

    def test_detects_replacement_and_preserves_both_pod_snapshots(self):
        old = self.pod("original", {"running": {"startedAt": "2026-01-01T00:00:00Z"}}, 0)
        new = self.pod("replacement", {"running": {"startedAt": "2026-01-01T00:00:01Z"}}, 0)
        result, output, _ = self.invoke({"responses": [[old], [new]]}, CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("original -> replacement", result.stderr)
        snapshots = {pod["uid"]: pod for pod in json.loads((output / "status.json").read_text())["pods"]}
        self.assertEqual(set(snapshots), {"original", "replacement"})
        self.assertEqual(snapshots["original"]["status"], old["status"])
        self.assertEqual(snapshots["replacement"]["status"], new["status"])
        self.assertEqual(snapshots["original"]["role"], "original")
        self.assertEqual(snapshots["replacement"]["role"], "replacement")

    def test_detects_disappearance_after_observation(self):
        pod = self.pod("original", {"running": {}}, 0)
        result, output, _ = self.invoke({"responses": [[pod], []]}, CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("disappeared", result.stderr)
        self.assertEqual(json.loads((output / "status.json").read_text())["pods"][0]["uid"], "original")

    def test_ignores_successful_init_container_completion(self):
        pod = self.pod("original", {"running": {}}, 0)
        pod["status"]["initContainerStatuses"] = [{"name": "setup", "state": {"terminated": {"exitCode": 0}}}]
        result, _, _ = self.invoke({"responses": [[pod]], "finish_after": 1}, CHILD)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_detects_terminal_phase_before_container_status_is_available(self):
        pod = self.pod("original", {"running": {}}, 0)
        pod["status"] = {"phase": "Failed"}
        result, _, _ = self.invoke({"responses": [[pod]]}, CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("terminal phase Failed", result.stderr)

    def test_tolerates_a_transient_status_read_failure(self):
        pod = self.pod("original", {"running": {}}, 0)
        result, _, _ = self.invoke({"responses": ["error", [pod]], "finish_after": 2}, CHILD)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_warns_and_continues_after_repeated_status_read_failures(self):
        result, _, _ = self.invoke({"responses": ["error"], "finish_after": 4}, CHILD)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("::warning::Cannot monitor database pods after 3 reads", result.stderr)
        self.assertEqual(result.stderr.count("::warning::"), 1)

    def test_warns_and_continues_after_malformed_status_responses(self):
        result, _, _ = self.invoke({"responses": [[{"metadata": {}}]], "finish_after": 4}, CHILD)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("::warning::Cannot monitor database pods after 3 reads", result.stderr)

    def test_cancellation_stops_and_reaps_test_process(self):
        pod = self.pod("original", {"running": {}}, 0)
        result, _, _ = self.invoke({"responses": [[pod]], "cancel": True}, CHILD)
        self.assertEqual(result.returncode, 128 + signal.SIGTERM, result.stderr)

    @unittest.skipUnless(sys.platform == "linux", "Uses Linux orphan adoption and pidfds")
    def test_stops_grandchild_even_when_direct_child_exits_on_sigterm(self):
        libc = ctypes.CDLL(None, use_errno=True)
        previous = ctypes.c_int()
        self.assertEqual(libc.prctl(37, ctypes.byref(previous), 0, 0, 0), 0)
        self.assertEqual(libc.prctl(36, 1, 0, 0, 0), 0)
        self.addCleanup(lambda: libc.prctl(36, previous.value, 0, 0, 0))
        command = (
            "import subprocess, sys; "
            f"subprocess.Popen([sys.executable, '-c', {GRANDCHILD!r}], "
            "stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()"
        )
        pod = self.pod("original", {"terminated": {"exitCode": 255}}, 0)
        result, _, _ = self.invoke({"responses": [[pod]]}, command)
        self.assertEqual(result.returncode, 1, result.stderr)

    def test_artifact_write_failure_does_not_hide_emulator_exit(self):
        pod = self.pod("original", {"terminated": {"exitCode": 255}}, 0)
        result, _, _ = self.invoke({"responses": [[pod]], "output_is_file": True}, CHILD)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("exit=255", result.stderr)
        self.assertIn("Unable to save dependency status", result.stderr)
        self.assertNotIn("Unable to start test command", result.stderr)

    def test_rejects_non_positive_poll_interval(self):
        result, _, commands = self.invoke({"responses": [[]], "interval": 0}, CHILD)
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertFalse(commands)


if __name__ == "__main__":
    unittest.main()
