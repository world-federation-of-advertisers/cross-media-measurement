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
import sys
import tempfile
import unittest


TOOL = Path(__file__).with_name("bazel-with-results")


def finished_event(code: int) -> str:
    exit_code = {"name": "SUCCESS"} if code == 0 else {"name": "FAILURE", "code": code}
    return (
        json.dumps({"id": {"buildFinished": {}}, "finished": {"exitCode": exit_code}})
        + "\n"
    )


class BazelWithResultsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.calls = self.root / "calls.jsonl"
        self.fake_bazel = self.root / "bazel"
        self.fake_bazel.write_text(
            "#!/usr/bin/env python3\n"
            + r"""
import json
import os
from pathlib import Path
import sys
import time

with Path(os.environ["FAKE_BAZEL_CALLS"]).open("a") as calls:
    calls.write(json.dumps(sys.argv[1:]) + "\n")
event_path = next((arg.split("=", 1)[1] for arg in sys.argv[1:] if arg.startswith("--build_event_json_file=")), None)
if event_path is not None and "FAKE_BAZEL_EVENTS" in os.environ:
    Path(event_path).write_text(os.environ["FAKE_BAZEL_EVENTS"])
print("Synthetic build output", flush=True)
print("Synthetic diagnostic output", file=sys.stderr, flush=True)
time.sleep(0.02)
sys.exit(int(os.environ["FAKE_BAZEL_EXIT"]))
"""
        )
        self.fake_bazel.chmod(0o755)

    def invoke(
        self, native_code: int, events: str | None, command: str, extra_args: list[str]
    ):
        self.calls.write_text("")
        env = dict(os.environ)
        env.update(
            {
                "FAKE_BAZEL_CALLS": str(self.calls),
                "FAKE_BAZEL_EXIT": str(native_code),
                "GITHUB_ACTIONS": "true",
            }
        )
        if events is not None:
            env["FAKE_BAZEL_EVENTS"] = events
        else:
            env.pop("FAKE_BAZEL_EVENTS", None)
        result = subprocess.run(
            [sys.executable, str(TOOL), str(self.fake_bazel), command, *extra_args],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()]
        if command in ("build", "test"):
            self.assertEqual(len(calls), 1)
            event_path = next(
                arg.split("=", 1)[1]
                for arg in calls[0]
                if arg.startswith("--build_event_json_file=")
            )
            self.assertFalse(Path(event_path).exists())
        return result, calls

    def test_upload_failure_preserves_passing_tests(self):
        result, _ = self.invoke(38, finished_event(0), "test", ["//:passing"])
        self.assertEqual(result.returncode, 0)
        self.assertIn("::warning::", result.stderr)
        self.assertIn("exit code 0", result.stderr)

    def test_upload_failure_preserves_failed_tests(self):
        result, _ = self.invoke(38, finished_event(3), "test", ["//:failing"])
        self.assertEqual(result.returncode, 3)
        self.assertIn("exit code 3", result.stderr)

    def test_upload_failure_preserves_failed_build(self):
        result, _ = self.invoke(38, finished_event(1), "build", ["//:broken"])
        self.assertEqual(result.returncode, 1)

    def test_other_process_failures_are_preserved(self):
        for code in (1, 2, 3, 7, 8, 36, 37):
            with self.subTest(code=code):
                result, _ = self.invoke(code, finished_event(0), "test", [])
                self.assertEqual(result.returncode, code)
                self.assertNotIn("::warning::", result.stderr)

    def test_successful_upload_is_preserved(self):
        result, _ = self.invoke(0, finished_event(0), "build", ["//:passing"])
        self.assertEqual(result.returncode, 0)
        self.assertNotIn("::warning::", result.stderr)

    def test_missing_or_incomplete_verdict_remains_failed(self):
        for events in (None, "", '{"id":', json.dumps({"id": {"started": {}}}) + "\n"):
            with self.subTest(events=events):
                result, _ = self.invoke(38, events, "test", [])
                self.assertEqual(result.returncode, 38)
                self.assertIn("::warning::", result.stderr)

    def test_multiple_completion_events_remain_failed(self):
        result, _ = self.invoke(38, finished_event(0) + finished_event(3), "test", [])
        self.assertEqual(result.returncode, 38)

    def test_invalid_exit_codes_remain_failed(self):
        for code in (-1, 256, "0", True):
            with self.subTest(code=code):
                events = (
                    json.dumps(
                        {
                            "id": {"buildFinished": {}},
                            "finished": {"exitCode": {"code": code}},
                        }
                    )
                    + "\n"
                )
                result, _ = self.invoke(38, events, "test", [])
                self.assertEqual(result.returncode, 38)

    def test_missing_failed_exit_code_is_not_success(self):
        events = (
            json.dumps(
                {
                    "id": {"buildFinished": {}},
                    "finished": {"exitCode": {"name": "TESTS_FAILED"}},
                }
            )
            + "\n"
        )
        result, _ = self.invoke(38, events, "test", [])
        self.assertEqual(result.returncode, 38)

    def test_missing_exit_code_remains_failed(self):
        events = json.dumps({"id": {"buildFinished": {}}, "finished": {}}) + "\n"
        result, _ = self.invoke(38, events, "test", [])
        self.assertEqual(result.returncode, 38)

    def test_subsequent_events_do_not_replace_verdict(self):
        events = (
            finished_event(3)
            + json.dumps({"id": {"buildToolLogs": {}}, "buildToolLogs": {}})
            + "\n"
        )
        result, _ = self.invoke(38, events, "test", [])
        self.assertEqual(result.returncode, 3)

    def test_waits_for_upload_and_preserves_target_arguments(self):
        arguments = [
            "--test_output=streamed",
            "--bes_upload_mode=nowait_for_upload_complete",
            "--",
            "//:passing",
        ]
        result, calls = self.invoke(0, finished_event(0), "test", arguments)
        self.assertEqual(result.returncode, 0)
        self.assertIn("Synthetic build output", result.stdout)
        self.assertIn("Synthetic diagnostic output", result.stderr)
        forwarded = calls[0]
        self.assertEqual(forwarded[0], "test")
        modes = [arg for arg in forwarded if arg.startswith("--bes_upload_mode=")]
        self.assertEqual(modes[-1], "--bes_upload_mode=wait_for_upload_complete")
        self.assertLess(forwarded.index(modes[-1]), forwarded.index("--"))
        self.assertEqual(forwarded[-1], "//:passing")

    def test_run_is_rejected_before_executing(self):
        result, calls = self.invoke(0, finished_event(0), "run", ["//:tool"])
        self.assertEqual(result.returncode, 2)
        self.assertEqual(calls, [])

    def test_missing_bazel_program_is_reported(self):
        self.fake_bazel.unlink()
        result = subprocess.run(
            [sys.executable, str(TOOL), str(self.fake_bazel), "test", "//:passing"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 127)
        self.assertIn("Unable to start Bazel", result.stderr)


if __name__ == "__main__":
    unittest.main()
