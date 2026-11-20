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

"""Regression checks for the pinned multirun runner's child launch cleanup."""

import contextlib
import io
import json
import os
from pathlib import Path
import runpy
import signal
import tempfile
import unittest
from unittest import mock

from python.runfiles import runfiles

RUNNER = Path(runfiles.Create().Rlocation(os.environ["MULTIRUN_EXECUTABLE"])).with_name(
    "multirun.py"
)


class MultirunTest(unittest.TestCase):
    def run_main(self, launch_results, forward_stdin=False):
        with tempfile.TemporaryDirectory() as directory:
            instructions = Path(directory) / "instructions.json"
            instructions.write_text(
                json.dumps(
                    {
                        "workspace_name": "_main",
                        "commands": [
                            {
                                "path": "push" + str(i),
                                "tag": "push" + str(i),
                                "args": [],
                                "env": {},
                            }
                            for i in range(4)
                        ],
                        "jobs": 0,
                        "print_command": False,
                        "buffer_output": True,
                        "forward_stdin": forward_stdin,
                        "keep_going": False,
                    }
                )
            )
            resolver = mock.Mock()
            resolver.Rlocation.side_effect = lambda path: path
            with mock.patch.object(
                runfiles, "Create", return_value=resolver
            ), mock.patch("sys.argv", [str(RUNNER), str(instructions)]), mock.patch(
                "subprocess.Popen", side_effect=launch_results
            ) as launch, mock.patch(
                "threading.Thread"
            ) as thread, contextlib.redirect_stdout(
                io.StringIO()
            ) as stdout, contextlib.redirect_stderr(
                io.StringIO()
            ), self.assertRaises(
                SystemExit
            ) as exit:
                runpy.run_path(str(RUNNER), run_name="__main__")
            return (
                exit.exception.code,
                launch.call_count,
                thread.call_count,
                stdout.getvalue(),
            )

    def child(self, code=0):
        child = mock.Mock()
        child.stdout = io.StringIO("child output\n")
        child.returncode = code
        return child

    def test_partial_launch_failure_drains_and_waits_for_started_children(self):
        children = [self.child(), self.child()]
        code, launched, _, output = self.run_main(
            [*children, OSError("cannot execute push2")]
        )
        self.assertEqual(code, 1)
        self.assertEqual(launched, 3)
        self.assertEqual(output.count("child output"), 2)
        for child in children:
            child.wait.assert_called_once_with()

    def test_first_launch_failure_returns_without_starting_stdin_thread(self):
        code, launched, threads, _ = self.run_main(
            [OSError("cannot execute push0")], forward_stdin=True
        )
        self.assertEqual(code, 1)
        self.assertEqual(launched, 1)
        self.assertEqual(threads, 0)

    def test_interrupted_launch_signals_and_waits_for_started_children(self):
        children = [self.child(), self.child()]
        code, launched, _, _ = self.run_main([*children, KeyboardInterrupt()])
        self.assertEqual(code, 1)
        self.assertEqual(launched, 3)
        for child in children:
            child.send_signal.assert_called_once_with(signal.SIGINT)
            child.wait.assert_called_once_with()

    def test_child_exit_failure_still_waits_for_every_child(self):
        children = [self.child(1), self.child(), self.child(), self.child()]
        code, launched, _, _ = self.run_main(children)
        self.assertEqual(code, 1)
        self.assertEqual(launched, 4)
        for child in children:
            child.wait.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
