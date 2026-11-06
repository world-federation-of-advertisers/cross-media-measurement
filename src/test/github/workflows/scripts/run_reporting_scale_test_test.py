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

import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[5] / ".github/workflows/scripts/run_reporting_scale_test.py"
REPORT = "measurementConsumers/test/basicReports/reporting-scale-test"


class ReportingScaleControllerTest(unittest.TestCase):
    def run_scenario(self, *, creation_verified=True, create_code=0,
                     cleanup_complete=False, initial_state="ENABLED", reuse_pause=False,
                     verification_retryable=False):
        spec = importlib.util.spec_from_file_location("reporting_scale_controller", SCRIPT)
        controller = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(controller)
        events = []
        state = [initial_state]
        forwarded_reports = []

        def scheduler(action, job=None):
            events.append(action)
            if action == "list":
                return [{"name": "projects/test/locations/test/jobs/fetcher", "state": state[0],
                         "httpTarget": {"uri": "https://fetcher.test"}}]
            if action == "pause":
                state[0] = "PAUSED"
            if action == "resume":
                state[0] = "ENABLED"
            if action == "describe":
                return {"name": "fetcher", "state": state[0]}

        def run_phase(environment, destination):
            phase = environment["REPORTING_SCALE_PHASE"]
            events.append(phase)
            if phase == "creation":
                self.assertEqual(state[0], "PAUSED")
                self.assertEqual(environment["REPORTING_SCALE_EXISTING_BASIC_REPORT"], "")
                self.assertEqual(environment["REPORTING_SCALE_CANCEL_FAILED_CREATION"], "true")
                return create_code, [{"basic_report": REPORT, "creation_verified": creation_verified,
                                      "outcome": "creation_succeeded" if creation_verified else "failed",
                                      "cleanup_complete": cleanup_complete,
                                      "create_http_status": 200 if verification_retryable else 500,
                                      "creation_verification_retryable": verification_retryable}]
            self.assertEqual(state[0], "ENABLED")
            self.assertEqual(environment["REPORTING_SCALE_CANCEL_FAILED_CREATION"], "false")
            forwarded_reports.append(environment["REPORTING_SCALE_EXISTING_BASIC_REPORT"])
            return 0, [{"basic_report": REPORT, "outcome": "fulfilled_existing"}]

        environment = {"REQUISITION_FETCHER_ENDPOINT": "https://fetcher.test",
                       "REPORTING_SCALE_REUSE_PAUSED_FETCHER": str(reuse_pause).lower()}
        with tempfile.TemporaryDirectory() as directory:
            controller.OUTPUT = Path(directory)
            with patch.dict(os.environ, environment, clear=True), \
                 patch.object(controller, "scheduler", scheduler), \
                 patch.object(controller, "run_phase", run_phase), \
                 patch.object(controller.subprocess, "run"), \
                 patch.object(controller.subprocess, "check_output", return_value="/tmp/output-base"):
                error = None
                try:
                    controller.main()
                except RuntimeError as exception:
                    error = exception
            state_file = controller.OUTPUT / "fetcher-state.json"
            recorded_state = json.loads(state_file.read_text())["state"] if state_file.exists() else None
        return events, state[0], forwarded_reports, error, recorded_state

    def test_private_api_targets_reuses_explicit_connections(self):
        spec = importlib.util.spec_from_file_location("reporting_scale_controller", SCRIPT)
        controller = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(controller)
        environment = {
            "REPORTING_SCALE_INTERNAL_API_TARGET": "127.0.0.1:10001",
            "REPORTING_SCALE_KINGDOM_INTERNAL_API_TARGET": "127.0.0.1:10002",
        }
        with patch.object(controller.subprocess, "run") as run, \
             patch.object(controller.subprocess, "Popen") as popen:
            with controller.internal_api_targets(environment) as forwarded:
                self.assertEqual(forwarded, environment)
            run.assert_not_called()
            popen.assert_not_called()

    def test_main_resumes_fetching_after_creation_and_reuses_the_same_report(self):
        events, state, reports, error, recorded = self.run_scenario()
        self.assertIsNone(error)
        self.assertEqual(state, "ENABLED")
        self.assertEqual(recorded, "ENABLED")
        self.assertEqual(reports, [REPORT])
        self.assertLess(events.index("creation"), events.index("resume"))
        self.assertLess(events.index("resume"), events.index("run"))
        self.assertLess(events.index("run"), events.index("full"))

    def test_main_keeps_fetching_paused_after_failed_creation_and_successful_cleanup(self):
        events, state, reports, error, recorded = self.run_scenario(
            creation_verified=False, create_code=1, cleanup_complete=True)
        self.assertIsNotNone(error)
        self.assertEqual(state, "PAUSED")
        self.assertEqual(recorded, "PAUSED")
        self.assertEqual(reports, [])
        self.assertNotIn("resume", events)
        self.assertNotIn("run", events)
        self.assertNotIn("full", events)

    def test_main_keeps_fetching_paused_when_a_successful_create_cannot_be_verified(self):
        events, state, reports, error, recorded = self.run_scenario(
            creation_verified=False, create_code=1, verification_retryable=True)
        self.assertIsNotNone(error)
        self.assertEqual(state, "PAUSED")
        self.assertEqual(recorded, "PAUSED")
        self.assertEqual(reports, [])
        self.assertNotIn("resume", events)
        self.assertNotIn("run", events)
        self.assertNotIn("full", events)

    def test_main_keeps_fetching_paused_when_cleanup_is_incomplete(self):
        events, state, reports, error, recorded = self.run_scenario(
            creation_verified=False, create_code=1)
        self.assertIsNotNone(error)
        self.assertEqual(state, "PAUSED")
        self.assertEqual(recorded, "PAUSED")
        self.assertNotIn("resume", events)
        self.assertNotIn("full", events)

    def test_main_requires_creation_verification_even_when_the_test_exits_successfully(self):
        events, state, reports, error, recorded = self.run_scenario(creation_verified=False)
        self.assertIsNotNone(error)
        self.assertEqual(state, "PAUSED")
        self.assertNotIn("resume", events)
        self.assertNotIn("full", events)

    def test_main_rejects_an_existing_pause_without_explicit_reuse(self):
        events, state, reports, error, recorded = self.run_scenario(initial_state="PAUSED")
        self.assertIsNotNone(error)
        self.assertEqual(state, "PAUSED")
        self.assertNotIn("creation", events)
        self.assertNotIn("resume", events)

    def test_main_can_reuse_its_previous_pause_and_resume_after_successful_creation(self):
        events, state, reports, error, recorded = self.run_scenario(
            initial_state="PAUSED", reuse_pause=True)
        self.assertIsNone(error)
        self.assertEqual(state, "ENABLED")
        self.assertEqual(reports, [REPORT])
        self.assertNotIn("pause", events)
        self.assertLess(events.index("creation"), events.index("resume"))


if __name__ == "__main__":
    unittest.main()
