"""CLI regression coverage for machine authorization configuration."""

import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).with_name("write_vid_labeling_machine_api_config.py")
REPO = SCRIPT.parents[3]


class WriteMachineConfigTest(unittest.TestCase):
    def render(self, config, audience, require=True):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / "config.textproto"
            args = [sys.executable, str(SCRIPT), "--audience", audience, "--output", str(output)]
            if config is not None:
                args += ["--config-json", json.dumps(config)]
            if require:
                args.append("--require-subjects")
            result = subprocess.run(args, text=True, capture_output=True)
            return result.returncode, output.read_text() if output.exists() else ""

    def config(self, audience):
        return {"audience": audience, "dispatcher_service_account_subjects": ["111111111111111111111"], "healing_controller_service_account_subjects": ["222222222222222222222"]}

    def test_all_environments_render_their_own_audience_and_distinct_subjects(self):
        for environment in ("dev", "head", "qa"):
            with self.subTest(environment=environment):
                settings = json.loads((REPO / ".github" / "env-configs" / environment / "cmms.json").read_text())
                audience = "https://" + settings["METADATA_STORAGE_PUBLIC_API_TARGET"] + "/vid-labeling-machine-api"
                status, text = self.render(self.config(audience), audience)
                self.assertEqual(status, 0)
                self.assertIn(json.dumps(audience), text)
                self.assertIn('dispatcher_service_account_subjects: "111111111111111111111"', text)
                self.assertIn('healing_controller_service_account_subjects: "222222222222222222222"', text)

    def test_apply_rejects_missing_terraform_subjects(self):
        audience = "https://system.example/vid-labeling-machine-api"
        status, text = self.render(None, audience)
        self.assertNotEqual(status, 0)
        self.assertEqual(text, "")

    def test_plan_can_render_without_apply_time_subjects(self):
        audience = "https://system.example/vid-labeling-machine-api"
        status, text = self.render(None, audience, require=False)
        self.assertEqual(status, 0)
        self.assertNotIn("service_account_subjects", text)

    def test_apply_rejects_a_subject_shared_between_roles(self):
        audience = "https://system.example/vid-labeling-machine-api"
        config = self.config(audience)
        config["healing_controller_service_account_subjects"] = config["dispatcher_service_account_subjects"]
        self.assertNotEqual(self.render(config, audience)[0], 0)

    def test_apply_rejects_wrong_audience_and_non_service_account_subjects(self):
        audience = "https://system.example/vid-labeling-machine-api"
        config = self.config(audience)
        config["audience"] = "https://other.example/vid-labeling-machine-api"
        self.assertNotEqual(self.render(config, audience)[0], 0)
        config = self.config(audience)
        config["dispatcher_service_account_subjects"] = ["ordinary-user@example.org"]
        self.assertNotEqual(self.render(config, audience)[0], 0)


if __name__ == "__main__":
    unittest.main()
