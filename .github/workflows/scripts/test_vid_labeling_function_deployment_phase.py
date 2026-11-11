"""Run the production function deploy script against a recording gcloud executable."""

import json
import os
import pathlib
import re
import sys
import subprocess
import tempfile
import textwrap
import unittest

REPO = pathlib.Path(__file__).resolve().parents[3]
MODULE = REPO / "src/main/terraform/gcloud/modules/http-cloud-function"


class FunctionDeploymentPhaseTest(unittest.TestCase):
    def test_preparation_retains_function_and_activation_deploys_once(self):
        source = (MODULE / "main.tf").read_text()
        script = textwrap.dedent(source.split("command = <<-EOT", 1)[1].split("\n    EOT", 1)[0])
        script = script.replace("${path.module}", str(MODULE)).replace("$${", "${")
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            calls = root / "calls.jsonl"
            fake = root / "gcloud"
            fake.write_text("#!/usr/bin/env python3\nimport json, os, pathlib, sys\nwith pathlib.Path(os.environ['GCLOUD_CALLS']).open('a') as out: out.write(json.dumps(sys.argv[1:]) + '\\n')\nprint('deployment complete')\n")
            fake.chmod(0o755)
            env = os.environ.copy()
            env.update({
                "PATH": str(root) + os.pathsep + env.get("PATH", ""),
                "GCLOUD_CALLS": str(calls),
                "FUNCTION_NAME": "vid-labeling-dispatcher",
                "ENTRY_POINT": "org.example.Function",
                "CLOUD_REGION": "us-central1",
                "RUN_SERVICE_ACCOUNT": "machine@example.iam.gserviceaccount.com",
                "EXTRA_ENV_VARS": "VID_LABELING_MACHINE_API_AUDIENCE=https://system.example/vid-labeling-machine-api",
                "SECRET_MAPPINGS": "",
                "UBER_JAR_DIRECTORY": str(root),
                "TIMEOUT_SECONDS": "600",
                "MAX_INSTANCES": "1",
                "CONCURRENCY": "1",
                "DEPLOYMENT_ENABLED": "false",
            })
            subprocess.run(["bash", "-c", script], env=env, check=True, capture_output=True, text=True)
            self.assertFalse(calls.exists())
            env["DEPLOYMENT_ENABLED"] = "true"
            subprocess.run(["bash", "-c", script], env=env, check=True, capture_output=True, text=True)
            recorded = [json.loads(line) for line in calls.read_text().splitlines()]
            self.assertEqual(len(recorded), 1)
            self.assertEqual(recorded[0][:3], ["functions", "deploy", "vid-labeling-dispatcher"])
            self.assertIn("--set-env-vars=" + env["EXTRA_ENV_VARS"], recorded[0])
            self.assertIn("--run-service-account=" + env["RUN_SERVICE_ACCOUNT"], recorded[0])
            self.assertIn("--no-allow-unauthenticated", recorded[0])


    def test_rollout_workflow_forwards_preparation_and_activation_modes(self):
        workflow = (REPO / ".github/workflows/terraform-cmms-v2.yml").read_text()
        rollout = (REPO / ".github/workflows/update-cmms.yml").read_text()
        input_block = workflow.split("      vid-labeling-machine-functions-enabled:", 1)[1].split("    outputs:", 1)[0]
        default = re.search(r"default:\s*(true|false)", input_block).group(1)
        plan_step = workflow.split("    - name: terraform plan", 1)[1].split("    - name: terraform apply", 1)[0]
        command = " ".join(line.strip() for line in plan_step.split("      run: >", 1)[1].splitlines())
        blocks = (
            (rollout.split("  terraform-quiesced:", 1)[1].split("  wait-for-tee-consumers-to-stop:", 1)[0], "false"),
            (rollout.split("  terraform:\n", 1)[1].split("  update-aggregator-duchy:", 1)[0], "true"),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            calls = root / "terraform.jsonl"
            fake = root / "terraform"
            fake.write_text("#!" + sys.executable + "\nimport json, os, pathlib, sys\nwith pathlib.Path(os.environ['TF_CALLS']).open('a') as out: out.write(json.dumps(sys.argv[1:]) + '\\n')\n")
            fake.chmod(0o755)
            env = {"PATH": str(root) + os.pathsep + "/usr/bin:/bin", "TF_CALLS": str(calls)}
            for block, expected in blocks:
                match = re.search(r"vid-labeling-machine-functions-enabled:\s*(true|false)", block)
                mode = match.group(1) if match else default
                def substitute(match):
                    expression = match.group(1).strip()
                    if expression == "inputs.vid-labeling-machine-functions-enabled":
                        return mode
                    if expression == "inputs.environment":
                        return "dev"
                    return "fixture"
                actual_command = re.sub(r"\$\{\{(.*?)\}\}", substitute, command)
                subprocess.run(["bash", "-c", actual_command], env=env, cwd=root, check=True, capture_output=True)
                recorded = json.loads(calls.read_text().splitlines()[-1])
                self.assertEqual(recorded[0], "plan")
                self.assertIn("-var=vid_labeling_machine_functions_deployment_enabled=" + expected, recorded)


if __name__ == "__main__":
    unittest.main()
