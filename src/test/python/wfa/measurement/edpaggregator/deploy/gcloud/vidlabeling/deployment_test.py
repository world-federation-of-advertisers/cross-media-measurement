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
import re
import subprocess
import tempfile
import unittest

ROOT = Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"]
CMMS = ROOT / "src/main/terraform/gcloud/cmms"
MODULES = ROOT / "src/main/terraform/gcloud/modules"


def string_value(text, name):
    match = re.search(r"(?m)^" + name + r'\s*=\s*"([^"\n]*)"', text)
    if not match:
        raise AssertionError("Missing Terraform value: " + name)
    return match.group(1)


class VidLabelingDeploymentTest(unittest.TestCase):
    def test_each_environment_uses_its_existing_public_targets_and_tls_paths(self):
        for environment in ("dev", "head", "qa"):
            with self.subTest(environment=environment):
                variables = (CMMS / "envs" / (environment + ".tfvars")).read_text()
                dispatcher = dict(item.split("=", 1) for item in string_value(variables, "vid_labeling_dispatcher_env_var").split(","))
                controller = dict(item.split("=", 1) for item in string_value(variables, "vid_labeling_healing_controller_env_var").split(","))
                for key in ("RAW_IMPRESSION_UPLOAD_TARGET", "RAW_IMPRESSION_UPLOAD_CERT_HOST", "CONTROL_PLANE_TARGET", "CONTROL_PLANE_CERT_HOST"):
                    self.assertEqual(controller[key], dispatcher[key])
                self.assertNotIn("HEALING_INTERNAL_API_TARGET", controller)
                self.assertNotIn("CORRECTION_DETECTION_TARGET", dispatcher)
                secrets = dict(item.split("=", 1) for item in string_value(variables, "vid_labeling_healing_controller_secret_mapping").split(","))
                config = (CMMS / "envs" / environment / "vid-labeling-dispatcher-config.textproto").read_text()
                connections = re.findall(r"(?:raw_impression_metadata_storage_connection|control_plane_connection)\s*\{([^}]+)\}", config)
                self.assertGreater(len(connections), 0)
                for connection in connections:
                    for path in re.findall(r'(?:cert_file_path|private_key_file_path|cert_collection_file_path):\s*"([^"\n]+)"', connection):
                        self.assertIn(path, secrets)
                        self.assertTrue(secrets[path].endswith(":latest"))
                declared = re.search(r"vid_labeling_healing_labeled_output_object_prefixes\s*=\s*\[([^]]+)\]", variables).group(1)
                allowed = re.findall(r'"([^"\n]+)"', declared)
                expected = {path.strip("/") + "/" for path in re.findall(r'edp_impression_path:\s*"([^"\n]+)"', config)}
                self.assertTrue(expected.issubset(set(allowed)))
                self.assertTrue(all(path.endswith("/") for path in allowed))

    def test_workflow_reads_checked_in_values_without_github_overrides(self):
        workflow = (ROOT / ".github/workflows/terraform-cmms-v2.yml").read_text()
        for name in ("VID_LABELING_HEALING_CONTROLLER_ENV_VAR", "VID_LABELING_HEALING_CONTROLLER_SECRET_MAPPING", "VID_LABELING_HEALING_LABELED_OUTPUT_OBJECT_PREFIXES"):
            self.assertNotIn(name, workflow)
        for name in ("vid_labeling_healing_controller_env_var", "vid_labeling_healing_controller_secret_mapping", "vid_labeling_healing_labeled_output_object_prefixes"):
            self.assertNotIn('-var="' + name + "=", workflow)
        self.assertIn("-var-file=envs/${{ inputs.environment }}.tfvars", workflow)
        self.assertNotIn("HEALING_INTERNAL_API_TARGET", (CMMS / "variables.tf").read_text())

    def test_quiescence_defers_deploy_and_activation_runs_the_real_deploy_command(self):
        module = MODULES / "http-cloud-function"
        content = (module / "main.tf").read_text()
        command = re.search(r"command\s*=\s*<<-EOT\n(.*?)\n\s*EOT", content, re.S).group(1)
        command = command.replace("${path.module}", str(module)).replace("$${", "${")
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            log = directory / "calls.jsonl"
            binary = directory / "gcloud"
            binary.write_text("#!/usr/bin/env python3\nimport json,os,sys\nwith open(os.environ['DEPLOY_TEST_LOG'], 'a') as f: f.write(json.dumps(sys.argv[1:])+'\\n')\n")
            binary.chmod(0o755)
            environment = dict(os.environ, PATH=str(directory) + os.pathsep + os.environ["PATH"], DEPLOY_TEST_LOG=str(log), FUNCTION_NAME="test-dispatcher", FUNCTION_MEMORY="512MB", ENTRY_POINT="test.Dispatcher", CLOUD_REGION="us-central1", RUN_SERVICE_ACCOUNT="test@example.invalid", EXTRA_ENV_VARS="RAW_IMPRESSION_UPLOAD_TARGET=system.example:8443", SECRET_MAPPINGS="", UBER_JAR_DIRECTORY=str(directory), TIMEOUT_SECONDS="600", MAX_INSTANCES="1", CONCURRENCY="1")
            for enabled in ("false", "true"):
                subprocess.run(["bash", "-c", command], env=dict(environment, DEPLOYMENT_ENABLED=enabled), check=True, timeout=15)
                if enabled == "false":
                    self.assertFalse(log.exists())
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][:3], ["functions", "deploy", "test-dispatcher"])
            self.assertIn("--no-allow-unauthenticated", calls[0])
            self.assertIn("--run-service-account=test@example.invalid", calls[0])
            self.assertIn("--memory=512MB", calls[0])
        edpa = (MODULES / "edp-aggregator/main.tf").read_text()
        for name in ("vid_labeling_dispatcher_cloud_function", "vid_labeling_healing_controller_cloud_function"):
            block = re.search(r'module "' + name + r'"\s*\{(.*?)\n\}', edpa, re.S).group(1)
            self.assertRegex(block, r"deployment_enabled\s*=\s*var.tee_consumers_enabled")
        self.assertRegex((CMMS / "edp_aggregator.tf").read_text(), r"paused\s*=\s*!var.tee_consumers_enabled")

    def test_recovery_writer_permission_is_bound_to_healing_and_done_markers(self):
        edpa = (MODULES / "edp-aggregator/main.tf").read_text()
        block = re.search(r'resource "google_storage_bucket_iam_member" "vid_labeling_healing_controller_done_writer"\s*\{(.*?)\n\}', edpa, re.S).group(1)
        self.assertRegex(block, r'role\s*=\s*"roles/storage.objectUser"')
        self.assertIn("module.vid_labeling_healing_controller_cloud_function.cloud_function_service_account.email", block)
        expression = re.search(r'expression\s*=\s*"([^"\n]+)"', block).group(1)
        self.assertEqual(expression, "resource.name.endsWith('/done')")
        self.assertIn("google_storage_bucket_iam_member.vid_labeling_healing_controller_done_writer,", edpa)


if __name__ == "__main__":
    unittest.main()
