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

"""Security and runtime-config tests for the TEE KMS deployment helper."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[4]
spec = importlib.util.spec_from_file_location("tee", ROOT / ".github/workflows/scripts/prepare_tee_workloads.py")
tee = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tee)


class PrepareTeeWorkloadsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = json.loads((ROOT / ".github/env-configs/tee-workloads.json").read_text())
        self.config["activate_environments"] = {env: False for env in tee.ENVIRONMENTS}
        self.config["retire_legacy_federation"] = False
        path = self.root / ".github/env-configs/tee-workloads.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(self.config))
        for env in tee.ENVIRONMENTS:
            path = self.root / f".github/env-configs/{env}/cmms.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({"EDPA_META_AWS_REGION": "us-east-1"}))
        patch = mock.patch.object(tee, "REPOSITORY", self.root)
        patch.start()
        self.addCleanup(patch.stop)

    def test_render_preserves_input_config_and_selects_cloud_and_output_purpose(self):
        text = '# Ignore a commented brace: }\nevent_data_provider_config {\n data_provider: "dataProviders/example"\n kms_config { kms_type: AWS_CONFIDENTIAL_SPACE }\n}\n'
        text += 'event_data_provider_config { data_provider: "dataProviders/google" kms_config { kms_type: GCP } }\n'
        outputs = {}
        for env in tee.ENVIRONMENTS:
            path = self.root / f"src/main/terraform/gcloud/cmms/envs/{env}/event-data-provider-configs.textproto"
            path.parent.mkdir(parents=True)
            path.write_text(text)
            outputs[env] = {
                "workloads": {job: {"kms_audience": "provider/" + job, "service_account": job + "@example.com", "aws_role_arn": "role/" + job, "aws_audience": "subject/" + job} for job in self.config["jobs"]},
                "aws_kek_uris": {w["output_purpose"]: "aws-kms://" + w["output_purpose"] for w in self.config["jobs"].values()},
                "google_kek_uris": {w["output_purpose"]: "gcp-kms://" + w["output_purpose"] for w in self.config["jobs"].values()},
            }
        output = self.root / "terraform.json"
        output.write_text(json.dumps({"environments": {"value": outputs}}))
        tee.render(self.root / "rendered", output)
        rendered = (self.root / "rendered/dev.textproto").read_text()
        self.assertEqual(rendered.count("workload_kms_configs {"), 10)
        self.assertEqual(rendered.count("kms_type: AWS\n"), 5)
        self.assertEqual(rendered.count("kms_type: GCP\n"), 5)
        self.assertIn('kek_uri: "aws-kms://rank_index"', rendered)
        self.assertIn('kek_uri: "aws-kms://labeled_impressions"', rendered)
        self.assertIn('kek_uri: "gcp-kms://rank_index"', rendered)
        self.assertIn('kek_uri: "gcp-kms://labeled_impressions"', rendered)
        source = self.root / "src/main/terraform/gcloud/cmms/envs/dev/event-data-provider-configs.textproto"
        self.assertEqual(source.read_text(), text)

    def test_retirement_requires_activation_in_every_environment(self):
        self.config["retire_legacy_federation"] = True
        (self.root / ".github/env-configs/tee-workloads.json").write_text(json.dumps(self.config))
        with self.assertRaisesRegex(ValueError, "every environment"):
            tee.configuration()

    def test_activation_rejects_a_release_without_job_config_support(self):
        self.config["activate_environments"]["dev"] = True
        (self.root / ".github/env-configs/tee-workloads.json").write_text(json.dumps(self.config))
        with mock.patch.object(tee, "command", return_value=b'{"content":""}'):
            with self.assertRaisesRegex(ValueError, "does not support"):
                tee.validate_version("dev", "old-release")

    def test_block_parser_ignores_braces_in_quoted_values(self):
        text = 'event_data_provider_config { data_provider: "dataProviders/{example}" kms_config { kms_type: GCP } }'
        self.assertEqual(len(list(tee.provider_blocks(text))), 1)
        with self.assertRaisesRegex(ValueError, "Unbalanced"):
            list(tee.provider_blocks(text[:-1]))

    def test_sign_rejects_copying_another_globally_signed_job_into_an_approved_repo(self):
        job = self.config["jobs"]["trustee-mill"]
        repo = job["image_repositories"][0]
        other = self.config["jobs"]["results-fulfiller"]["image_repositories"][0]
        signature = {"critical": {"identity": {"docker-reference": other}, "image": {"docker-manifest-digest": "sha256:" + "a" * 64}}}
        keys = {name: {"public_key": "test", "name": "kms/" + name} for name in self.config["jobs"]}
        with mock.patch.object(tee, "deployed_images", return_value=(repo + ":v1", repo)), mock.patch.object(tee, "signing_keys", return_value=keys), mock.patch.object(tee, "command", return_value=json.dumps([signature]).encode()) as command:
            with self.assertRaisesRegex(ValueError, "identifying trustee-mill"):
                tee.sign("dev", self.root, None, None)
        self.assertEqual(command.call_count, 1)
        self.assertEqual(command.call_args.args[1], "verify")

    def test_sign_uses_the_verified_digest_and_a_different_key_for_each_job(self):
        keys = {name: {"public_key": "test", "name": "kms/" + name} for name in self.config["jobs"]}
        calls = []

        def images(env, values, workload):
            repo = workload["image_repositories"][0]
            return repo + ":mutable", repo

        def command(*args):
            calls.append(args)
            if args[1] == "verify":
                repo = args[-1].rsplit(":", 1)[0]
                return json.dumps([{"critical": {"identity": {"docker-reference": repo}, "image": {"docker-manifest-digest": "sha256:" + "b" * 64}}}]).encode()
            return b""

        with mock.patch.object(tee, "deployed_images", side_effect=images), mock.patch.object(tee, "signing_keys", return_value=keys), mock.patch.object(tee, "command", side_effect=command):
            tee.sign("dev", self.root, None, None)
        signs = [args for args in calls if args[1] == "sign"]
        self.assertEqual(len(signs), 5)
        self.assertTrue(all(args[-1].endswith("@sha256:" + "b" * 64) for args in signs))
        self.assertEqual(len({args[args.index("--key") + 1] for args in signs}), 5)


if __name__ == "__main__":
    unittest.main()
