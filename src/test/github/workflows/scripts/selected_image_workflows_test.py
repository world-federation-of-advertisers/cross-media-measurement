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

"""Exercise selected-image workflow commands without publishing artifacts."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[5]


def generated_targets():
    targets = set()
    scope = {
        "load": lambda *args: None,
        "struct": SimpleNamespace,
        "IMAGE_REPOSITORY_SETTINGS": SimpleNamespace(
            repository_prefix="test", container_registry="example.invalid", image_tag="test"
        ),
    }
    exec((ROOT / "src/main/docker/images.bzl").read_text(), scope)

    def target(**kwargs):
        name = "//src/main/docker:" + kwargs["name"]
        assert name not in targets, name
        targets.add(name)

    scope.update(package=lambda **kwargs: None, container_push=target,
                 container_push_all=target, sign_all=target)
    exec((ROOT / "src/main/docker/BUILD.bazel").read_text(), scope)
    return targets


def step_commands(file, name):
    text = (ROOT / ".github/workflows" / file).read_text()
    step = text.split("- name: " + name, 1)[1]
    return textwrap.dedent(step.split("run: |", 1)[1])


class SelectedImageWorkflowsTest(unittest.TestCase):
    def run_step(self, file, step, name):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            targets = root / "targets.json"
            targets.write_text(json.dumps(sorted(generated_targets())))
            log = root / "calls.jsonl"
            bazel = root / "bazelisk"
            bazel.write_text("#!" + sys.executable + "\n" + textwrap.dedent("""
                import json, os, sys
                from pathlib import Path
                args = sys.argv[1:]
                with open(os.environ['BAZEL_TEST_CALLS'], 'a') as output:
                    output.write(json.dumps(args) + '\\n')
                if args[0] == 'query':
                    targets = json.loads(Path(os.environ['BAZEL_TEST_TARGETS']).read_text())
                    sys.exit(0 if args[1] in targets else 1)
                if args[0] != 'run':
                    sys.exit('Unexpected publishing command')
            """))
            bazel.chmod(0o755)
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"],
                       IMAGE_NAME=name, PUBLIC_KEY="test-public-key",
                       GCP_KMS_KEY_FOR_SIGNING="test-signing-key",
                       BAZEL_TEST_CALLS=str(log), BAZEL_TEST_TARGETS=str(targets))
            result = subprocess.run(["bash", "-c", step_commands(file, step)],
                                    cwd=ROOT, env=env, capture_output=True, text=True)
            calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            return result, calls

    def test_supported_image_is_validated_before_publish_and_sign(self):
        name = "gcloud_reporting_v2_internal_server_image"
        for file, step, operation in [
            ("push-images.yml", "Build and push selected image", "push"),
            ("sign-images.yml", "Sign images", "sign"),
        ]:
            with self.subTest(file=file):
                result, calls = self.run_step(file, step, name)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(calls[0][:2], ["query", "//src/main/docker:sign_" + name])
                self.assertEqual(calls[1][:2], ["run", "//src/main/docker:" + operation + "_" + name])

    def test_local_image_with_push_target_is_rejected_before_publish_or_sign(self):
        name = "forwarded_storage_herald_daemon_image"
        self.assertIn("//src/main/docker:push_" + name, generated_targets())
        self.assertNotIn("//src/main/docker:sign_" + name, generated_targets())
        for file, step in [("push-images.yml", "Build and push selected image"),
                           ("sign-images.yml", "Sign images")]:
            with self.subTest(file=file):
                result, calls = self.run_step(file, step, name)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(len(calls), 1)
                self.assertEqual(calls[0][0], "query")

    def test_empty_selection_preserves_bulk_signing(self):
        result, calls = self.run_step("sign-images.yml", "Sign images", "")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([call[:2] for call in calls], [
            ["run", "//src/main/docker:sign_all_images"],
            ["run", "//src/main/docker/panel_exchange_client:sign_all_images"],
        ])


if __name__ == "__main__":
    unittest.main()
