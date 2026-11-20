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

"""Checks signing isolation, completion, and restricted retries without a registry."""

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[5] / ".github/workflows/scripts/sign_image_manifest.py"
SPEC = importlib.util.spec_from_file_location("sign_image_manifest", SCRIPT)
signing = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(signing)


class SignImageManifestTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "images.json"
        self.images = [
            {"repository": f"ghcr.io/halo/image{i}", "digest": "sha256:" + "a" * 64, "sign": True}
            for i in range(9)
        ]
        self.write_manifest()

    def write_manifest(self, commit="commit", image_tag="tag"):
        self.path.write_text(json.dumps({"commit": commit, "image_tag": image_tag, "images": self.images}))

    def run_main(self):
        with mock.patch("sys.argv", [str(SCRIPT), "--manifest", str(self.path), "--commit", "commit",
                                     "--image-tag", "tag", "--cosign", "cosign", "--key", "gcpkms://key",
                                     "--public-key", "public-key"]):
            return signing.main()

    def test_four_workers_join_after_failure_and_skip_unsigned(self):
        self.images[-1]["sign"] = False
        self.write_manifest()
        active = 0
        maximum = 0
        completed = []
        lock = threading.Lock()
        first_batch = threading.Barrier(4)

        def run(command, **kwargs):
            nonlocal active, maximum
            reference = command[2]
            index = int(reference.split("image")[1].split("@")[0])
            with lock:
                active += 1
                maximum = max(maximum, active)
            if index < 4:
                first_batch.wait(timeout=5)
            with lock:
                active -= 1
                completed.append(reference)
            return subprocess.CompletedProcess(command, 1 if index == 0 else 0, "", "denied" if index == 0 else "")

        with mock.patch.object(signing.subprocess, "run", side_effect=run) as execution:
            self.assertEqual(self.run_main(), 1)
        self.assertEqual(maximum, 4)
        self.assertEqual(active, 0)
        self.assertEqual(len(completed), 8)
        self.assertEqual(len(set(completed)), 8)
        for call in execution.call_args_list:
            self.assertIn("--key=gcpkms://key", call.args[0])
            self.assertIn("--tlog-upload=false", call.args[0])
            self.assertIn("dev.cosignproject.cosign/pub=public-key", call.args[0])
            self.assertIn("dev.cosignproject.cosign/sigalg=ECDSA_P256_SHA256", call.args[0])

    def test_invalid_manifest_starts_no_workers(self):
        for invalid in ["duplicate", "digest", "commit", "tag", "repository"]:
            with self.subTest(invalid=invalid):
                self.setUp()
                if invalid == "duplicate":
                    self.images.append(self.images[0])
                elif invalid == "digest":
                    self.images[-1]["digest"] = "latest"
                elif invalid == "repository":
                    self.images[-1]["repository"] = "ghcr.io/halo//image"
                self.write_manifest(commit="other" if invalid == "commit" else "commit",
                                    image_tag="other" if invalid == "tag" else "tag")
                with mock.patch.object(signing.subprocess, "run") as execution:
                    self.assertEqual(self.run_main(), 1)
                    execution.assert_not_called()

    def test_only_identity_transport_errors_are_retried(self):
        reference = self.images[0]
        temporary = "invalid response when retrieving subject token: dial tcp: timeout"
        with mock.patch.object(signing.subprocess, "run", side_effect=[
            subprocess.CompletedProcess([], 1, "", temporary),
            subprocess.CompletedProcess([], 1, "", temporary),
            subprocess.CompletedProcess([], 0, "", ""),
        ]) as execution, mock.patch.object(signing.time, "sleep") as sleep:
            signing.sign_image(reference, "cosign", "key", "public")
            self.assertEqual(execution.call_count, 3)
            self.assertEqual(sleep.call_args_list, [mock.call(15), mock.call(30)])
        for error in ["registry timeout", "invalid response when retrieving subject token: permission denied"]:
            with mock.patch.object(signing.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, "", error)) as execution:
                with self.assertRaises(RuntimeError):
                    signing.sign_image(reference, "cosign", "key", "public")
                self.assertEqual(execution.call_count, 1)


if __name__ == "__main__":
    unittest.main()
