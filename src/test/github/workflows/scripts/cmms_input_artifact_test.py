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

"""Verify deployment artifact identity and file integrity without Terraform."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

SCRIPT = (
    Path(__file__).resolve().parents[5]
    / ".github/workflows/scripts/cmms_input_artifact.py"
)
SPEC = importlib.util.spec_from_file_location("cmms_input_artifact", SCRIPT)
artifact = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(artifact)


class CmmsInputArtifactTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.jars = self.root / "uber-jars"
        for key in artifact.JAR_KEYS:
            directory = self.jars / key
            directory.mkdir(parents=True)
            (directory / "0.0.0-tag-SNAPSHOT-function.jar").write_bytes(key.encode())
        self.descriptors = self.root / "descriptors.pb"
        self.descriptors.write_bytes(b"descriptors")
        self.certs = self.root / "public.pem"
        self.certs.write_bytes(b"public roots")
        (self.root / "auth.bazelrc").write_text("credential")
        self.directory = self.root / "artifact"
        artifact.pack(
            self.directory,
            self.jars,
            self.descriptors,
            self.certs,
            "commit",
            "dev",
            "tag",
        )

    def test_round_trip_has_only_allowlisted_inputs(self):
        outputs = artifact.verify(self.directory, "commit", "dev", "tag")
        self.assertEqual(
            {p.name for p in self.directory.iterdir()},
            artifact.INPUT_FILES | {"manifest.json"},
        )
        self.assertEqual(len(outputs), 11)
        for key in artifact.JAR_KEYS:
            path = Path(outputs[key.replace("-", "_") + "_source_path"])
            self.assertEqual(path.read_bytes(), key.encode())
        self.assertEqual(
            Path(outputs["event_proto_descriptors_path"]).read_bytes(), b"descriptors"
        )

    def test_commit_environment_and_tag_must_match(self):
        for identity in [
            ("other", "dev", "tag"),
            ("commit", "qa", "tag"),
            ("commit", "dev", "other"),
        ]:
            with self.subTest(identity=identity), self.assertRaises(ValueError):
                artifact.verify(self.directory, *identity)

    def test_restore_preserves_versioned_isolated_function_directories(self):
        destination = self.root / "restored-jars"
        outputs = artifact.restore(
            self.directory,
            destination,
            self.root / "restored.pem",
            "commit",
            "dev",
            "tag",
        )
        for key in artifact.JAR_KEYS:
            path = Path(outputs[key.replace("-", "_") + "_source_path"])
            self.assertEqual(
                path, destination / key / "0.0.0-tag-SNAPSHOT-function.jar"
            )
            self.assertEqual(list(path.parent.iterdir()), [path])
            self.assertEqual(path.read_bytes(), key.encode())
        self.assertEqual(artifact.jar_filename("1.2.3-rc1"), "1.2.3-rc1-function.jar")

    def test_rejects_missing_modified_extra_and_symlink_files(self):
        for invalid in ["missing", "modified", "extra", "symlink", "manifest"]:
            with self.subTest(invalid=invalid):
                self.setUp()
                path = self.directory / "data-watcher.jar"
                if invalid == "missing":
                    path.unlink()
                elif invalid == "modified":
                    path.write_bytes(b"modified")
                elif invalid == "extra":
                    (self.directory / "auth.bazelrc").write_text("credential")
                elif invalid == "symlink":
                    path.unlink()
                    path.symlink_to(
                        self.jars / "data-watcher/0.0.0-tag-SNAPSHOT-function.jar"
                    )
                else:
                    manifest = self.directory / "manifest.json"
                    data = json.loads(manifest.read_text())
                    data["files"]["../auth.bazelrc"] = "bad"
                    manifest.write_text(json.dumps(data))
                with self.assertRaises(ValueError):
                    artifact.verify(self.directory, "commit", "dev", "tag")

    def test_failed_restore_emits_no_paths_or_certificates(self):
        (self.directory / "data-watcher.jar").write_bytes(b"modified")
        destination = self.root / "restored.pem"
        output = self.root / "outputs"
        with mock.patch(
            "sys.argv",
            [
                str(SCRIPT),
                "restore",
                "--directory",
                str(self.directory),
                "--commit",
                "commit",
                "--environment",
                "dev",
                "--image-tag",
                "tag",
                "--certificates",
                str(destination),
                "--uber-jars",
                str(self.root / "restored-jars"),
                "--output",
                str(output),
            ],
        ):
            self.assertEqual(artifact.main(), 1)
        self.assertFalse(destination.exists())
        self.assertFalse(output.exists())

    def test_pack_rejects_ambiguous_jar_selection(self):
        (self.jars / "data-watcher/second.jar").write_bytes(b"other")
        with self.assertRaises(ValueError):
            artifact.pack(
                self.root / "other-artifact",
                self.jars,
                self.descriptors,
                self.certs,
                "commit",
                "dev",
                "tag",
            )


if __name__ == "__main__":
    unittest.main()
