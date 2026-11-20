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

"""Package and verify immutable CMMS deployment inputs without credentials or plans."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys

JAR_KEYS = (
    "data-watcher",
    "requisition-fetcher",
    "event-group-sync",
    "data-availability-sync",
    "data-availability-cleanup",
    "data-availability-monitor",
    "vid-labeling-dispatcher",
    "vid-labeling-monitor",
    "vid-labeling-healing-controller",
    "dashboard-compliance-check",
)
INPUT_FILES = {key + ".jar" for key in JAR_KEYS} | {
    "event-descriptors.pb",
    "all_root_certs.pem",
}


def jar_filename(image_tag):
    # Match get-maven-artifact-version so existing Terraform replacement triggers
    # retain the versioned source path across both rollout stages.
    version = (
        image_tag
        if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9]+)?", image_tag)
        else "0.0.0-" + image_tag + "-SNAPSHOT"
    )
    name = version + "-function.jar"
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name):
        raise ValueError("Invalid artifact version")
    return name


def checksum(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def pack(
    directory, uber_jars, descriptors, certificates, commit, environment, image_tag
):
    directory.mkdir(parents=True, exist_ok=False)
    sources = {"event-descriptors.pb": descriptors, "all_root_certs.pem": certificates}
    for key in JAR_KEYS:
        matches = list((uber_jars / key).glob("*.jar"))
        if len(matches) != 1 or matches[0].name != jar_filename(image_tag):
            raise ValueError("Expected exactly one Uber JAR for " + key)
        sources[key + ".jar"] = matches[0]
    for name, source in sources.items():
        if source.is_symlink() or not source.is_file():
            raise ValueError("Input must be a regular file: " + str(source))
        shutil.copyfile(source, directory / name)
    manifest = {
        "version": 1,
        "commit": commit,
        "environment": environment,
        "image_tag": image_tag,
        "jar_filename": jar_filename(image_tag),
        "files": {name: checksum(directory / name) for name in sorted(INPUT_FILES)},
    }
    (directory / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True) + "\n"
    )


def verify(directory, commit, environment, image_tag):
    # Reject extra files and symlinks before reading any artifact content.
    entries = list(directory.iterdir())
    if {entry.name for entry in entries} != INPUT_FILES | {"manifest.json"}:
        raise ValueError("Prepared inputs have missing or unexpected files")
    if any(entry.is_symlink() or not entry.is_file() for entry in entries):
        raise ValueError("Prepared inputs must contain only regular files")
    manifest = json.loads((directory / "manifest.json").read_text())
    if (
        manifest["version"] != 1
        or manifest["commit"] != commit
        or manifest["environment"] != environment
        or manifest["image_tag"] != image_tag
        or manifest["jar_filename"] != jar_filename(image_tag)
    ):
        raise ValueError(
            "Prepared inputs do not match this commit, environment, and image tag"
        )
    if set(manifest["files"]) != INPUT_FILES:
        raise ValueError("Prepared input manifest has an unexpected file set")
    for name in INPUT_FILES:
        if checksum(directory / name) != manifest["files"][name]:
            raise ValueError("Prepared input checksum mismatch: " + name)
    return {
        **{
            key.replace("-", "_")
            + "_source_path": str((directory / (key + ".jar")).resolve())
            for key in JAR_KEYS
        },
        "event_proto_descriptors_path": str(
            (directory / "event-descriptors.pb").resolve()
        ),
    }


def restore(directory, uber_jars, certificates, commit, environment, image_tag):
    outputs = verify(directory, commit, environment, image_tag)
    for key in JAR_KEYS:
        # gcloud deploys dirname(uber_jar_path); never put multiple functions'
        # JARs in the same source directory.
        destination = uber_jars / key
        destination.mkdir(parents=True, exist_ok=False)
        path = destination / jar_filename(image_tag)
        shutil.copyfile(directory / (key + ".jar"), path)
        outputs[key.replace("-", "_") + "_source_path"] = str(path.resolve())
    shutil.copyfile(directory / "all_root_certs.pem", certificates)
    return outputs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["pack", "restore"])
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--image-tag", required=True)
    parser.add_argument("--uber-jars", type=Path, required=True)
    parser.add_argument("--descriptors", type=Path)
    parser.add_argument("--certificates", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        if args.operation == "pack":
            if args.descriptors is None:
                parser.error("pack requires --uber-jars and --descriptors")
            pack(
                args.directory,
                args.uber_jars,
                args.descriptors,
                args.certificates,
                args.commit,
                args.environment,
                args.image_tag,
            )
        else:
            if args.output is None:
                parser.error("restore requires --output")
            outputs = restore(
                args.directory,
                args.uber_jars,
                args.certificates,
                args.commit,
                args.environment,
                args.image_tag,
            )
            with args.output.open("a") as output:
                for key, path in outputs.items():
                    output.write(f"{key}={path}\n")
    except (OSError, ValueError, KeyError, TypeError) as error:
        print("::error::" + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
