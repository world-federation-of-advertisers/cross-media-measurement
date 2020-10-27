# Copyright 2020 The Measurement System Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load("@io_bazel_rules_docker//docker/util:run.bzl", "container_run_and_commit_layer")
load(
    "//src/main/docker:constants.bzl",
    "APT_CLEANUP_COMMANDS",
    "APT_UPGRADE_COMMANDS",
)

def container_commit_install_apt_packages(name, image, packages, tags = [], **kwargs):
    """Commits a new layer with the APT packages installed."""
    if len(packages) == 0:
        fail("Must specify at least one package")

    container_run_and_commit_layer(
        name = name,
        image = image,
        commands = APT_UPGRADE_COMMANDS + [
            "apt-get install -y --no-install-recommends " + " ".join(packages),
        ] + APT_CLEANUP_COMMANDS,
        tags = tags + [
            "no-remote-exec",
            "requires-network",
        ],
        **kwargs
    )

def container_commit_upgrade_apt_packages(name, image, tags = [], **kwargs):
    """Commits a new layer with the APT packages upgraded."""
    container_run_and_commit_layer(
        name = name,
        image = image,
        commands = APT_UPGRADE_COMMANDS + APT_CLEANUP_COMMANDS,
        tags = tags + [
            "no-remote-exec",
            "requires-network",
        ],
        **kwargs
    )
