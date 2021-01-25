# Copyright 2020 The Cross-Media Measurement Authors
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

"""Build defs for container images."""

load("@io_bazel_rules_docker//docker/util:run.bzl", "container_run_and_commit_layer")

_APT_GET_ENV = {
    "DEBIAN_FRONTEND": "noninteractive",
}

_APT_GET_OPTIONS = [
    "-y",
    "--no-install-recommends",
]

def _apt_get_command(command, args = []):
    env_assignments = [
        "=".join([name, value])
        for name, value in _APT_GET_ENV.items()
    ]
    parts = env_assignments + ["apt-get", command] + _APT_GET_OPTIONS + args
    return " ".join(parts)

_APT_CLEANUP_COMMANDS = [
    _apt_get_command("clean"),
    "rm -f /var/log/dpkg.log",
    "rm -f /var/log/alternatives.log",
]

def container_commit_install_apt_packages(
        name,
        image,
        packages,
        tags = [],
        upgrade = False,
        **kwargs):
    """Commits a new layer with the APT packages installed.

    Args:
        name: Target name.
        image: Image archive.
        packages: List of APT packages to install.
        tags: Standard attribute.
        upgrade: Whether to upgrade existing packages.
        **kwargs: Keyword arguments.
    """
    if len(packages) == 0:
        fail("Must specify at least one package")

    commands = [_apt_get_command("update")]
    if upgrade:
        commands.append(_apt_get_command("dist-upgrade"))
    commands.append(_apt_get_command("install", packages))
    commands.extend(_APT_CLEANUP_COMMANDS)

    container_run_and_commit_layer(
        name = name,
        image = image,
        commands = commands,
        tags = tags + [
            "no-remote-exec",
            "requires-network",
        ],
        **kwargs
    )

def container_commit_add_apt_key(
        name,
        image,
        keyring_path,
        key_path,
        tags = [],
        **kwargs):
    """Commits a new layer with the GPG key added to an APT keyring.

    Args:
        name: Target name.
        image: Image archive.
        keyring_path: Path of the keyring file within the image.
        key_path: Path of the key file within the image.
        tags: Standard attribute.
        **kwargs: Keyword arguments.
    """
    command = "apt-key --keyring {keyring} add {key}".format(
        keyring = keyring_path,
        key = key_path,
    )
    container_run_and_commit_layer(
        name = name,
        image = image,
        commands = [command],
        tags = tags + [
            "no-remote-exec",
            "requires-network",
        ],
        **kwargs
    )
