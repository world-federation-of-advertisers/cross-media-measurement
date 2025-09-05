# Copyright 2023 The Cross-Media Measurement Authors
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

"""Macros for container images."""

load(
    "@wfa_common_jvm//build/rules_oci:defs.bzl",
    _java_image = "java_image",
)
load("//build:variables.bzl", "MEASUREMENT_SYSTEM_REPO")

def java_image(
        name,
        binary,
        # buildifier: disable=unused-variable
        main_class = None,
        args = None,
        base = None,
        tags = None,
        visibility = None,
        labels = None,
        **kwargs):
    """Java container image.

    This is a replacement for the java_image rule which sets common attrs.

    Args:
      name: Name of the target.
      binary: The java_binary target to containerize.
      main_class: (Unused) Entry point for the Java program.
      args: Optional list of arguments passed as cmd_args.
      base: Base image to use.
      tags: Additional tags to apply.
      visibility: Target visibility.
      labels: Extra OCI labels to set (in addition to common defaults).
      **kwargs: Additional arguments forwarded to the underlying rule.
    """
    tags = tags or []

    labels = labels or {}
    labels.update({
        "org.opencontainers.image.source": MEASUREMENT_SYSTEM_REPO,
        "tee.launch_policy.allow_cmd_override": "true",
    })

    _java_image(
        name = name,
        binary = binary,
        base = base,
        labels = labels,
        cmd_args = args,
        tags = tags + ["no-remote-cache"],
        visibility = visibility,
        **kwargs
    )
