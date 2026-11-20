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

load("@rules_multirun//:defs.bzl", "command", "multirun")
load(
    "@wfa_common_jvm//build/rules_oci:defs.bzl",
    _container_push = "container_push",
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

def parallel_push_all(name, images, tag, **kwargs):
    """Push each destination once in sequential batches of at most four.

    Args:
      name: Target name.
      images: Mapping from registry/repository to OCI image label.
      tag: Tag to publish at each repository.
      **kwargs: Common attributes forwarded to generated rules.
    """
    pushes = []
    for index, repository in enumerate(sorted(images.keys())):
        push_name = "{}_push_{}".format(name, index)
        command_name = push_name + "_command"
        registry, path = repository.split("/", 1)
        _container_push(
            name = push_name,
            image = images[repository],
            registry = registry,
            repository = path,
            tag = tag,
            **kwargs
        )
        command(name = command_name, command = ":" + push_name, **kwargs)
        pushes.append(":" + command_name)

    batches = []
    for start in range(0, len(pushes), 4):
        batch_name = "{}_batch_{}".format(name, start // 4)

        # rules_multirun 0.12 only treats jobs=0 as parallel. Its concurrent
        # patched runner joins all started children even if a later launch fails.
        multirun(name = batch_name, commands = pushes[start:start + 4], jobs = 0, **kwargs)
        command(name = batch_name + "_command", command = ":" + batch_name, **kwargs)
        batches.append(":" + batch_name + "_command")
    multirun(name = name, commands = batches, jobs = 1, keep_going = False, **kwargs)
