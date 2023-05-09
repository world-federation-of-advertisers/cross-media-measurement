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

load("@io_bazel_rules_docker//java:image.bzl", "DEFAULT_JAVA_BASE", "jar_app_layer")
load("//build:repositories.bzl", "MEASUREMENT_SYSTEM_REPO")

def java_image(
        name,
        binary,
        main_class = None,
        args = None,
        base = None,
        visibility = None,
        **kwargs):
    """Java container image.

    This is a replacement for the java_image rule from rules_docker which sets
    common attrs.
    """
    jar_app_layer(
        name = name,
        base = base or DEFAULT_JAVA_BASE,
        binary = binary,
        labels = {"org.opencontainers.image.source": MEASUREMENT_SYSTEM_REPO},
        main_class = main_class,
        visibility = visibility,
        args = args,
        **kwargs
    )
