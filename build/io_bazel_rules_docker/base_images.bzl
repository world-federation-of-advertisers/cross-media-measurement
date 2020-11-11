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

"""Build defs for base container images."""

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")

def base_java_images(digest, debug_digest):
    """Default base Java image targets.

    These must come before calling repositories() in
    @io_bazel_rules_docker//java:image.bzl. The target names are significant.

    See https://console.cloud.google.com/gcr/images/distroless/GLOBAL/java

    Args:
        digest: Digest of the standard base image.
        debug_digest: Digest of the debug base image.
    """

    container_pull(
        name = "java_image_base",
        digest = "sha256:7fc091e8686df11f7bf0b7f67fd7da9862b2b9a3e49978d1184f0ff62cb673cc",
        registry = "gcr.io",
        repository = "distroless/java",
    )

    container_pull(
        name = "java_debug_image_base",
        digest = "sha256:c3fe781de55d375de2675c3f23beb3e76f007e53fed9366ba931cc6d1df4b457",
        registry = "gcr.io",
        repository = "distroless/java",
    )
