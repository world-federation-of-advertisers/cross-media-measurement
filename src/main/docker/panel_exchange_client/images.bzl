# Copyright 2022 The Cross-Media Measurement Authors
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

"""Container image specs."""

load("//build:variables.bzl", "IMAGE_REPOSITORY_SETTINGS")

_PREFIX = IMAGE_REPOSITORY_SETTINGS.repository_prefix

# List of specs for all Docker containers to push to a container registry.
# These are only used on GKE.
# TODO(@MarcoPremier): Merge this with images.bzl in cross-media-measurement repo
ALL_GKE_IMAGES = [
    struct(
        name = "google_cloud_example_daemon_image",
        image = "//src/main/kotlin/org/wfanet/panelmatch/client/deploy/example/gcloud:google_cloud_example_daemon_image",
        repository = _PREFIX + "/example-panel-exchange-daemon",
    ),
    struct(
        name = "aws_example_daemon_image",
        image = "//src/main/kotlin/org/wfanet/panelmatch/client/deploy/example/aws:aws_example_daemon_image",
        repository = "example-panel-exchange-daemon",
    ),
]
