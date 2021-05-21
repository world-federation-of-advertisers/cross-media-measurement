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

"""
This file contains local settings for building and deploying the codebase.
It is in .gitignore; please don't commit your local changes.
"""

# Settings for the repository where Docker images are stored.
# The image path is the container_registry, the prefix, and an
# image-specific suffix joined with slashes.
IMAGE_REPOSITORY_SETTINGS = struct(
    # URL of the container registry.
    container_registry = "",
    # Common prefix of all images.
    repository_prefix = "",
)

# Settings for deploying tests to Google Cloud.
TEST_GOOGLE_CLOUD_SETTINGS = struct(
    spanner_project = "",
    spanner_instance = "",
    cloud_storage_project = "",
    cloud_storage_bucket = "",
    bigquery_table = "",
)
