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

"""
Adds external repos necessary for virtual_people_core_serving.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def virtual_people_core_serving_repositories():
    """Imports all direct dependencies for virtual_people_core_serving."""
    http_archive(
        name = "wfa_common_cpp",
        sha256 = "60e9c808d55d14be65347cab008b8bd4f8e2dd8186141609995333bc75fc08ce",
        strip_prefix = "common-cpp-0.8.0",
        url = "https://github.com/world-federation-of-advertisers/common-cpp/archive/refs/tags/v0.8.0.tar.gz",
    )

    http_archive(
        name = "wfa_common_jvm",
        sha256 = "2b1a363e9ed5057437f3bcd90356f645f21aaa451a53ff487f688d0f668fa13b",
        strip_prefix = "common-jvm-0.58.0",
        url = "https://github.com/world-federation-of-advertisers/common-jvm/archive/refs/tags/v0.58.0.tar.gz",
    )

    http_archive(
        name = "wfa_virtual_people_common",
        sha256 = "9cd92c85a86c86c7228e860969bc31148049ece3c6ce9f1ce2047e907c3187ff",
        strip_prefix = "virtual-people-common-0.2.4",
        url = "https://github.com/world-federation-of-advertisers/virtual-people-common/archive/refs/tags/v0.2.4.tar.gz",
    )

    http_archive(
        name = "wfa_measurement_proto",
        sha256 = "cc327047bc094768c46a45b6e7a1cde3d0dfc3a89585f316932f0abbf78d2612",
        strip_prefix = "cross-media-measurement-api-0.39.0",
        url = "https://github.com/world-federation-of-advertisers/cross-media-measurement-api/archive/refs/tags/v0.39.0.tar.gz",
    )
