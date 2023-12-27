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

"""
Add tink_cc deps.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def tink_cc():
    maybe(
        http_archive,
        name = "tink_cc",
        sha256 = "c2c252b09969576965fd4610d933682a71890d90f01a96c418fcbcf808edf513",
        strip_prefix = "tink-1.7.0/cc",
        url = "https://github.com/google/tink/archive/refs/tags/v1.7.0.tar.gz",
    )
