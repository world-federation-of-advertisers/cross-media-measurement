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

"""Repository macros for bazel-toolchains library.

See https://github.com/bazelbuild/bazel-toolchains
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

_URL_TEMPLATES = [
    "https://github.com/bazelbuild/bazel-toolchains/releases/download/{version}/bazel-toolchains-{version}.tar.gz",
    "https://mirror.bazel.build/github.com/bazelbuild/bazel-toolchains/releases/download/{version}/bazel-toolchains-{version}.tar.gz",
]

def bazel_toolchains(name, version, sha256):
    prefix = "bazel-toolchains-" + version
    urls = [template.format(version = version) for template in _URL_TEMPLATES]

    http_archive(
        name = name,
        sha256 = sha256,
        strip_prefix = prefix,
        urls = urls,
    )
