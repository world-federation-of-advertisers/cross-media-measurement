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

def tink_cc(tink_commit):

    _tink_sha256 = "0b8bbaffee4903faea66dbad76f8eb6d0eea3f94367807bebc49180f9f417031"
    _tink_url = "https://github.com/google/tink/archive/{commit}.tar.gz".format(
        commit = tink_commit,
    )

    maybe(
        http_archive,
        name = "tink_cc",
        sha256 = _tink_sha256,
        strip_prefix = "tink-{commit}/cc".format(commit = tink_commit),
        url = _tink_url,
    )
