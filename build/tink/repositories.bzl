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

"""Repository targets for Tink (https://github.com/google/tink)."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")
load("@wfa_common_jvm//build:versions.bzl", "TINK_COMMIT")

_TINK_SHA256 = "0b8bbaffee4903faea66dbad76f8eb6d0eea3f94367807bebc49180f9f417031"
_TINK_URL = "https://github.com/google/tink/archive/{commit}.tar.gz".format(
    commit = TINK_COMMIT,
)

def tink_base():
    """Repository target for tink_base."""
    maybe(
        http_archive,
        name = "tink_base",
        sha256 = _TINK_SHA256,
        strip_prefix = "tink-{commit}".format(commit = TINK_COMMIT),
        url = _TINK_URL,
    )

def tink_cc():
    """Repository target for tink_cc."""
    tink_base()

    maybe(
        http_archive,
        name = "tink_cc",
        sha256 = _TINK_SHA256,
        strip_prefix = "tink-{commit}/cc".format(commit = TINK_COMMIT),
        url = _TINK_URL,
        repo_mapping = {
            # TODO(bazelbuild/rules_proto#121): Remove this once
            # protobuf_workspace is fixed.
            "@com_google_protobuf": "@com_github_protocolbuffers_protobuf",
        },
    )
