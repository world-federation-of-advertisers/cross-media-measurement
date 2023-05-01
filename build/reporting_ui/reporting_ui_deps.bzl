# Copyright 2021 The gRPC Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Adds external repos necessary for Reporting UI.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def reporting_ui_repositories():
    """Imports all direct dependencies for Reporting UI."""
    http_archive(
        name = "com_github_grpc_ecosystem_grpc_gateway",
        sha256 = "c0fad5e28e40526f69a4c245c6e0786b409ccb5131ea6659dd314cea7876a0ca",
        strip_prefix = "grpc-gateway-2.15.0",
        urls = ["https://github.com/grpc-ecosystem/grpc-gateway/archive/refs/tags/v2.15.0.tar.gz"],
    )

    http_archive(
        name = "io_bazel_rules_go",
        sha256 = "dd926a88a564a9246713a9c00b35315f54cbd46b31a26d5d8fb264c07045f05d",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.38.1/rules_go-v0.38.1.zip",
            "https://github.com/bazelbuild/rules_go/releases/download/v0.38.1/rules_go-v0.38.1.zip",
        ],
    )

    git_repository(
        name = "bazel_gazelle",
        commit = "f377e6eff8e24508feb1a34b1e5e681982482a9f",
        remote = "https://github.com/bazelbuild/bazel-gazelle",
        shallow_since = "1648046534 -0400",
    )

    http_archive(
        name = "com_github_bazelbuild_buildtools",
        sha256 = "ca524d4df8c91838b9e80543832cf54d945e8045f6a2b9db1a1d02eec20e8b8c",
        strip_prefix = "buildtools-6.0.1",
        urls = ["https://github.com/bazelbuild/buildtools/archive/6.0.1.tar.gz"],
    )

    http_archive(
        name = "build_bazel_rules_nodejs",
        sha256 = "94070eff79305be05b7699207fbac5d2608054dd53e6109f7d00d923919ff45a",
        urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/5.8.2/rules_nodejs-5.8.2.tar.gz"],
    )
