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
    # rules_kotlin_version = "1.7.1"
    # rules_kotlin_sha = "fd92a98bd8a8f0e1cdcb490b93f5acef1f1727ed992571232d33de42395ca9b3"
    # http_archive(
    #     name = "io_bazel_rules_kotlin",
    #     urls = ["https://github.com/bazelbuild/rules_kotlin/releases/download/v%s/rules_kotlin_release.tgz" % rules_kotlin_version],
    #     sha256 = rules_kotlin_sha,
    # )

    # http_archive(
    #     name = "io_grpc_grpc_java",
    #     sha256 = "fd0a649d03a8da06746814f414fb4d36c1b2f34af2aad4e19ae43f7c4bd6f15e",
    #     strip_prefix = "grpc-java-1.53.0",
    #     url = "https://github.com/grpc/grpc-java/archive/refs/tags/v1.53.0.tar.gz",
    # )

    http_archive(
        name = "com_github_grpc_ecosystem_grpc_gateway",
        sha256 = "c0fad5e28e40526f69a4c245c6e0786b409ccb5131ea6659dd314cea7876a0ca",
        strip_prefix = "grpc-gateway-2.15.0",
        urls = ["https://github.com/grpc-ecosystem/grpc-gateway/archive/refs/tags/v2.15.0.tar.gz"],
    )

    # # Define before rules_proto, otherwise we receive the version of com_google_protobuf from there
    # http_archive(
    #     name = "com_google_protobuf",
    #     sha256 = "930c2c3b5ecc6c9c12615cf5ad93f1cd6e12d0aba862b572e076259970ac3a53",
    #     strip_prefix = "protobuf-3.21.12",
    #     urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.21.12.tar.gz"],
    # )

    # http_archive(
    #     name = "bazel_skylib",
    #     sha256 = "b8a1527901774180afc798aeb28c4634bdccf19c4d98e7bdd1ce79d1fe9aaad7",
    #     urls = [
    #         "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.4.1/bazel-skylib-1.4.1.tar.gz",
    #         "https://github.com/bazelbuild/bazel-skylib/releases/download/1.4.1/bazel-skylib-1.4.1.tar.gz",
    #     ],
    # )

    # http_archive(
    #     name = "rules_proto",
    #     sha256 = "66bfdf8782796239d3875d37e7de19b1d94301e8972b3cbd2446b332429b4df1",
    #     strip_prefix = "rules_proto-4.0.0",
    #     urls = [
    #         "https://mirror.bazel.build/github.com/bazelbuild/rules_proto/archive/refs/tags/4.0.0.tar.gz",
    #         "https://github.com/bazelbuild/rules_proto/archive/refs/tags/4.0.0.tar.gz",
    #     ],
    # )

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
