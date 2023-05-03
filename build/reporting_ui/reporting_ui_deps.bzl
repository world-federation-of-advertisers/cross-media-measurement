# Copyright 2021 The Cross-Media Measurement Authors
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
Adds external repos necessary for Reporting UI.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

def reporting_ui_repositories():
    """Imports all direct dependencies for Reporting UI."""
    http_archive(
        name = "io_bazel_rules_go",
        sha256 = "6b65cb7917b4d1709f9410ffe00ecf3e160edf674b78c54a894471320862184f",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.39.0/rules_go-v0.39.0.zip",
            "https://github.com/bazelbuild/rules_go/releases/download/v0.39.0/rules_go-v0.39.0.zip",
        ],
    )

    http_archive(
        name = "bazel_gazelle",
        sha256 = "ecba0f04f96b4960a5b250c8e8eeec42281035970aa8852dda73098274d14a1d",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.29.0/bazel-gazelle-v0.29.0.tar.gz",
            "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.29.0/bazel-gazelle-v0.29.0.tar.gz",
        ],
    )

    http_file(
        name = "protoc_gen_grpc_gateway",
        sha256 = "d60028423c44b85c0bfbcf6393c35be7c53d439bc74b2e6f6caca863ad6df812",
        urls = ["https://github.com/grpc-ecosystem/grpc-gateway/releases/download/v2.15.2/protoc-gen-grpc-gateway-v2.15.2-linux-x86_64"],
        executable = True,
        downloaded_file_path = "protoc-gen-grpc-gateway",
    )

    http_archive(
        name = "com_github_grpc_ecosystem_grpc_gateway",
        sha256 = "0675f7f8300f659a23e7ea4b8be5b38726c173b506a4d25c4309e93b4f1616ae",
        strip_prefix = "grpc-gateway-2.15.2",
        urls = ["https://github.com/grpc-ecosystem/grpc-gateway/archive/refs/tags/v2.15.2.tar.gz"],
    )
