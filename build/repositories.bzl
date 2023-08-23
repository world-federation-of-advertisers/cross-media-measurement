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
Adds external repos necessary for wfa_measurement_system.
"""

load("//build/wfa:repositories.bzl", "wfa_repo_archive")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

MEASUREMENT_SYSTEM_REPO = "https://github.com/world-federation-of-advertisers/cross-media-measurement"

def wfa_measurement_system_repositories():
    """Imports all direct dependencies for wfa_measurement_system."""

    wfa_repo_archive(
        name = "wfa_common_jvm",
        repo = "common-jvm",
        sha256 = "328f4707c79ff1b309784463309f4c5918c6a2fab8c0187efda014f8b3bc79eb",
        version = "0.63.0",
    )

    wfa_repo_archive(
        name = "wfa_common_cpp",
        repo = "common-cpp",
        sha256 = "fd4475b587741fa8af65c580b783054d09bf3890197830290a22b3823c778eeb",
        version = "0.10.0",
    )

    wfa_repo_archive(
        name = "wfa_measurement_proto",
        # DO_NOT_SUBMIT(world-federation-of-advertisers/cross-media-measurement-api#165): Use version.
        commit = "0c5c23a3cfd0dfdadce854d842fc53584a486b7a",
        repo = "cross-media-measurement-api",
        sha256 = "ff8ef13159101fb78c8bb13174eca9ee3b8355c2b0867a82d592e58f443483bb",
    )

    wfa_repo_archive(
        name = "wfa_rules_swig",
        commit = "653d1bdcec85a9373df69920f35961150cf4b1b6",
        repo = "rules_swig",
        sha256 = "34c15134d7293fc38df6ed254b55ee912c7479c396178b7f6499b7e5351aeeec",
    )

    wfa_repo_archive(
        name = "any_sketch",
        repo = "any-sketch",
        sha256 = "dfa9eece9965b8c043e7562d0f7c6e06cd649c62d88d9be99c0295f9f5980d7b",
        version = "0.4.3",
    )

    wfa_repo_archive(
        name = "any_sketch_java",
        repo = "any-sketch-java",
        sha256 = "0d463c7eb9cce9e94a4af2575f8c9e1c79f1d5ebbe6fa8db168be167bd80cf5a",
        version = "0.4.1",
    )

    wfa_repo_archive(
        name = "wfa_rules_cue",
        repo = "rules_cue",
        sha256 = "0261b7797fa9083183536667958b1094fc732725fc48fca5cb68e6f731cdce2f",
        version = "0.3.0",
    )

    wfa_repo_archive(
        name = "wfa_consent_signaling_client",
        repo = "consent-signaling-client",
        sha256 = "dbe586f8fb1da9ea88ab87a1acac036aa6142fe24005ae0577bba6dd60e185c8",
        version = "0.16.0",
    )

    wfa_repo_archive(
        name = "wfa_virtual_people_common",
        repo = "virtual-people-common",
        sha256 = "0a663e5517f50052ecc5e5745564935a3c15ebce2e9550b11dda451e341ea624",
        version = "0.2.3",
    )

    http_archive(
        name = "private_membership",
        sha256 = "b1e0e7f74f4da09a6011c6fa91d7b968cdff6bb571712490dae427704b2af14c",
        strip_prefix = "private-membership-84e45669f7357bffcdafbc1b0cc26e72512808ce",
        url = "https://github.com/google/private-membership/archive/84e45669f7357bffcdafbc1b0cc26e72512808ce.zip",
    )

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
        name = "grpc_ecosystem_grpc_gateway",
        sha256 = "0675f7f8300f659a23e7ea4b8be5b38726c173b506a4d25c4309e93b4f1616ae",
        strip_prefix = "grpc-gateway-2.15.2",
        urls = ["https://github.com/grpc-ecosystem/grpc-gateway/archive/refs/tags/v2.15.2.tar.gz"],
    )

    http_archive(
        name = "aspect_rules_js",
        sha256 = "dcd1567d4a93a8634ec0b888b371a60b93c18d980f77dace02eb176531a71fcf",
        strip_prefix = "rules_js-1.26.0",
        url = "https://github.com/aspect-build/rules_js/releases/download/v1.26.0/rules_js-v1.26.0.tar.gz",
    )

    http_archive(
        name = "aspect_rules_ts",
        sha256 = "ace5b609603d9b5b875d56c9c07182357c4ee495030f40dcefb10d443ba8c208",
        strip_prefix = "rules_ts-1.4.0",
        url = "https://github.com/aspect-build/rules_ts/releases/download/v1.4.0/rules_ts-v1.4.0.tar.gz",
    )

    http_archive(
        name = "aspect_rules_jest",
        sha256 = "d3bb833f74b8ad054e6bff5e41606ff10a62880cc99e4d480f4bdfa70add1ba7",
        strip_prefix = "rules_jest-0.18.4",
        url = "https://github.com/aspect-build/rules_jest/releases/download/v0.18.4/rules_jest-v0.18.4.tar.gz",
    )

    http_archive(
        name = "aspect_rules_webpack",
        sha256 = "78d05d9e87ee804accca80a4fec98a66f146b6058e915eae3d97190397ad12df",
        strip_prefix = "rules_webpack-0.12.0",
        url = "https://github.com/aspect-build/rules_webpack/releases/download/v0.12.0/rules_webpack-v0.12.0.tar.gz",
    )
