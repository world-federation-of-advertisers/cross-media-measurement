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
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//build/com_google_riegeli:repo.bzl", "com_google_riegeli_repo")

MEASUREMENT_SYSTEM_REPO = "https://github.com/world-federation-of-advertisers/cross-media-measurement"

def wfa_measurement_system_repositories():
    """Imports all direct dependencies for wfa_measurement_system."""

    com_google_riegeli_repo()

    wfa_repo_archive(
        name = "wfa_common_jvm",
        repo = "common-jvm",
        sha256 = "06ab7259708f490c052bd6ada0b9f193a99e9d621abd5428649d67378530a977",
        version = "0.55.0",
    )

    wfa_repo_archive(
        name = "wfa_common_cpp",
        repo = "common-cpp",
        sha256 = "be7564a574c60176dc63e48daba6135263779deb301baacb77d2328bdaf38d3d",
        version = "0.9.0",
    )

    wfa_repo_archive(
        name = "wfa_measurement_proto",
        repo = "cross-media-measurement-api",
        sha256 = "4ae6664c57496632dd75d9731b82aaaa7a6e3134ab81b2d70def3f6a07028683",
        version = "0.30.0",
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
        sha256 = "fa7ff63e181692b9060c50fedd519b7ea7e9a6683bf4d5ba9d19213b6cb6dac4",
        version = "0.4.2",
    )

    wfa_repo_archive(
        name = "any_sketch_java",
        repo = "any-sketch-java",
        sha256 = "117642633c1b0a6a539f75b21d396146fcb7c51ae60f8c63859b0e9cce490e77",
        version = "0.4.0",
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
        sha256 = "99fde5608b79ff12a2a466cdd213e1535c62f80a96035006433ae9ba5a4a4d21",
        version = "0.15.0",
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
        name = "rules_pkg",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.6.0/rules_pkg-0.6.0.tar.gz",
            "https://github.com/bazelbuild/rules_pkg/releases/download/0.6.0/rules_pkg-0.6.0.tar.gz",
        ],
        sha256 = "62eeb544ff1ef41d786e329e1536c1d541bb9bcad27ae984d57f18f314018e66",
    )
