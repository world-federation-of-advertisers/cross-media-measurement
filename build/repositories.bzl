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

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//build/wfa:repositories.bzl", "wfa_repo_archive")

_PJC_COMMIT = "505ba981d66c9e5e73e18cfa647b4685f74784cb"

def wfa_measurement_system_repositories():
    """Imports all direct dependencies for wfa_measurement_system."""

    wfa_repo_archive(
        name = "wfa_common_jvm",
        # DO_NOT_SUBMIT(world-federation-of-advertisers/common-jvm#117): Use
        # version.
        commit = "3c2a99d830247bedc9720491f517d1c586d5a197",
        repo = "common-jvm",
        sha256 = "5d27fb66156976b91f78f143ee62eb8ec850a5642a9515bc110f4089d752d545",
    )

    wfa_repo_archive(
        name = "wfa_common_cpp",
        repo = "common-cpp",
        sha256 = "e8efc0c9f5950aff13a59f21f40ccc31c26fe40c800743f824f92df3a05588b2",
        version = "0.5.0",
    )

    wfa_repo_archive(
        name = "wfa_measurement_proto",
        repo = "cross-media-measurement-api",
        sha256 = "da28ccac88a12b3b75b974b92604b8e332b8bc91cd276afab1ee41415fa320a3",
        version = "0.22.2",
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
        sha256 = "3e3c90c3e2fab73a853c3b802171cbf04178eecfd0f7e5740a3b27c515110129",
        version = "0.2.0",
    )

    wfa_repo_archive(
        name = "any_sketch_java",
        repo = "any-sketch-java",
        sha256 = "1bff87bbb99cd567c04e634a1a7bf55ca7135d626d44b226f034b3ff325de38a",
        version = "0.3.0",
    )

    wfa_repo_archive(
        name = "wfa_consent_signaling_client",
        repo = "consent-signaling-client",
        sha256 = "b907c0dd4f6efbe4f6db3f34efeca0f1763d3cc674c37cbfebac1ee2a80c86f5",
        version = "0.12.0",
    )

    wfa_repo_archive(
        name = "wfa_rules_cue",
        repo = "rules_cue",
        sha256 = "62def6a4dc401fd1549e44e2a4e2ae73cf75e6870025329bc78a0150d9a2594a",
        version = "0.1.0",
    )

    http_archive(
        name = "com_google_private_join_and_compute",
        urls = [
            "https://github.com/google/private-join-and-compute/archive/{commit}.tar.gz".format(commit = _PJC_COMMIT),
        ],
        strip_prefix = "private-join-and-compute-{commit}".format(commit = _PJC_COMMIT),
        sha256 = "b1a83e1bc778fe902b782ae6d06fdf590a1f74684954c05592463ddad75f8ddb",
        repo_mapping = {
            # PJC depends on @com_google_protobuf//:protobuf_lite, which isn't
            # exposed by rules_proto's protobuf_workspace.
            "@com_google_protobuf": "@com_github_protocolbuffers_protobuf",
        },
    )
