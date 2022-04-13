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

def wfa_measurement_system_repositories():
    """Imports all direct dependencies for wfa_measurement_system."""

    wfa_repo_archive(
        name = "wfa_common_jvm",
        repo = "common-jvm",
        sha256 = "ea877e30c868980cc67a0a72295fc34fb662dd7d200ad1b380688b6475ec47ed",
        version = "0.32.1",
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
        sha256 = "f182bf4ce98aae513cd60b4a157b6cacd133156768b1dec8b6b70de52a4fff27",
        version = "0.22.0",
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
