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
        sha256 = "15479b3928e045fe68a0adbca7884c4d4aacb4d6486c3da70b32bd083ecafd66",
        version = "0.21.3",
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
        sha256 = "611bbc8c653868c1dbc973a520a192d8ac1678375167181354fc9b1bc8e3a3ea",
        version = "0.14.0",
    )

    wfa_repo_archive(
        name = "wfa_rules_swig",
        commit = "653d1bdcec85a9373df69920f35961150cf4b1b6",
        repo = "rules_swig",
        sha256 = "34c15134d7293fc38df6ed254b55ee912c7479c396178b7f6499b7e5351aeeec",
    )

    wfa_repo_archive(
        name = "any_sketch",
        version = "0.1.0",
        repo = "any-sketch",
        sha256 = "904a3dd0b48bccbbd0b84830c85e47aa56fe1257211514bfad99a88595ce6325",
    )

    wfa_repo_archive(
        name = "any_sketch_java",
        version = "0.2.0",
        repo = "any-sketch-java",
        sha256 = "55f20dfe98c71b4fdd5068f44ea5df5d88bac51c1d24061438a8aa5ed4b853b7",
    )

    wfa_repo_archive(
        name = "wfa_consent_signaling_client",
        version = "0.10.0",
        repo = "consent-signaling-client",
        sha256 = "59079953c01f2f9e2f22c0c2e2e01cfbb5da0a7b9348980e18827bf1947b21f5",
    )
