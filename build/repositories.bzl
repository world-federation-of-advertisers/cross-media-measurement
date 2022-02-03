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
load("//build/cue:repo.bzl", "cue_binaries")

def wfa_measurement_system_repositories():
    """Imports all direct dependencies for wfa_measurement_system."""

    wfa_repo_archive(
        name = "wfa_common_jvm",
        repo = "common-jvm",
        version = "0.27.0",
        sha256 = "116ef23bac19cdb3b6310c25ecc2718a843a9a3e6e2da981b3bd4b04ca1c6197",
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
        sha256 = "3c570fc82d8cbe4ef9901c7b2e69a1895cba386b7a15af54d922e79f946c12fa",
        version = "0.15.5",
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
        repo = "consent-signaling-client",
        version = "0.12.0",
        sha256 = "b907c0dd4f6efbe4f6db3f34efeca0f1763d3cc674c37cbfebac1ee2a80c86f5",
    )

    cue_binaries(
        name = "cue_binaries",
        sha256 = "d3f1df656101a498237d0a8b168a22253dde11f6b6b8cc577508b13a112142de",
        version = "0.4.1",
    )
