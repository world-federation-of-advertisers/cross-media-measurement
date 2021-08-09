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
        sha256 = "d08bbabe8f78592fe58109ddad17dab1a4a2d0e910d602bf3fa3b3f00ff5ddf3",
        version = "0.6.0",
    )

    wfa_repo_archive(
        name = "wfa_common_cpp",
        repo = "common-cpp",
        sha256 = "3110be93990a449ac8f60b534319d7d3a08aa118908fecd7b571a5e08260e560",
        version = "0.2.1",
    )

    wfa_repo_archive(
        name = "wfa_measurement_proto",
        repo = "cross-media-measurement-api",
        sha256 = "d6f4844455793b25ba9af230fe09068c5567fc4957ebfe5ae610e2622832f49d",
        version = "0.2.0",
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
        commit = "a63d47ace86d025ec3330f341d1ba4b5573fe756",
        repo = "any-sketch-java",
        sha256 = "9dc3cea71dfeecad40ef67a6198846177d750d84401336d196d4d83059e8301e",
    )

    wfa_repo_archive(
        name = "wfa_consent_signaling_client",
        repo = "consent-signaling-client",
        sha256 = "23c570c4d7315feca30609d0ec0b4ec9e3aae484568e5d903bdbc22b363801dd",
        version = "0.2.0",
    )
