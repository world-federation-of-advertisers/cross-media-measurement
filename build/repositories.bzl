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
        sha256 = "f60316de53fe9b5ea3cd9bb0f9c8666091a61fb5684a0577602ecdcdf4a5d2be",
        version = "0.41.2",
    )

    wfa_repo_archive(
        name = "wfa_common_cpp",
        repo = "common-cpp",
        sha256 = "60e9c808d55d14be65347cab008b8bd4f8e2dd8186141609995333bc75fc08ce",
        version = "0.8.0",
    )

    wfa_repo_archive(
        name = "wfa_measurement_proto",
        repo = "cross-media-measurement-api",
        sha256 = "69ee69cbfa11ba90ca172d3141a9465a4408883e1aa559d56ef740bd01d474ff",
        version = "0.23.0",
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
        sha256 = "a30369e28ae3788356b734239559f3d0c035d9121963ab00a797615364d4f0c4",
        version = "0.3.0",
    )

    wfa_repo_archive(
        name = "any_sketch_java",
        repo = "any-sketch-java",
        sha256 = "117642633c1b0a6a539f75b21d396146fcb7c51ae60f8c63859b0e9cce490e77",
        version = "0.4.0",
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
