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
        sha256 = "cbc253d5a240b5587d8defbf2af08772d0eda997aa348c7ed870f413750abc35",
        version = "0.16.0",
    )

    wfa_repo_archive(
        name = "wfa_common_cpp",
        repo = "common-cpp",
        sha256 = "2c30e218a595483a9d0f2ca7117bc40cbc522cf513b2b8ee9db4570ffd35027f",
        version = "0.3.0",
    )

    wfa_repo_archive(
        name = "wfa_measurement_proto",
        repo = "cross-media-measurement-api",
        sha256 = "78122d642c9a4fdad3463ddb1d4902b9448e6ef733c9608fd9df33e942a454b6",
        # TODO: update to a specific version once consent-signaling-client is also updated
        commit = "d7746e5eec85b679fd64459e82779b40f1c7850e",
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
        version = "0.8.0",
        repo = "consent-signaling-client",
        sha256 = "b4d0eac461d1b7039519960bf1aab1f8e7a923a4a552dd77a13841ca67302e33",
    )
