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
        sha256 = "dd5a1184f01c8ddc67eef5280fbeecbba090602f9f3ac262a0d4241f8ce0e869",
        version = "0.50.0",
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
        sha256 = "4fb33696b16dfcd2baeb3808f53b56b49e4c18884c0e0cbd9dbcb1700fe55934",
        version = "0.25.0",
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
        sha256 = "99fde5608b79ff12a2a466cdd213e1535c62f80a96035006433ae9ba5a4a4d21",
        version = "0.15.0",
    )

    wfa_repo_archive(
        name = "wfa_rules_cue",
        repo = "rules_cue",
        sha256 = "652379dec5174ed7fa8fe4223d0adf9a1d610ff0aa02e1bd1e74f79834b526a6",
        version = "0.2.0",
    )
