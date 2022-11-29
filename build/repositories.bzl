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
        # DO_NOT_SUBMIT(world-federation-of-advertisers/common-jvm#172): Use version once released.
        commit = "a16e9d5dfda8661c635f4dfccf86c4fdc11e5c0c",
        name = "wfa_common_jvm",
        repo = "common-jvm",
        sha256 = "a114026676a41ef5a247e19516e1bdbe73d86714314c7ec6e04e3278f282f80b",
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
        sha256 = "8412e478f15119b624e6696b578ca308b55f61a240e83ea2f72444692118d1ff",
        version = "0.24.0",
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
        # DO_NOT_SUBMIT(world-federation-of-advertisers/consent-signaling-client#40): Use version once released.
        commit = "18a4ceffca545e84607fbaebdc022c608fdf3f02",
        name = "wfa_consent_signaling_client",
        repo = "consent-signaling-client",
        sha256 = "fc502ba3e6c7adee2f28591263059f283dfc0471091becc71eb911dc24de06e5",
    )

    wfa_repo_archive(
        name = "wfa_rules_cue",
        repo = "rules_cue",
        sha256 = "652379dec5174ed7fa8fe4223d0adf9a1d610ff0aa02e1bd1e74f79834b526a6",
        version = "0.2.0",
    )
