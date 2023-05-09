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

MEASUREMENT_SYSTEM_REPO = "https://github.com/world-federation-of-advertisers/cross-media-measurement"

def wfa_measurement_system_repositories():
    """Imports all direct dependencies for wfa_measurement_system."""

    wfa_repo_archive(
        name = "wfa_common_jvm",
        repo = "common-jvm",
        sha256 = "06ab7259708f490c052bd6ada0b9f193a99e9d621abd5428649d67378530a977",
        version = "0.55.0",
    )

    wfa_repo_archive(
        name = "wfa_common_cpp",
        repo = "common-cpp",
        sha256 = "fd4475b587741fa8af65c580b783054d09bf3890197830290a22b3823c778eeb",
        version = "0.10.0",
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
        sha256 = "dfa9eece9965b8c043e7562d0f7c6e06cd649c62d88d9be99c0295f9f5980d7b",
        version = "0.4.3",
    )

    wfa_repo_archive(
        name = "any_sketch_java",
        repo = "any-sketch-java",
        sha256 = "0d463c7eb9cce9e94a4af2575f8c9e1c79f1d5ebbe6fa8db168be167bd80cf5a",
        version = "0.4.1",
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
        sha256 = "0261b7797fa9083183536667958b1094fc732725fc48fca5cb68e6f731cdce2f",
        version = "0.3.0",
    )
