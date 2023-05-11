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
Step 2 of configuring WORKSPACE: adds transitive deps.
"""

load("@private_membership//build:private_membership_repositories.bzl", "private_membership_repositories")
load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")
load("//build/tink:repositories.bzl", "tink_cc")

def panel_exchange_client_deps():
    """Installs transitive deps for panel_exchange_client."""
    tink_cc()
    private_membership_repositories()
    rules_pkg_dependencies()
