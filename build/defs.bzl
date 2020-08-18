# Copyright 2020 The Measurement System Authors
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

"""Utility functions definitions."""

def to_label(label_string):
    """Returns a Label object for a possibly relative label string."""
    if label_string.startswith("@") or label_string.startswith("//"):
        return Label(label_string)

    return Label("{repo}//{package}".format(
        repo = native.repository_name(),
        package = native.package_name(),
    )).relative(label_string)

def test_target(target):
    """Returns the label for the corresponding target in the test tree."""
    label = to_label(target)
    test_package = label.package.replace("src/main/", "src/test/", 1)
    return Label("@{workspace}//{package}:{target_name}".format(
        workspace = label.workspace_name,
        package = test_package,
        target_name = label.name,
    ))
