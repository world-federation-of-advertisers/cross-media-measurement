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

"""Repository defs for Truth library."""

_ARTIFACT_NAMES = [
    "com.google.truth.extensions:truth-java8-extension",
    "com.google.truth.extensions:truth-proto-extension",
    "com.google.truth:truth",
]

def com_google_truth_artifact_dict(version):
    return {name: version for name in _ARTIFACT_NAMES}
