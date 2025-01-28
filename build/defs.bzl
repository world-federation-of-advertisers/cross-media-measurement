# Copyright 2025 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Common utility function definitions."""

def proto_lib_prefix(label_str):
    """Returns the prefix of the proto_library name.

    Args:
        label_str: the proto_library label as a string.
    Returns:
        The prefix of the name with the `_proto` suffix removed.
    """
    return Label(label_str).name.removesuffix("_proto")
