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

"""Maven artifact utils."""

def _list_to_dict(artifact_list):
    """Returns a dict of artifact name to version."""
    tuples = [tuple(item.rsplit(":", 1)) for item in artifact_list]
    return {name: version for (name, version) in tuples}

def _dict_to_list(artifact_dict):
    """Returns a list artifacts from a dict of name to version."""
    return [
        ":".join([name, version])
        for (name, version) in artifact_dict.items()
    ]

artifacts = struct(
    list_to_dict = _list_to_dict,
    dict_to_list = _dict_to_list,
)
