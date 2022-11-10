# Copyright 2020 The Cross-Media Measurement Authors
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

"""Build rules for generating Kubernetes yaml from CUE files."""

load("@wfa_rules_cue//cue:defs.bzl", "cue_export")

def cue_dump(name, srcs = None, deps = None, cue_tags = None, **kwargs):
    cue_export(
        name = name,
        srcs = srcs,
        deps = deps,
        filetype = "yaml",
        expression = "listObject",
        cue_tags = cue_tags,
        **kwargs
    )
