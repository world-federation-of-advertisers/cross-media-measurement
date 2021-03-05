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

"""Common sets of constraint values."""

DISTROLESS_JAVA = [
    "@platforms//os:linux",
    "@platforms//cpu:x86_64",
] + select({
    "//build/platforms:glibc_2_23": [],
    "//build/platforms:glibc_2_27": [],
    "//build/platforms:glibc_2_28": [],
    "//conditions:default": ["@platforms//:incompatible"],
})
