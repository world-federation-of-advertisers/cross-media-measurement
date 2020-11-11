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

"""Proto library macros."""

load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_java//java:defs.bzl", "java_proto_library")

def proto_and_java_proto_library(name, deps = []):
    proto_library(
        name = "%s_proto" % name,
        srcs = ["%s.proto" % name],
        deps = deps,
    )
    java_proto_library(
        name = "%s_java_proto" % name,
        deps = [":%s_proto" % name],
    )
