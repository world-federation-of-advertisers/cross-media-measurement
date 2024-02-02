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

"""Proto library macros."""

load("@rules_proto//proto:defs.bzl", "proto_library")
load("@wfa_rules_kotlin_jvm//kotlin:defs.bzl", "kt_jvm_proto_library")

IMPORT_PREFIX = "/src/main/proto"

def proto_and_kt_jvm_proto_library(name, deps = []):
    proto_library(
        name = "%s_proto" % name,
        srcs = ["%s.proto" % name],
        strip_import_prefix = IMPORT_PREFIX,
        deps = deps,
        visibility = ["//visibility:private"],
    )
    kt_jvm_proto_library(
        name = "%s_kt_jvm_proto" % name,
        deps = [":%s_proto" % name],
    )
