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

load("@com_github_grpc_grpc_kotlin//:kt_jvm_grpc.bzl", "kt_jvm_grpc_library")
load("@io_grpc_grpc_java//:java_grpc_library.bzl", "java_grpc_library")
load("@rules_java//java:defs.bzl", "java_proto_library")

def java_and_kt_grpc_library(name, deps):
    java_proto_library(
        name = name + "_java_proto",
        deps = deps,
    )
    java_grpc_library(
        name = name + "_java_grpc",
        srcs = deps,
        deps = [":%s_java_proto" % name],
    )
    kt_jvm_grpc_library(
        name = name + "_kt_jvm_grpc_internal",
        srcs = deps,
        deps = [":%s_java_proto" % name],
    )

    # Bundle all the dependencies together for convenience.
    native.java_library(
        name = name + "_kt_jvm_grpc",
        exports = [
            "//imports/kotlin/kotlinx/coroutines:core",
            ":%s_java_proto" % name,
            ":%s_java_grpc" % name,
            ":%s_kt_jvm_grpc_internal" % name,
        ],
    )
