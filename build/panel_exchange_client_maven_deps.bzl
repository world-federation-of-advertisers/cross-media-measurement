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
Defs for exporting panel-exchange-client Maven artifacts.
"""

load("@rules_jvm_external//:defs.bzl", "artifact")

_DEPLOY_ENV = [
    "@com_github_grpc_grpc_kotlin//stub/src/main/java/io/grpc/kotlin:stub",
    "@com_github_jetbrains_kotlin//:kotlin-reflect",
    "@com_github_jetbrains_kotlin//:kotlin-stdlib-jdk7",
    "@io_grpc_grpc_java//netty",
    "@io_grpc_grpc_java//services:health",
]

_TEST_DEPLOY_ENV = [
    "@com_github_jetbrains_kotlin//:kotlin-test",
]

_RUNTIME_DEPS = [
    artifact("org.jetbrains.kotlin:kotlin-reflect", "maven_export"),
    artifact("org.jetbrains.kotlin:kotlin-stdlib-jdk7", "maven_export"),
    artifact("io.grpc:grpc-kotlin-stub", "maven_export"),
    artifact("io.grpc:grpc-services", "maven_export"),
    artifact("io.grpc:grpc-netty", "maven_export"),
]

_TEST_RUNTIME_DEPS = [
    artifact("org.jetbrains.kotlin:kotlin-test", "maven_export"),
]

def panel_exchange_client_maven_deploy_env():
    return _DEPLOY_ENV

def panel_exchange_client_maven_runtime_deps():
    return _RUNTIME_DEPS

def panel_exchange_client_maven_test_deploy_env():
    return _TEST_DEPLOY_ENV

def panel_exchange_client_maven_test_runtime_deps():
    return _TEST_RUNTIME_DEPS
