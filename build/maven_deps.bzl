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
Dependencies needed for maven.
"""

load("@rules_jvm_external//:defs.bzl", "artifact")
load("@wfa_common_jvm//build/maven:artifacts.bzl", "artifacts")
load(
    "@wfa_common_jvm//build:common_jvm_maven.bzl",
    "COMMON_JVM_EXCLUDED_ARTIFACTS",
    "COMMON_JVM_MAVEN_OVERRIDE_TARGETS",
    "common_jvm_maven_artifacts",
)

_DEPLOY_ENV = [
    "@com_github_grpc_grpc_kotlin//stub/src/main/java/io/grpc/kotlin:stub",
    "@com_github_jetbrains_kotlin//:kotlin-reflect",
    "@com_github_jetbrains_kotlin//:kotlin-stdlib-jdk7",
    "@io_grpc_grpc_java//netty",
    "@io_grpc_grpc_java//services:health",
]

_RUNTIME_DEPS = [
    artifact("org.jetbrains.kotlin:kotlin-reflect", "maven_export"),
    artifact("org.jetbrains.kotlin:kotlin-stdlib-jdk7", "maven_export"),
    artifact("io.grpc:grpc-kotlin-stub", "maven_export"),
    artifact("io.grpc:grpc-services", "maven_export"),
    artifact("io.grpc:grpc-netty", "maven_export"),
]

# TODO: this list can likely be minimized
_ARTIFACTS = {
    "com.google.crypto.tink:tink": "1.6.0",
    #"com.google.protobuf:protobuf-java": "3.20.1",
    #"com.google.protobuf:protobuf-java-util": "3.20.1",
    #"com.google.protobuf:protobuf-javalite": "3.20.1",
    #"com.google.protobuf:protobuf-kotlin": "3.20.1",
    "com.opentable.components:otj-pg-embedded": "1.0.0",
    "software.aws.rds:aws-postgresql-jdbc": "0.1.0",
    "org.projectnessie.cel:cel-core": "0.2.4",
    "org.projectnessie.cel:cel-tools": "0.2.4",
    "org.projectnessie.cel:cel-generated-pb": "0.2.4",
}

def cross_media_measurement_maven_artifacts():
    """Collects the Maven artifacts for cross-media-measurement.

    Returns:
        A dict of Maven artifact name to version.
    """
    common_jvm_artifacts = artifacts.list_to_dict(
        # TODO(@SanjayVas): Fix common_jvm_maven_artifacts to return a dict like
        # its documentation says.
        common_jvm_maven_artifacts(),
    )

    artifacts_dict = {}
    artifacts_dict.update(common_jvm_artifacts)
    artifacts_dict.update(_ARTIFACTS)
    return artifacts_dict

def cross_media_measurement_maven_override_targets():
    return COMMON_JVM_MAVEN_OVERRIDE_TARGETS

def cross_media_measurement_maven_excluded_artifacts():
    # TODO(@efoxepstein): why does org.slf4j:slf4j-log4j12 cause build failures?
    common_jvm_exclusions = [x for x in COMMON_JVM_EXCLUDED_ARTIFACTS if x != "org.slf4j:slf4j-log4j12"]
    return common_jvm_exclusions

def cross_media_measurement_maven_deploy_env():
    return _DEPLOY_ENV

def cross_media_measurement_maven_runtime_deps():
    return _RUNTIME_DEPS
