workspace(name = "wfa_measurement_system")

load("//build:repositories.bzl", "wfa_measurement_system_repositories")

wfa_measurement_system_repositories()

load("@wfa_common_cpp//build:common_cpp_repositories.bzl", "common_cpp_repositories")

wfa_repo_archive(
    name = "any_sketch_java",
    commit = "a63d47ace86d025ec3330f341d1ba4b5573fe756",
    repo = "any-sketch-java",
    sha256 = "9dc3cea71dfeecad40ef67a6198846177d750d84401336d196d4d83059e8301e",
)


wfa_repo_archive(
    name = "wfa_common_jvm",
    repo = "common-jvm",
    sha256 = "a1fa7136cf95ed7d00fe98ab33a862d20b35dc468cff041158fe7850315ec405",
    version = "0.3.0",
)

# @com_google_truth_truth
load("@wfa_common_jvm//build/com_google_truth:repo.bzl", "com_google_truth_artifact_dict")

# @io_bazel_rules_kotlin

load("@wfa_common_jvm//build/io_bazel_rules_kotlin:repo.bzl", "rules_kotlin_repo")

load("@wfa_common_cpp//build:common_cpp_deps.bzl", "common_cpp_deps")

common_cpp_deps()

load("@wfa_common_jvm//build:common_jvm_repositories.bzl", "common_jvm_repositories")

common_jvm_repositories()

load("@wfa_common_jvm//build:common_jvm_deps.bzl", "common_jvm_deps")

common_jvm_deps()

# Maven
load("@wfa_common_jvm//build:common_jvm_maven.bzl", "COMMON_JVM_MAVEN_OVERRIDE_TARGETS", "common_jvm_maven_artifacts")
load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@wfa_common_jvm//build/maven:artifacts.bzl", "artifacts")

ADDITIONAL_MAVEN_ARTIFACTS = artifacts.dict_to_list({
    "com.google.crypto.tink:tink": "1.6.0",
})

maven_install(
    artifacts = common_jvm_maven_artifacts() + ADDITIONAL_MAVEN_ARTIFACTS,
    fetch_sources = True,
    generate_compat_repositories = True,
    override_targets = COMMON_JVM_MAVEN_OVERRIDE_TARGETS,
    repositories = [
        "https://repo.maven.apache.org/maven2/",
    ],
)

load("@wfa_common_jvm//build:common_jvm_extra_deps.bzl", "common_jvm_extra_deps")

common_jvm_extra_deps()
