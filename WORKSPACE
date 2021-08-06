workspace(name = "wfa_measurement_system")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("//build/wfa:repositories.bzl", "wfa_repo_archive")

wfa_repo_archive(
    name = "wfa_common_jvm",
    repo = "common-jvm",
    sha256 = "d08bbabe8f78592fe58109ddad17dab1a4a2d0e910d602bf3fa3b3f00ff5ddf3",
    version = "0.6.0",
)

load("@wfa_common_jvm//build:common_jvm_repositories.bzl", "common_jvm_deps_repositories")

common_jvm_deps_repositories()

load("@wfa_common_jvm//build:common_jvm_deps.bzl", "common_jvm_deps")

common_jvm_deps()

wfa_repo_archive(
    name = "wfa_common_cpp",
    repo = "common-cpp",
    sha256 = "aac2fa570a63c974094e09a5c92585a4e992b429c658057d187f46381be3ce50",
    version = "0.1.0",
)

load("@wfa_common_cpp//build:common_cpp_deps.bzl", "common_cpp_deps")

common_cpp_deps()

load("@wfa_common_cpp//build:common_cpp_repositories.bzl", "common_cpp_repositories")

common_cpp_repositories()

wfa_repo_archive(
    name = "wfa_measurement_proto",
    repo = "cross-media-measurement-api",
    sha256 = "d6f4844455793b25ba9af230fe09068c5567fc4957ebfe5ae610e2622832f49d",
    version = "0.2.0",
)

wfa_repo_archive(
    name = "wfa_rules_swig",
    commit = "653d1bdcec85a9373df69920f35961150cf4b1b6",
    repo = "rules_swig",
    sha256 = "34c15134d7293fc38df6ed254b55ee912c7479c396178b7f6499b7e5351aeeec",
)

wfa_repo_archive(
    name = "any_sketch",
    commit = "995fe42006a56f926e568c0b02adae5f834a813d",
    repo = "any-sketch",
    sha256 = "2477a9cb52a6a415b0d498f7ba19010965145af4a449029df2e64d2379d3cc01",
)

wfa_repo_archive(
    name = "any_sketch_java",
    commit = "a63d47ace86d025ec3330f341d1ba4b5573fe756",
    repo = "any-sketch-java",
    sha256 = "9dc3cea71dfeecad40ef67a6198846177d750d84401336d196d4d83059e8301e",
)

# Maven
load("@wfa_common_jvm//build:common_jvm_maven.bzl", "COMMON_JVM_MAVEN_TARGETS", "common_jvm_maven_artifacts")
load("@rules_jvm_external//:defs.bzl", "maven_install")

maven_install(
    artifacts = common_jvm_maven_artifacts(),
    fetch_sources = True,
    generate_compat_repositories = True,
    override_targets = COMMON_JVM_MAVEN_TARGETS,
    repositories = [
        "https://repo.maven.apache.org/maven2/",
    ],
)

load("@wfa_common_jvm//build:common_jvm_extra_deps.bzl", "common_jvm_extra_deps")

common_jvm_extra_deps()
