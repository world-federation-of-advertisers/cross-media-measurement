workspace(name = "wfa_measurement_system")

load("//build:repositories.bzl", "wfa_measurement_system_repositories")

wfa_measurement_system_repositories()

load("@wfa_rules_cue//cue:repositories.bzl", "rules_cue_dependencies")

rules_cue_dependencies()

load("@wfa_common_jvm//build:common_jvm_repositories.bzl", "common_jvm_repositories")

common_jvm_repositories()

load("@wfa_common_jvm//build:common_jvm_deps.bzl", "common_jvm_deps")

common_jvm_deps()

load("@wfa_common_cpp//build:common_cpp_repositories.bzl", "common_cpp_repositories")

common_cpp_repositories()

load("@wfa_common_cpp//build:common_cpp_deps.bzl", "common_cpp_deps")

common_cpp_deps()

# Maven
load(
    "//build:maven_deps.bzl",
    "cross_media_measurement_maven_artifacts",
    "cross_media_measurement_maven_excluded_artifacts",
    "cross_media_measurement_maven_override_targets",
)
load("@wfa_common_jvm//build/maven:artifacts.bzl", "artifacts")
load("@rules_jvm_external//:defs.bzl", "maven_install")

maven_install(
    artifacts = artifacts.dict_to_list(cross_media_measurement_maven_artifacts()),
    excluded_artifacts = cross_media_measurement_maven_excluded_artifacts(),
    fetch_sources = True,
    generate_compat_repositories = True,
    override_targets = cross_media_measurement_maven_override_targets(),
    repositories = [
        "https://repo.maven.apache.org/maven2/",
    ],
)

GRPC_JAVA_VERSION = "1.43.2"

KOTLIN_VERSION = "1.4.31"

maven_install(
    name = "maven_export",
    artifacts = [
        "io.grpc:grpc-kotlin-stub:1.2.0",
        "io.grpc:grpc-netty:" + GRPC_JAVA_VERSION,
        "io.grpc:grpc-services:" + GRPC_JAVA_VERSION,
        "org.jetbrains.kotlin:kotlin-reflect:" + KOTLIN_VERSION,
        "org.jetbrains.kotlin:kotlin-stdlib-jdk7:" + KOTLIN_VERSION,
        "org.jetbrains.kotlin:kotlin-test:" + KOTLIN_VERSION,
    ],
    excluded_artifacts = cross_media_measurement_maven_excluded_artifacts(),
    generate_compat_repositories = True,
    repositories = [
        "https://repo.maven.apache.org/maven2/",
    ],
)

load("@wfa_common_jvm//build:common_jvm_extra_deps.bzl", "common_jvm_extra_deps")

common_jvm_extra_deps()
