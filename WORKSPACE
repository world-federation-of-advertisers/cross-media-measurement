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

load("//build/open_telemetry_java_agent:repo.bzl", "open_telemetry_java_agent")

open_telemetry_java_agent()

# Maven
load("@wfa_common_jvm//build:common_jvm_maven.bzl", "COMMON_JVM_MAVEN_OVERRIDE_TARGETS", "common_jvm_maven_artifacts")
load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@wfa_common_jvm//build/maven:artifacts.bzl", "artifacts")

ADDITIONAL_MAVEN_ARTIFACTS = artifacts.dict_to_list({
    "software.aws.rds:aws-postgresql-jdbc": "0.1.0",
    "org.projectnessie.cel:cel-core": "0.2.4",
    "org.projectnessie.cel:cel-tools": "0.2.4",
    "org.projectnessie.cel:cel-generated-pb": "0.2.4",
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
