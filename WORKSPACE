workspace(name = "wfa_measurement_system")

load("//build:repositories.bzl", "wfa_measurement_system_repositories")

wfa_measurement_system_repositories()

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()

load("@private_membership//build:private_membership_repositories.bzl", "private_membership_repositories")

private_membership_repositories()

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

load("//build:deps.bzl", "panel_exchange_client_deps")

panel_exchange_client_deps()

load(
    "//build:panel_exchange_client_maven_deps.bzl",
    "panel_exchange_client_maven_artifacts",
    "panel_exchange_client_maven_excluded_artifacts",
)

# TODO(@MarcoPremier): Remove grpc_health_probe dependencies in favor of the 'healthServer' added in this commit: https://github.com/world-federation-of-advertisers/common-jvm/commit/2929e0aafdd82d4317c193ac2632729a4a1e3538#diff-6b1a2b97ef5b48abd2074dc2030c6fe833ced76a800ef9b051002da548370592
load("//build:repo.bzl", "grpc_health_probe")

grpc_health_probe()

load(
    "@wfa_common_jvm//build:versions.bzl",
    "GRPC_JAVA",
    "GRPC_KOTLIN",
    "KOTLIN_RELEASE_VERSION",
)

# Maven
load(
    "@wfa_common_jvm//build:common_jvm_maven.bzl",
    "COMMON_JVM_EXCLUDED_ARTIFACTS",
    "COMMON_JVM_MAVEN_OVERRIDE_TARGETS",
    "common_jvm_maven_artifacts_dict",
)
load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@wfa_common_jvm//build/maven:artifacts.bzl", "artifacts")

MAVEN_ARTIFACTS_DICT = dict(common_jvm_maven_artifacts_dict().items() + {
    "software.aws.rds:aws-postgresql-jdbc": "0.1.0",
    "org.projectnessie.cel:cel-core": "0.3.11",
    "io.opentelemetry:opentelemetry-api": "1.19.0",
    "io.opentelemetry:opentelemetry-sdk": "1.19.0",
    "io.opentelemetry:opentelemetry-exporter-otlp": "1.19.0",
    "io.opentelemetry:opentelemetry-semconv": "1.19.0-alpha",
    "io.kubernetes:client-java": "16.0.0",
    "io.kubernetes:client-java-extended": "16.0.0",
}.items())

maven_install(
    artifacts = artifacts.dict_to_list(MAVEN_ARTIFACTS_DICT),
    excluded_artifacts = COMMON_JVM_EXCLUDED_ARTIFACTS,
    fetch_sources = True,
    generate_compat_repositories = True,
    override_targets = COMMON_JVM_MAVEN_OVERRIDE_TARGETS,
    repositories = [
        "https://repo.maven.apache.org/maven2/",
    ],
)

maven_install(
    name = "maven_export",
    artifacts = [
        "io.grpc:grpc-kotlin-stub:" + GRPC_KOTLIN.version,
        "io.grpc:grpc-netty:" + GRPC_JAVA.version,
        "io.grpc:grpc-services:" + GRPC_JAVA.version,
        "org.jetbrains.kotlin:kotlin-reflect:" + KOTLIN_RELEASE_VERSION,
        "org.jetbrains.kotlin:kotlin-stdlib-jdk7:" + KOTLIN_RELEASE_VERSION,
        "org.jetbrains.kotlin:kotlin-test:" + KOTLIN_RELEASE_VERSION,
    ],
    excluded_artifacts = panel_exchange_client_maven_excluded_artifacts(),
    generate_compat_repositories = True,
    repositories = [
        "https://repo.maven.apache.org/maven2/",
    ],
)

load("@wfa_common_jvm//build:common_jvm_extra_deps.bzl", "common_jvm_extra_deps")

common_jvm_extra_deps()
