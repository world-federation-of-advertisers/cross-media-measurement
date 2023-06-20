workspace(name = "wfa_measurement_system")

load(
    "//build:versions.bzl",
    "APACHE_BEAM_VERSION",
    "AWS_SDK_VERSION",
    "K8S_CLIENT_VERSION",
    "OPEN_TELEMETRY_SDK_VERSION",
)



load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "aspect_rules_js",
    sha256 = "dcd1567d4a93a8634ec0b888b371a60b93c18d980f77dace02eb176531a71fcf",
    strip_prefix = "rules_js-1.26.0",
    url = "https://github.com/aspect-build/rules_js/releases/download/v1.26.0/rules_js-v1.26.0.tar.gz",
)

http_archive(
    name = "aspect_rules_ts",
    sha256 = "ace5b609603d9b5b875d56c9c07182357c4ee495030f40dcefb10d443ba8c208",
    strip_prefix = "rules_ts-1.4.0",
    url = "https://github.com/aspect-build/rules_ts/releases/download/v1.4.0/rules_ts-v1.4.0.tar.gz",
)

http_archive(
    name = "aspect_rules_jest",
    sha256 = "d3bb833f74b8ad054e6bff5e41606ff10a62880cc99e4d480f4bdfa70add1ba7",
    strip_prefix = "rules_jest-0.18.4",
    url = "https://github.com/aspect-build/rules_jest/releases/download/v0.18.4/rules_jest-v0.18.4.tar.gz",
)

load("@aspect_rules_js//js:repositories.bzl", "rules_js_dependencies")

rules_js_dependencies()

load("@aspect_rules_ts//ts:repositories.bzl", "rules_ts_dependencies")

rules_ts_dependencies(ts_version_from = "//:package.json")

load("@aspect_rules_jest//jest:dependencies.bzl", "rules_jest_dependencies")

rules_jest_dependencies()

load("@rules_nodejs//nodejs:repositories.bzl", "DEFAULT_NODE_VERSION", "nodejs_register_toolchains")

nodejs_register_toolchains(
    name = "nodejs",
    node_version = DEFAULT_NODE_VERSION,
)

load("@aspect_rules_js//npm:npm_import.bzl", "npm_translate_lock")

npm_translate_lock(
    name = "npm",
    npmrc = "//:.npmrc",
    pnpm_lock = "//:pnpm-lock.yaml",
    # public_hoist_packages = {
    #     "eslint-config-react-app": [""],
    #     "eslint": [""],
    # },
    verify_node_modules_ignored = "//:.bazelignore",
)

load("@npm//:repositories.bzl", "npm_repositories")

npm_repositories()





load("//build:repositories.bzl", "wfa_measurement_system_repositories")

wfa_measurement_system_repositories()

load(
    "@wfa_common_jvm//build:versions.bzl",
    "GRPC_JAVA",
    "GRPC_KOTLIN",
    "KOTLIN_RELEASE_VERSION",
    "TINK_COMMIT",
)
load("//build/tink:repositories.bzl", "tink_cc")

tink_cc(TINK_COMMIT)

load("@wfa_common_cpp//build:common_cpp_repositories.bzl", "common_cpp_repositories")

common_cpp_repositories()

load("@wfa_common_cpp//build:common_cpp_deps.bzl", "common_cpp_deps")

common_cpp_deps()

load("@wfa_rules_cue//cue:repositories.bzl", "rules_cue_dependencies")

rules_cue_dependencies()

# TODO(@renjiez): Update grpc version in common-jvm to address abseil version
# discrepancy with common-cpp.
load("@wfa_common_jvm//build:common_jvm_repositories.bzl", "common_jvm_repositories")

common_jvm_repositories()

load("@wfa_common_jvm//build:common_jvm_deps.bzl", "common_jvm_deps")

common_jvm_deps()

load("@private_membership//build:private_membership_repositories.bzl", "private_membership_repositories")

private_membership_repositories()

# TODO(@MarcoPremier): Remove grpc_health_probe dependencies in favor of the 'healthServer' added in this commit: https://github.com/world-federation-of-advertisers/common-jvm/commit/2929e0aafdd82d4317c193ac2632729a4a1e3538#diff-6b1a2b97ef5b48abd2074dc2030c6fe833ced76a800ef9b051002da548370592
load("//build/grpc_health_probe:repo.bzl", "grpc_health_probe")

grpc_health_probe()

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
    "io.opentelemetry:opentelemetry-api": OPEN_TELEMETRY_SDK_VERSION,
    "io.opentelemetry:opentelemetry-sdk": OPEN_TELEMETRY_SDK_VERSION,
    "io.opentelemetry:opentelemetry-exporter-otlp": OPEN_TELEMETRY_SDK_VERSION,
    "io.opentelemetry:opentelemetry-semconv": OPEN_TELEMETRY_SDK_VERSION + "-alpha",
    "io.kubernetes:client-java": K8S_CLIENT_VERSION,
    "io.kubernetes:client-java-extended": K8S_CLIENT_VERSION,
    "com.google.cloud:google-cloud-security-private-ca": "2.3.1",
    "org.apache.beam:beam-runners-direct-java": APACHE_BEAM_VERSION,
    "org.apache.beam:beam-runners-google-cloud-dataflow-java": APACHE_BEAM_VERSION,
    "org.apache.beam:beam-sdks-java-io-google-cloud-platform": APACHE_BEAM_VERSION,
    "org.slf4j:slf4j-simple": "1.7.32",
    "software.amazon.awssdk:sts": AWS_SDK_VERSION,
    "software.amazon.awssdk:auth": AWS_SDK_VERSION,
    "software.amazon.awssdk:acmpca": AWS_SDK_VERSION,
}.items())

EXCLUDED_MAVEN_ARTIFACTS = [x for x in COMMON_JVM_EXCLUDED_ARTIFACTS if x != "org.slf4j:slf4j-log4j12"] + ["org.apache.beam:beam-sdks-java-io-kafka"]

maven_install(
    artifacts = artifacts.dict_to_list(MAVEN_ARTIFACTS_DICT),
    excluded_artifacts = EXCLUDED_MAVEN_ARTIFACTS,
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
    excluded_artifacts = EXCLUDED_MAVEN_ARTIFACTS,
    generate_compat_repositories = True,
    repositories = [
        "https://repo.maven.apache.org/maven2/",
    ],
)

load("@bazel_gazelle//:deps.bzl", "go_repository")

go_repository(
    name = "org_golang_google_grpc_cmd_protoc_gen_go_grpc",
    importpath = "google.golang.org/grpc/cmd/protoc-gen-go-grpc",
    sum = "h1:TLkBREm4nIsEcexnCjgQd5GQWaHcqMzwQV0TX9pq8S0=",
    version = "v1.2.0",
)

load("@wfa_common_jvm//build:common_jvm_extra_deps.bzl", "common_jvm_extra_deps")

common_jvm_extra_deps()
