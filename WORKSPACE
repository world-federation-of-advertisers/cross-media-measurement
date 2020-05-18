workspace(name = "wfa_measurement_system")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# Support Maven sources

http_archive(
    name = "rules_jvm_external",
    sha256 = "62133c125bf4109dfd9d2af64830208356ce4ef8b165a6ef15bbff7460b35c3a",
    strip_prefix = "rules_jvm_external-3.0",
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/3.0.zip",
)

load("@rules_jvm_external//:defs.bzl", "maven_install")

# Kotlin
# https://github.com/bazelbuild/rules_kotlin

rules_kotlin_version = "legacy-1.3.0"

rules_kotlin_sha = "4fd769fb0db5d3c6240df8a9500515775101964eebdf85a3f9f0511130885fde"

http_archive(
    name = "io_bazel_rules_kotlin",
    sha256 = rules_kotlin_sha,
    strip_prefix = "rules_kotlin-%s" % rules_kotlin_version,
    type = "zip",
    urls = ["https://github.com/bazelbuild/rules_kotlin/archive/%s.zip" % rules_kotlin_version],
)

load("@io_bazel_rules_kotlin//kotlin:kotlin.bzl", "kotlin_repositories", "kt_register_toolchains")

KOTLIN_VERSION = "1.3.72"

KOTLINC_RELEASE_SHA = "ccd0db87981f1c0e3f209a1a4acb6778f14e63fe3e561a98948b5317e526cc6c"

KOTLINC_RELEASE = {
    "urls": [
        "https://github.com/JetBrains/kotlin/releases/download/v{v}/kotlin-compiler-{v}.zip".format(v = KOTLIN_VERSION),
    ],
    "sha256": KOTLINC_RELEASE_SHA,
}

kotlin_repositories(compiler_release = KOTLINC_RELEASE)

kt_register_toolchains()

# gRPC Java
# See https://github.com/grpc/grpc-java/blob/master/examples/WORKSPACE

http_archive(
    name = "io_grpc_grpc_java",
    sha256 = "ca30194aa4ff175f910bbf212911f1b35c17307833da0afcfba07c525f28fff7",
    strip_prefix = "grpc-java-1.28.0",
    url = "https://github.com/grpc/grpc-java/archive/v1.28.0.tar.gz",
)

load(
    "@io_grpc_grpc_java//:repositories.bzl",
    "IO_GRPC_GRPC_JAVA_ARTIFACTS",
    "IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS",
    "grpc_java_repositories",
)

# gRPC Kotlin

git_repository(
    name = "com_github_grpc_grpc_kotlin",
    # TODO: use a commit number instead of pulling from the head of master.
    # However, in the short term, we're iterating on this so it's handy.
    branch = "master",
    remote = "https://github.com/efoxepstein/grpc-kotlin",
)

load(
    "@com_github_grpc_grpc_kotlin//:repositories.bzl",
    "IO_GRPC_GRPC_KOTLIN_ARTIFACTS",
    "IO_GRPC_GRPC_KOTLIN_OVERRIDE_TARGETS",
    "grpc_kt_repositories",
)

# Maven
maven_install(
    artifacts = [
        "com.google.api.grpc:grpc-google-cloud-pubsub-v1:0.1.24",
        "com.google.api.grpc:proto-google-cloud-pubsub-v1:0.1.24",
        "com.google.cloud:google-cloud-core:1.93.4",
        "com.google.cloud:google-cloud-spanner:1.54.0",
        "com.google.guava:guava:29.0-jre",
        "com.google.truth.extensions:truth-liteproto-extension:1.0.1",
        "com.google.truth.extensions:truth-proto-extension:1.0.1",
        "com.google.truth:truth:1.0.1",
        "com.squareup:kotlinpoet:1.5.0",
        "io.grpc:grpc-kotlin-stub:0.1.1",
        "junit:junit:4.13",
        "org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.5",
        "org.jetbrains.kotlinx:kotlinx-coroutines-test:1.3.5",
    ] + IO_GRPC_GRPC_JAVA_ARTIFACTS + IO_GRPC_GRPC_KOTLIN_ARTIFACTS,
    generate_compat_repositories = True,
    override_targets = dict(
        IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS.items() +
        IO_GRPC_GRPC_KOTLIN_OVERRIDE_TARGETS.items(),
    ),
    repositories = [
        "https://repo.maven.apache.org/maven2/",
    ],
)

load("@maven//:compat.bzl", "compat_repositories")

compat_repositories()

# Run after compat_repositories to ensure the maven_install-selected
# dependencies are used.
grpc_kt_repositories()

grpc_java_repositories()

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

# Docker
# https://github.com/bazelbuild/rules_docker

# Apparently the Bazel rules for Docker require this.
http_archive(
    name = "bazel_skylib",
    sha256 = "e5d90f0ec952883d56747b7604e2a15ee36e288bb556c3d0ed33e818a4d971f2",
    strip_prefix = "bazel-skylib-1.0.2",
    urls = ["https://github.com/bazelbuild/bazel-skylib/archive/1.0.2.tar.gz"],
)

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")

# The Kotlin image is more recent than the latest versioned release.
git_repository(
    name = "io_bazel_rules_docker",
    commit = "62a1072965e98f74662a11ba89e11df77d7e4305",
    remote = "https://github.com/bazelbuild/rules_docker.git",
    shallow_since = "1585331411 -0700",
)

load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)

container_repositories()

load(
    "@io_bazel_rules_docker//kotlin:image.bzl",
    _kotlin_image_repos = "repositories",
)

_kotlin_image_repos()

# Public APIs for measurement system

git_repository(
    name = "wfa_measurement_proto",
    # For now, until protos stabilize, use the latest version from master.
    branch = "master",
    remote = "sso://team/ads-xmedia-open-measurement-team/wfa-measurement-proto",
)
