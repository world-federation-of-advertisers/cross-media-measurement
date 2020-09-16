workspace(name = "wfa_measurement_system")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

http_archive(
    name = "googletest",
    sha256 = "94c634d499558a76fa649edb13721dce6e98fb1e7018dfaeba3cd7a083945e91",
    strip_prefix = "googletest-release-1.10.0",
    urls = ["https://github.com/google/googletest/archive/release-1.10.0.zip"],
)

http_archive(
    name = "absl",
    sha256 = "f342aac71a62861ac784cadb8127d5a42c6c61ab1cd07f00aef05f2cc4988c42",
    strip_prefix = "abseil-cpp-20200225.2",
    urls = ["https://github.com/abseil/abseil-cpp/archive/20200225.2.zip"],
)

# @com_google_truth_truth

load("//build/com_google_truth_truth:repo.bzl", "COM_GOOGLE_TRUTH_TRUTH_ARTIFACTS")

# @io_bazel_rules_kotlin

load("//build/io_bazel_rules_kotlin:repo.bzl", "kotlinc_release", "rules_kotin_repo")

rules_kotin_repo(
    sha256 = "da0e6e1543fcc79e93d4d93c3333378f3bd5d29e82c1bc2518de0dbe048e6598",
    version = "legacy-1.4.0-rc3",
)

load("//build/io_bazel_rules_kotlin:deps.bzl", "rules_kotlin_deps")

rules_kotlin_deps(compiler_release = kotlinc_release(
    sha256 = "ccd0db87981f1c0e3f209a1a4acb6778f14e63fe3e561a98948b5317e526cc6c",
    version = "1.3.72",
))

# @com_github_grpc_grpc_kotlin

http_archive(
    name = "com_github_grpc_grpc_kotlin",
    sha256 = "bfc60770a48aaec1489b4cb7dbf0ff712bed7ed7d2479281d94f56f565832048",
    strip_prefix = "grpc-kotlin-0.1.4",
    url = "https://github.com/grpc/grpc-kotlin/archive/v0.1.4.zip",
)

load(
    "@com_github_grpc_grpc_kotlin//:repositories.bzl",
    "IO_GRPC_GRPC_KOTLIN_ARTIFACTS",
    "IO_GRPC_GRPC_KOTLIN_OVERRIDE_TARGETS",
    "grpc_kt_repositories",
    "io_grpc_grpc_java",
)

io_grpc_grpc_java()

load(
    "@io_grpc_grpc_java//:repositories.bzl",
    "IO_GRPC_GRPC_JAVA_ARTIFACTS",
    "IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS",
    "grpc_java_repositories",
)

# Maven

http_archive(
    name = "rules_jvm_external",
    sha256 = "82262ff4223c5fda6fb7ff8bd63db8131b51b413d26eb49e3131037e79e324af",
    strip_prefix = "rules_jvm_external-3.2",
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/3.2.zip",
)

load("@rules_jvm_external//:defs.bzl", "maven_install")
load("//build/maven:artifacts.bzl", "artifacts")

KOTLINX_COROUTINES_VERSION = "1.3.6"

MAVEN_ARTIFACTS = artifacts.list_to_dict(
    IO_GRPC_GRPC_JAVA_ARTIFACTS +
    IO_GRPC_GRPC_KOTLIN_ARTIFACTS +
    COM_GOOGLE_TRUTH_TRUTH_ARTIFACTS,
)

MAVEN_ARTIFACTS.update({
    "com.google.api.grpc:grpc-google-cloud-pubsub-v1": "0.1.24",
    "com.google.api.grpc:proto-google-cloud-pubsub-v1": "0.1.24",
    "com.google.api:gax": "1.58.2",
    "com.google.cloud:google-cloud-core": "1.93.5",
    "com.google.cloud:google-cloud-nio": "0.121.0",
    "com.google.cloud:google-cloud-spanner": "1.55.1",
    "com.google.cloud:google-cloud-storage": "1.109.0",
    "com.nhaarman.mockitokotlin2:mockito-kotlin": "2.2.0",
    "info.picocli:picocli": "4.4.0",
    "io.grpc:grpc-kotlin-stub": "0.1.2",
    "junit:junit": "4.13",
    "org.jetbrains.kotlinx:kotlinx-coroutines-core": KOTLINX_COROUTINES_VERSION,
    "org.jetbrains.kotlinx:kotlinx-coroutines-debug": KOTLINX_COROUTINES_VERSION,
    "org.jetbrains.kotlinx:kotlinx-coroutines-test": KOTLINX_COROUTINES_VERSION,
    "org.mockito:mockito-core": "3.3.3",
})

maven_install(
    artifacts = artifacts.dict_to_list(MAVEN_ARTIFACTS),
    fetch_sources = True,
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

grpc_java_repositories()  # For gRPC Kotlin.

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")  # From gRPC.

protobuf_deps()

# @io_bazel_rules_docker

load("//build/io_bazel_rules_docker:repo.bzl", "rules_docker_repo")

rules_docker_repo(
    name = "io_bazel_rules_docker",
    sha256 = "4521794f0fba2e20f3bf15846ab5e01d5332e587e9ce81629c7f96c793bb7036",
    version = "0.14.4",
)

load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)

container_repositories()

load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")

container_deps()

load("@io_bazel_rules_docker//repositories:pip_repositories.bzl", "pip_deps")

pip_deps()

load(
    "@io_bazel_rules_docker//java:image.bzl",
    java_image_repositories = "repositories",
)

java_image_repositories()

load(
    "@io_bazel_rules_docker//kotlin:image.bzl",
    kotlin_image_repositories = "repositories",
)

kotlin_image_repositories()

load(
    "@io_bazel_rules_docker//container:container.bzl",
    "container_pull",
)

# docker.io/debian:bullseye-slim
container_pull(
    name = "debian_bullseye",
    digest = "sha256:d92a89b71e6adc50535710f53c78bb5bc84f80549e48342856bb2cc6c87e4f2a",
    registry = "docker.io",
    repository = "debian",
)

# See //src/main/docker/base:push_java_base
container_pull(
    name = "debian_java_base",
    digest = "sha256:e2d6a9216a49e4c909690a1f98d4670aa503d6a3fe5d6f1f9a171eadb45e23e5",
    registry = "gcr.io",
    repository = "ads-open-measurement/java-base",
)

# @com_google_private_join_and_compute

load("//build/com_google_private_join_and_compute:repo.bzl", "private_join_and_compute_repo")

private_join_and_compute_repo(
    commit = "b040c117663747c7d0f3fae082a613ca8bf60943",
    sha256 = "9fc5ff2134ba87332596199289c7752e062567fe67802b73495297a851b9c240",
)

# glog
# Needed for private-join-and-compute
http_archive(
    name = "com_github_glog_glog",
    sha256 = "f28359aeba12f30d73d9e4711ef356dc842886968112162bc73002645139c39c",
    strip_prefix = "glog-0.4.0",
    urls = ["https://github.com/google/glog/archive/v0.4.0.tar.gz"],
)

# gflags
# Needed for glog
http_archive(
    name = "com_github_gflags_gflags",
    sha256 = "34af2f15cf7367513b352bdcd2493ab14ce43692d2dcd9dfc499492966c64dcf",
    strip_prefix = "gflags-2.2.2",
    urls = [
        "https://mirror.bazel.build/github.com/gflags/gflags/archive/v2.2.2.tar.gz",
        "https://github.com/gflags/gflags/archive/v2.2.2.tar.gz",
    ],
)

# gRPC
# Needed for private-join-and-compute
http_archive(
    name = "com_github_grpc_grpc",
    sha256 = "4cbce7f708917b6e58b631c24c59fe720acc8fef5f959df9a58cdf9558d0a79b",
    strip_prefix = "grpc-1.28.1",
    urls = [
        "https://github.com/grpc/grpc/archive/v1.28.1.tar.gz",
    ],
)

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

# Includes boringssl, com_google_absl, and other dependencies.
# Needed for private-join-and-compute
grpc_deps()

load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

# Loads transitive dependencies of GRPC.
# Needed for private-join-and-compute
grpc_extra_deps()

# @bazel_toolchains
# For RBE (Foundry).

load("//build/bazel_toolchains:repo.bzl", "bazel_toolchains")

bazel_toolchains(
    name = "bazel_toolchains",
    sha256 = "89a053218639b1c5e3589a859bb310e0a402dedbe4ee369560e66026ae5ef1f2",
    version = "3.5.0",
)

RBE_BASE_DIGEST = "sha256:7e09344798abaf239a283b508b17f16827bef619579d38f97e9d028f700f5226"

container_pull(
    name = "rbe_ubuntu_18_04",
    digest = RBE_BASE_DIGEST,
    registry = "marketplace.gcr.io",
    repository = "google/rbe-ubuntu18-04",
)

load("@bazel_toolchains//rules:rbe_repo.bzl", "rbe_autoconfig")

# Configuration for RBE (Foundry).
# See //src/main/docker/rbe:push_rbe for container.
rbe_autoconfig(
    name = "rbe_default",
    base_container_digest = RBE_BASE_DIGEST,
    digest = "sha256:ce08021cc0a64bcfd9c2abb3261c1304459a8bacbcac9489bb1544d6bca731b8",
    java_home = "/usr/lib/jvm/java-11-openjdk-amd64",
    registry = "gcr.io",
    repository = "ads-open-measurement/rbe",
    use_legacy_platform_definition = False,
)

# @cloud_spanner_emulator

load("//build/cloud_spanner_emulator:defs.bzl", "cloud_spanner_emulator_binaries")

cloud_spanner_emulator_binaries(
    name = "cloud_spanner_emulator",
    sha256 = "19eb279c0f0a93b14796e347e6b26a27bc90b91c5578f1de1532448a37b3e3d2",
    version = "0.8.0",
)

# CUE

git_repository(
    name = "com_github_tnarg_rules_cue",
    commit = "540ca8c02f438f7ef3e53d64d4e4e859d578cc15",
    remote = "https://github.com/tnarg/rules_cue",
    shallow_since = "1590098645 -0700",
)

load("@com_github_tnarg_rules_cue//cue:deps.bzl", "cue_register_toolchains")
load("@com_github_tnarg_rules_cue//:go.bzl", "go_modules")

go_modules()

cue_register_toolchains()

# Rules for swig wrapping.
git_repository(
    name = "wfa_rules_swig",
    commit = "4799cbfa2d0e335208d790729ed4b49d34968245",
    remote = "sso://team/ads-xmedia-open-measurement-team/rules_swig",
    shallow_since = "1595012448 -0700",
)

# Public APIs for measurement system.
git_repository(
    name = "wfa_measurement_proto",
    commit = "9dfde0372d05ade1489d52362e4914c44e1c063b",
    remote = "sso://team/ads-xmedia-open-measurement-team/wfa-measurement-proto",
    shallow_since = "1597770206 +0000",
)

# AnySketch.
git_repository(
    name = "any_sketch",
    commit = "60a6034ea85dead3b43543633436880ac74bf19e",
    remote = "sso://team/ads-xmedia-open-measurement-team/any-sketch",
    shallow_since = "1598644541 -0400",
)
