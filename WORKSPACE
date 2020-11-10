workspace(name = "wfa_measurement_system")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# @bazel_skylib

http_archive(
    name = "bazel_skylib",
    sha256 = "1c531376ac7e5a180e0237938a2536de0c54d93f5c278634818e0efc952dd56c",
    urls = [
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.0.3/bazel-skylib-1.0.3.tar.gz",
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.0.3/bazel-skylib-1.0.3.tar.gz",
    ],
)

load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")

bazel_skylib_workspace()

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
load("//build/com_google_truth:repo.bzl", "com_google_truth_artifact_dict")

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

# kotlinx.coroutines
load("//build/kotlinx_coroutines:repo.bzl", "kotlinx_coroutines_artifact_dict")

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

MAVEN_ARTIFACTS = artifacts.list_to_dict(
    IO_GRPC_GRPC_JAVA_ARTIFACTS +
    IO_GRPC_GRPC_KOTLIN_ARTIFACTS,
)

MAVEN_ARTIFACTS.update(com_google_truth_artifact_dict(version = "1.0.1"))

MAVEN_ARTIFACTS.update(kotlinx_coroutines_artifact_dict(version = "1.3.6"))

# Add Maven artifacts or override versions (e.g. those pulled in by gRPC Kotlin
# or default dependency versions).
MAVEN_ARTIFACTS.update({
    "com.google.api.grpc:grpc-google-cloud-pubsub-v1": "0.1.24",
    "com.google.cloud:google-cloud-nio": "0.121.2",
    "com.google.cloud:google-cloud-spanner": "2.0.2",
    "com.google.code.gson:gson": "2.8.6",
    "com.nhaarman.mockitokotlin2:mockito-kotlin": "2.2.0",
    "info.picocli:picocli": "4.4.0",
    "junit:junit": "4.13",
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

http_archive(
    name = "rules_python",
    sha256 = "b6d46438523a3ec0f3cead544190ee13223a52f6a6765a29eae7b7cc24cc83a0",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.1.0/rules_python-0.1.0.tar.gz",
)

load("//build/io_bazel_rules_docker:repo.bzl", "rules_docker_repo")

rules_docker_repo(
    name = "io_bazel_rules_docker",
    commit = "cc45596d140b3b8651eb7b51b561f1bf72d1eea9",
    sha256 = "4975b23f8eff1f0763b5654fbdf325c0089631dacd4b37f9903c404401a93f05",
)

load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)

container_repositories()

load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")

container_deps()

load("@io_bazel_rules_docker//repositories:pip_repositories.bzl", "io_bazel_rules_docker_pip_deps")

io_bazel_rules_docker_pip_deps()

load("//build/io_bazel_rules_docker:base_images.bzl", "base_java_images")

# Defualt base images for java_image targets. Must come before
# java_image_repositories().
base_java_images(
    # gcr.io/distroless/java:11-debug
    debug_digest = "sha256:c3fe781de55d375de2675c3f23beb3e76f007e53fed9366ba931cc6d1df4b457",
    # gcr.io/distroless/java:11
    digest = "sha256:7fc091e8686df11f7bf0b7f67fd7da9862b2b9a3e49978d1184f0ff62cb673cc",
)

load(
    "@io_bazel_rules_docker//java:image.bzl",
    java_image_repositories = "repositories",
)

java_image_repositories()

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")

# docker.io/debian:bullseye-slim
container_pull(
    name = "debian_bullseye",
    digest = "sha256:d92a89b71e6adc50535710f53c78bb5bc84f80549e48342856bb2cc6c87e4f2a",
    registry = "docker.io",
    repository = "debian",
)

# docker.io/library/ubuntu:18.04
container_pull(
    name = "ubuntu_18_04",
    digest = "sha256:d1bf40f712c466317f5e06d38b3e7e4c98fef1229872bf6e2a8d1e01836c7ec4",
    registry = "docker.io",
    repository = "library/ubuntu",
)

# gcr.io/ads-open-measurement/bazel
container_pull(
    name = "bazel_image",
    digest = "sha256:a1437a53ce0f06028d7a4527dc403b8acf00bb5674e15e4f1b993e29b02a0d82",
    registry = "gcr.io",
    repository = "ads-open-measurement/bazel",
)

# See //src/main/docker/base:push_java_base
container_pull(
    name = "debian_java_base",
    digest = "sha256:e2d6a9216a49e4c909690a1f98d4670aa503d6a3fe5d6f1f9a171eadb45e23e5",
    registry = "gcr.io",
    repository = "ads-open-measurement/java-base",
)

# See https://docs.bazel.build/versions/3.7.0/install-ubuntu.html
http_file(
    name = "bazel_apt_key",
    downloaded_file_path = "bazel-release.pub.gpg",
    sha256 = "547ec71b61f94b07909969649d52ee069db9b0c55763d3add366ca7a30fb3f6d",
    urls = ["https://bazel.build/bazel-release.pub.gpg"],
)

# APT key for Google cloud.
# See https://cloud.google.com/sdk/docs/install
http_file(
    name = "gcloud_apt_key",
    downloaded_file_path = "cloud.google.gpg",
    urls = ["https://packages.cloud.google.com/apt/doc/apt-key.gpg"],
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

RBE_BASE_DIGEST = "sha256:9c844370b42dcdc01284f8e99c7b6872201c4bca0f536b1020b7a5791b678b4d"

container_pull(
    name = "rbe_ubuntu_18_04",
    digest = RBE_BASE_DIGEST,
    registry = "marketplace.gcr.io",
    repository = "google/rbe-ubuntu18-04",
)

load("@bazel_toolchains//rules:rbe_repo.bzl", "rbe_autoconfig")

# Configuration for RBE (Foundry).
# See //src/main/docker/rbe:rbe_image.
rbe_autoconfig(
    name = "rbe_default",
    base_container_digest = RBE_BASE_DIGEST,
    digest = "sha256:1fdad048b3a8fffb924852488401056ca6efb18467578fe85f7e2b8034fdb5aa",
    java_home = "/usr/lib/jvm/java-11-openjdk-amd64",
    registry = "gcr.io",
    repository = "ads-open-measurement/rbe",
    use_legacy_platform_definition = False,
)

# @cloud_spanner_emulator

load("//build/cloud_spanner_emulator:defs.bzl", "cloud_spanner_emulator_binaries")

cloud_spanner_emulator_binaries(
    name = "cloud_spanner_emulator",
    sha256 = "86df7eeb4c2c03c8f1254f222360637bb84d43f64ee5464f98a8057104791dad",
    version = "1.1.0",
)

# CUE binaries.

load("//build/cue:repo.bzl", "cue_binaries")

cue_binaries(
    name = "cue_binaries",
    sha256 = "810851e0e7d38192a6d0e09a6fa89ab5ff526ce29c9741f697995601edccb134",
    version = "0.2.2",
)

# ktlint
http_file(
    name = "ktlint",
    downloaded_file_path = "ktlint",
    executable = True,
    sha256 = "cf1c1a2efca79d07957a4de815af6e74287e46730d02393593edfa304e237153",
    urls = ["https://github.com/pinterest/ktlint/releases/download/0.39.0/ktlint"],
)

# buildifier
http_file(
    name = "buildifier",
    downloaded_file_path = "buildifier",
    executable = True,
    sha256 = "f9a9c082b8190b9260fce2986aeba02a25d41c00178855a1425e1ce6f1169843",
    urls = ["https://github.com/bazelbuild/buildtools/releases/download/3.5.0/buildifier"],
)

# Rules for swig wrapping.
git_repository(
    name = "wfa_rules_swig",
    commit = "c207f4e6517bc9491df17d407b561a3916739c1c",
    remote = "sso://team/ads-xmedia-open-measurement-team/rules_swig",
    shallow_since = "1603481983 -0700",
)

# Public APIs for measurement system.
git_repository(
    name = "wfa_measurement_proto",
    commit = "5eb82b5732f588a899e64153973bfcd38f2376ad",
    remote = "sso://team/ads-xmedia-open-measurement-team/wfa-measurement-proto",
    shallow_since = "1601429634 -0700",
)

# AnySketch.
git_repository(
    name = "any_sketch",
    commit = "523107ea635c4aabb39496d1bd776bd439dc65c9",
    remote = "sso://team/ads-xmedia-open-measurement-team/any-sketch",
    shallow_since = "1603139261 +0000",
)

git_repository(
    name = "any_sketch_java",
    commit = "54089b0f800fd6707d00c099a63d4963c40bb652",
    remote = "sso://team/ads-xmedia-open-measurement-team/any-sketch-java",
    shallow_since = "1603231855 -0700",
)
