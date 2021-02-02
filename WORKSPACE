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

# @com_google_protobuf
http_archive(
    name = "com_google_protobuf",
    sha256 = "d0f5f605d0d656007ce6c8b5a82df3037e1d8fe8b121ed42e536f569dec16113",
    strip_prefix = "protobuf-3.14.0",
    urls = ["https://github.com/google/protobuf/archive/v3.14.0.tar.gz"],
)

http_archive(
    name = "googletest",
    sha256 = "94c634d499558a76fa649edb13721dce6e98fb1e7018dfaeba3cd7a083945e91",
    strip_prefix = "googletest-release-1.10.0",
    urls = ["https://github.com/google/googletest/archive/release-1.10.0.zip"],
)

# Abseil C++ libraries
git_repository(
    name = "com_google_absl",
    commit = "0f3bb466b868b523cf1dc9b2aaaed65c77b28862",
    remote = "https://github.com/abseil/abseil-cpp.git",
    shallow_since = "1603283562 -0400",
)

# @com_google_truth_truth
load("//build/com_google_truth:repo.bzl", "com_google_truth_artifact_dict")

# @io_bazel_rules_kotlin

load("//build/io_bazel_rules_kotlin:repo.bzl", "kotlinc_release", "rules_kotlin_repo")

rules_kotlin_repo(
    sha256 = "9cc0e4031bcb7e8508fd9569a81e7042bbf380604a0157f796d06d511cff2769",
    version = "legacy-1.4.0-rc4",
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
    sha256 = "08f06a797ec806d68e8811018cefd1d5a6b8bf1782b63937f2618a6be86a9e2d",
    strip_prefix = "grpc-kotlin-0.2.1",
    url = "https://github.com/grpc/grpc-kotlin/archive/v0.2.1.zip",
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
    "com.google.cloud:google-cloud-nio": "0.122.0",
    "com.google.cloud:google-cloud-spanner": "3.0.3",
    "com.google.code.gson:gson": "2.8.6",
    "com.google.guava:guava": "30.0-jre",
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

# @io_bazel_rules_docker

load("//build/io_bazel_rules_docker:repo.bzl", "rules_docker_repo")

rules_docker_repo(
    name = "io_bazel_rules_docker",
    commit = "7da0de3d094aae5601c45ae0855b64fb2771cd72",
    sha256 = "c15ef66698f5d2122a3e875c327d9ecd34a231a9dc4753b9500e70518464cc21",
)

load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)

container_repositories()

load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")

container_deps()

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
    digest = "sha256:8ab4e348f60ebd18b891593a531ded31cbbc3878f6e476116f3b49b15c199110",
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
    digest = "sha256:4d63678c47062af86b9149fd24985dfabf83db62dc9ff18b209337d5c321670b",
    registry = "gcr.io",
    repository = "ads-open-measurement/bazel",
)

# See //src/main/docker/base:push_java_base
container_pull(
    name = "debian_java_base",
    digest = "sha256:c6746729103a1a306a1ed572012562496512a691b3b23c3abacd64ad503cebc2",
    registry = "docker.io",
    repository = "wfameasurement/java-base",
)

# APT key for Google cloud.
# See https://cloud.google.com/sdk/docs/install
http_file(
    name = "gcloud_apt_key",
    downloaded_file_path = "cloud.google.gpg",
    urls = ["https://packages.cloud.google.com/apt/doc/apt-key.gpg"],
)

# @com_google_private_join_and_compute
git_repository(
    name = "com_google_private_join_and_compute",
    commit = "842f43b08cecba36f8e6c2d94d7467c3b7338397",
    remote = "https://github.com/google/private-join-and-compute.git",
    shallow_since = "1610640414 +0000",
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

# glog
# Needed for private-join-and-compute
http_archive(
    name = "com_github_glog_glog",
    sha256 = "f28359aeba12f30d73d9e4711ef356dc842886968112162bc73002645139c39c",
    strip_prefix = "glog-0.4.0",
    urls = ["https://github.com/google/glog/archive/v0.4.0.tar.gz"],
)

# gRPC
# Needed for private-join-and-compute
http_archive(
    name = "com_github_grpc_grpc",
    sha256 = "27dd2fc5c9809ddcde8eb6fa1fa278a3486566dfc28335fca13eb8df8bd3b958",
    strip_prefix = "grpc-1.35.0",
    urls = [
        "https://github.com/grpc/grpc/archive/v1.35.0.tar.gz",
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
    sha256 = "8e0633dfb59f704594f19ae996a35650747adc621ada5e8b9fb588f808c89cb0s",
    version = "3.7.0",
)

RBE_BASE_DIGEST = "sha256:687b98e38d2bd85499951cacacf714c06e4bd94f0f44836be9e6326e074450f5"

container_pull(
    name = "rbe_ubuntu_18_04",
    digest = RBE_BASE_DIGEST,
    registry = "marketplace.gcr.io",
    repository = "google/rbe-ubuntu18-04",
)

load("@bazel_toolchains//rules:rbe_repo.bzl", "rbe_autoconfig")
load("//src/main/docker:constants.bzl", "DEBIAN_JAVA_11")

# Configuration for RBE (Foundry).
# See //src/main/docker/rbe:rbe_image.
rbe_autoconfig(
    name = "rbe_default",
    base_container_digest = RBE_BASE_DIGEST,
    digest = "sha256:308c797bf7374748ac17e094be0060831dd4650aa4d89b812f4b178e6d799e8e",
    java_home = DEBIAN_JAVA_11.home,
    registry = "gcr.io",
    repository = "ads-open-measurement/rbe",
    use_legacy_platform_definition = False,
)

# @cloud_spanner_emulator

load("//build/cloud_spanner_emulator:defs.bzl", "cloud_spanner_emulator_binaries")

cloud_spanner_emulator_binaries(
    name = "cloud_spanner_emulator",
    sha256 = "7a3cdd5db7f5a427230ab67a8dc09cfcb6752dd7f0b28d51e8d08150b2641506",
    version = "1.1.1",
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

# bazel
http_file(
    name = "bazel",
    downloaded_file_path = "bazel",
    executable = True,
    sha256 = "b7583eec83cc38302997098a40b8c870c37e0ab971a83cb3364c754a178b74ec",
    urls = ["https://github.com/bazelbuild/bazel/releases/download/3.7.0/bazel-3.7.0-linux-x86_64"],
)

# Grpc Health Check Probe
http_file(
    name = "grpc_health_probe",
    downloaded_file_path = "grpc-health-probe",
    executable = True,
    sha256 = "c78e988a4aad5e9e599c6a69e681ac68579c000b8f0571593325ccbc0c1638b7",
    urls = [
        "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.6/grpc_health_probe-linux-amd64",
    ],
)

# Rules for swig wrapping.
local_repository(
    name = "wfa_rules_swig",
    path = "../rules-swig",
)

# Public APIs for measurement system.
local_repository(
    name = "wfa_measurement_proto",
    path = "../cross-media-measurement-api",
)

# AnySketch.
local_repository(
    name = "any_sketch",
    path = "../any-sketch",
)

local_repository(
    name = "any_sketch_java",
    path = "../any-sketch-java",
)

