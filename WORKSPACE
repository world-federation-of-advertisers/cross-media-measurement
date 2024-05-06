workspace(name = "wfa_measurement_system")

load(":deps.bzl", "buildfarm_dependencies")

buildfarm_dependencies()

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# Virtual-people-core-serving
git_repository(
    name = "virtual_people_core_serving",
    commit = "124eaffbe4f771f515010e957497aabb4027efc1",
    remote = "https://github.com/world-federation-of-advertisers/virtual-people-core-serving",
)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "io_bazel_rules_kotlin",
    sha256 = "34e8c0351764b71d78f76c8746e98063979ce08dcf1a91666f3f3bc2949a533d",
    url = "https://github.com/bazelbuild/rules_kotlin/releases/download/v1.9.5/rules_kotlin-v1.9.5.tar.gz",
)

load("@io_bazel_rules_kotlin//kotlin:repositories.bzl", "kotlin_repositories", "kotlinc_version")

KOTLIN_VERSION = "1.9.23"

# Get from https://github.com/JetBrains/kotlin/releases/
KOTLINC_RELEASE_SHA = "93137d3aab9afa9b27cb06a824c2324195c6b6f6179d8a8653f440f5bd58be88"

kotlin_repositories(
    compiler_release = kotlinc_version(
        release = KOTLIN_VERSION,
        sha256 = KOTLINC_RELEASE_SHA,
    ),
)

load("@io_bazel_rules_kotlin//kotlin:core.bzl", "kt_register_toolchains")

kt_register_toolchains()
