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

load(":build/io_bazel_rules_kotlin/repo.bzl", "kotlinc_release", "rules_kotin_repo")

rules_kotin_repo(
    sha256 = "4fd769fb0db5d3c6240df8a9500515775101964eebdf85a3f9f0511130885fde",
    version = "legacy-1.3.0",
)

load(":build/io_bazel_rules_kotlin/deps.bzl", "rules_kotlin_deps")

rules_kotlin_deps(compiler_release = kotlinc_release(
    sha256 = "ccd0db87981f1c0e3f209a1a4acb6778f14e63fe3e561a98948b5317e526cc6c",
    version = "1.3.72",
))

# @com_github_grpc_grpc_kotlin

git_repository(
    name = "com_github_grpc_grpc_kotlin",
    commit = "3ce43b8713c080dac60dec0ef0d83367a2cece3b",
    remote = "https://github.com/fashing/grpc-kotlin",
    shallow_since = "1590091633 -0400",
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

MAVEN_ARTIFACTS = [
    "com.google.api.grpc:grpc-google-cloud-pubsub-v1:0.1.24",
    "com.google.api.grpc:proto-google-cloud-pubsub-v1:0.1.24",
    "com.google.cloud:google-cloud-core:1.93.5",
    "com.google.cloud:google-cloud-spanner:1.55.1",
    "com.google.cloud:google-cloud-storage:1.109.0",
    "com.google.cloud:google-cloud-nio:0.121.0",
    "io.grpc:grpc-kotlin-stub:0.1.2",
    "junit:junit:4.13",
    "org.jetbrains.kotlinx:kotlinx-coroutines-test:1.3.5",
    "org.mockito:mockito-core:3.3.3",
]

MAVEN_ARTIFACTS += IO_GRPC_GRPC_JAVA_ARTIFACTS

MAVEN_ARTIFACTS += IO_GRPC_GRPC_KOTLIN_ARTIFACTS

MAVEN_ARTIFACTS += COM_GOOGLE_TRUTH_TRUTH_ARTIFACTS

maven_install(
    artifacts = MAVEN_ARTIFACTS,
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

# rules_docker needs @bazel_skylib defined.
http_archive(
    name = "bazel_skylib",
    sha256 = "e5d90f0ec952883d56747b7604e2a15ee36e288bb556c3d0ed33e818a4d971f2",
    strip_prefix = "bazel-skylib-1.0.2",
    urls = ["https://github.com/bazelbuild/bazel-skylib/archive/1.0.2.tar.gz"],
)

load(":build/io_bazel_rules_docker/repo.bzl", "rules_docker_repo")

rules_docker_repo(
    sha256 = "3efbd23e195727a67f87b2a04fb4388cc7a11a0c0c2cf33eec225fb8ffbb27ea",
    version = "0.14.2",
)

load(":build/io_bazel_rules_docker/deps.bzl", "rules_docker_deps")

rules_docker_deps()

load(
    "@io_bazel_rules_docker//kotlin:image.bzl",
    kotlin_image_repositories = "repositories",
)

kotlin_image_repositories()

# @com_google_private_join_and_compute

load(":build/com_google_private_join_and_compute/repo.bzl", "private_join_and_compute_repo")

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

# Public APIs for measurement system.

git_repository(
    name = "wfa_measurement_proto",
    commit = "d6b42bb0fd73a287110e54203783b9c02e35a5b2",
    remote = "sso://team/ads-xmedia-open-measurement-team/wfa-measurement-proto",
)

# AnySketch.

git_repository(
    name = "any_sketch",
    commit = "29945ad07f133aaa03c51834cd2e61c1cc30a5a5",
    remote = "sso://team/ads-xmedia-open-measurement-team/any-sketch",
)
