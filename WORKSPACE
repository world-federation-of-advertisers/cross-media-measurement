workspace(name = "wfa_measurement_proto")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Support Maven sources

http_archive(
    name = "rules_jvm_external",
    sha256 = "62133c125bf4109dfd9d2af64830208356ce4ef8b165a6ef15bbff7460b35c3a",
    strip_prefix = "rules_jvm_external-3.0",
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/3.0.zip",
)

load("@rules_jvm_external//:defs.bzl", "maven_install")

# gRPC Java
# See https://github.com/grpc/grpc-java/blob/master/examples/WORKSPACE

http_archive(
    name = "io_grpc_grpc_java",
    sha256 = "ca30194aa4ff175f910bbf212911f1b35c17307833da0afcfba07c525f28fff7",
    strip_prefix = "grpc-java-1.28.0",
    url = "https://github.com/grpc/grpc-java/archive/v1.28.0.tar.gz",
)

load("@io_grpc_grpc_java//:repositories.bzl", "IO_GRPC_GRPC_JAVA_ARTIFACTS", "IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS", "grpc_java_repositories")

maven_install(
    artifacts = [
        "com.google.api.grpc:grpc-google-cloud-pubsub-v1:0.1.24",
        "com.google.api.grpc:proto-google-cloud-pubsub-v1:0.1.24",
    ] + IO_GRPC_GRPC_JAVA_ARTIFACTS,
    generate_compat_repositories = True,
    override_targets = IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS,
    repositories = [
        "https://repo.maven.apache.org/maven2/",
    ],
)

load("@maven//:compat.bzl", "compat_repositories")

compat_repositories()

# Run grpc_java_repositories after compat_repositories to ensure the
# maven_install-selected dependencies are used.
grpc_java_repositories()

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()
