load("@com_github_grpc_grpc_kotlin//:kt_jvm_grpc.bzl", "kt_jvm_grpc_library")
load("@io_grpc_grpc_java//:java_grpc_library.bzl", "java_grpc_library")
load("@rules_java//java:defs.bzl", "java_proto_library")

def java_and_kt_grpc_library(name, deps):
    java_proto_library(
        name = name + "_java_proto",
        deps = deps,
    )
    java_grpc_library(
        name = name + "_java_grpc",
        srcs = deps,
        deps = [":%s_java_proto" % name],
    )
    kt_jvm_grpc_library(
        name = name + "_kt_jvm_grpc_internal",
        srcs = deps,
        deps = [":%s_java_grpc" % name],
    )

    # Bundle all the dependencies together for convenience.
    native.java_library(
        name = name + "_kt_jvm_grpc",
        exports = [
            "//:kotlinx_coroutines_core",
            ":%s_java_proto" % name,
            ":%s_java_grpc" % name,
            ":%s_kt_jvm_grpc_internal" % name,

            # These aren't strictly necessary, but without them, IntelliJ
            # complains. Until we figure out how to fix IntelliJ's Bazel sync,
            # we will keep these here.
            "@com_github_grpc_grpc_kotlin//stub/src/main/java/io/grpc/kotlin:context",
            "@com_github_grpc_grpc_kotlin//stub/src/main/java/io/grpc/kotlin:stub",
        ],
    )
