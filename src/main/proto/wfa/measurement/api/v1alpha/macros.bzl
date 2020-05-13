load("@rules_java//java:defs.bzl", "java_proto_library")
load("@com_github_grpc_grpc_kotlin//:kt_jvm_grpc.bzl", "kt_jvm_grpc_library")

def java_and_kt_grpc_library(name, deps):
    java_proto_library(name = name + "_java_proto", deps = deps)
    kt_jvm_grpc_library(name = name + "_kt_jvm_grpc", srcs = deps, deps = [":%s_java_proto" % name])
