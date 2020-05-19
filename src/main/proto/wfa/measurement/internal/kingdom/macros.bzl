load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_java//java:defs.bzl", "java_proto_library")

def proto_and_java_proto_library(name, deps = []):
    proto_library(
        name = "%s_proto" % name,
        srcs = ["%s.proto" % name],
        deps = deps,
    )
    java_proto_library(
        name = "%s_java_proto" % name,
        deps = [":%s_proto" % name],
    )
