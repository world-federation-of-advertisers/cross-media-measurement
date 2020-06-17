load(
    "@io_bazel_rules_kotlin//kotlin:kotlin.bzl",
    "kt_jvm_binary",
    "kt_jvm_library",
)
load("@io_bazel_rules_docker//kotlin:image.bzl", "kt_jvm_image")

def kt_jvm_binary_and_image(name, deps = [], runtime_deps = [], **kwargs):
    extended_deps = deps + ["//src/main/kotlin/org/wfanet/measurement/common"]
    extended_runtime_deps = runtime_deps + ["//third_party/java/io/grpc/netty"]

    kt_jvm_binary(
        name = name,
        deps = extended_deps,
        runtime_deps = extended_runtime_deps,
        **kwargs
    )

    kt_jvm_image(
        name = name + "_image",
        deps = extended_deps,
        runtime_deps = extended_runtime_deps,
        **kwargs
    )
