load(
    "@io_bazel_rules_kotlin//kotlin:kotlin.bzl",
    "kt_jvm_binary",
    "kt_jvm_library",
)
load("@io_bazel_rules_docker//kotlin:image.bzl", "kt_jvm_image")

def kt_jvm_binary_and_image(name, srcs, deps, main_class, visibility):
    kt_jvm_binary(
        name = name,
        srcs = srcs,
        main_class = main_class,
        deps = deps + [
            "//src/main/kotlin/org/wfanet/measurement/common",
            "@io_grpc_grpc_java//netty",
        ],
        visibility = visibility,
    )

    kt_jvm_image(
        name = name + "_image",
        srcs = srcs,
        main_class = main_class,
        deps = deps + [
            "//src/main/kotlin/org/wfanet/measurement/common",
            "@io_grpc_grpc_java//netty",
        ],
        visibility = visibility,
    )
