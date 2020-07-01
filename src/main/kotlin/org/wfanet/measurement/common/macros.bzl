load(
    "@io_bazel_rules_kotlin//kotlin:kotlin.bzl",
    "kt_jvm_binary",
    "kt_jvm_library",
)
load("@io_bazel_rules_docker//kotlin:image.bzl", "kt_jvm_image")
load("@bazel_skylib//lib:collections.bzl", "collections")

_COMMON_DEPS = [
    "//src/main/kotlin/org/wfanet/measurement/common",
]

def kt_jvm_binary_and_image(name, deps = [], **kwargs):
    extended_deps = collections.uniq(deps + _COMMON_DEPS)

    kt_jvm_binary(
        name = name,
        deps = extended_deps,
        **kwargs
    )

    kt_jvm_image(
        name = name + "_image",
        deps = extended_deps,
        **kwargs
    )
