load("@io_bazel_rules_kotlin//kotlin:kotlin.bzl", "kt_jvm_test")

def spanner_emulator_test(name, data = [], **kwargs):
    kt_jvm_test(
        name = name,
        data = data + ["@cloud_spanner_emulator//:emulator"],
        **kwargs
    )
