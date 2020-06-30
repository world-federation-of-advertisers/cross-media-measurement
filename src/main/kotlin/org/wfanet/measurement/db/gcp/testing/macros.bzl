load("@io_bazel_rules_kotlin//kotlin:kotlin.bzl", "kt_jvm_test")

def spanner_emulator_test(name, data = [], tags = [], **kwargs):
    kt_jvm_test(
        name = name,
        data = data + ["@cloud_spanner_emulator//:emulator"],
        tags = tags + ["no-remote-exec"],
        **kwargs
    )
