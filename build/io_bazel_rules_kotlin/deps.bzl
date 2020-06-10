load("@io_bazel_rules_kotlin//kotlin:kotlin.bzl", "kotlin_repositories", "kt_register_toolchains")

def rules_kotlin_deps(compiler_release):
    kotlin_repositories(compiler_release = compiler_release)

    kt_register_toolchains()
