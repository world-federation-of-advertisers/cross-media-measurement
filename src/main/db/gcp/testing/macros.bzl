load("@io_bazel_rules_kotlin//kotlin:kotlin.bzl", "kt_jvm_binary")

def spanner_emulator_test(name, test_class, srcs, deps, resources):
    binary_name = name + "_binary"
    kt_jvm_binary(
        name = binary_name,
        # Use the bazel test runner sourced from the bazel test runner jar
        # as the main class for this binary.
        main_class = "com.google.testing.junit.runner.BazelTestRunner",
        args = [
            # This magical arg will allow the binary to be run with bazel run
            # which may be useful for debugging, but is not how the bazel will
            # run the spanner_emulator_test.
            "--wrapper_script_flag=--jvm_flag=-Dbazel.test_suite=%s" % test_class,
        ],
        srcs = srcs,
        deps = deps + ["//src/main/db/gcp/testing:bazel_test_runner_jar"],
        resources = resources,
        testonly = 1,
    )
    native.sh_test(
        name = name,
        srcs = ["//src/main/db/gcp/testing:spanner_emulator_bootstrap"],
        args = [
            "$(rootpath :%s)" % binary_name,
            test_class,
        ],
        # Pulls the kotlin_jvm_binary into the sh_test so it can be run in
        # spanner_emulator_bootstrap.sh
        data = [binary_name],
    )
