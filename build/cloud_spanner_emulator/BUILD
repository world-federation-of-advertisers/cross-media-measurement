package(default_visibility = ["//visibility:private"])

load(":defs.bzl", "PREFIX")

filegroup(
    name = "cloud_spanner_emulator",
    srcs = [":emulator_main"],
    visibility = ["//visibility:public"],
)

sh_binary(
    name = "build",
    srcs = ["build.sh"],
)

BUILD_ARGS = [
    "$(RULEDIR)",
    "$(execpath @bazel_tools//tools/zip:zipper)",
    PREFIX,
    "$<",
    "$@",
]

genrule(
    name = "gen_cloud_spanner_emulator",
    srcs = ["@com_google_cloud_spanner_emulator//file"],
    outs = [
        "emulator_main",
    ],
    cmd = "$(execpath :build) " + " ".join(BUILD_ARGS),
    local = True,
    tools = [
        ":build",
        "@bazel_tools//tools/zip:zipper",
    ],
)
