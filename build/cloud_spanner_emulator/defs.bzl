load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_file")

_COMMIT = "c949744161c94a89acd107e34fa3a1bda0fc1b48"
PREFIX = "cloud-spanner-emulator-" + _COMMIT

def cloud_spanner_emulator_archive():
    http_file(
        name = "com_google_cloud_spanner_emulator",
        sha256 = "6028670454dee895be990dd4cc549c191d3fc0e895f6d4a32f28cf537570f9bb",
        urls = ["https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/archive/%s.zip" % _COMMIT],
    )
