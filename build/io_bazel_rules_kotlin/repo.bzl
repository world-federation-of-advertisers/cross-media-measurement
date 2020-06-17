# @io_bazel_rules_kotlin
# See https://github.com/bazelbuild/rules_kotlin/

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def rules_kotin_repo(version, sha256):
    http_archive(
        name = "io_bazel_rules_kotlin",
        urls = ["https://github.com/bazelbuild/rules_kotlin/releases/download/%s/rules_kotlin_release.tgz" % version],
        sha256 = sha256,
    )

def kotlinc_release(version, sha256):
    return {
        "urls": [
            "https://github.com/JetBrains/kotlin/releases/download/v{v}/kotlin-compiler-{v}.zip".format(v = version),
        ],
        "sha256": sha256,
    }
