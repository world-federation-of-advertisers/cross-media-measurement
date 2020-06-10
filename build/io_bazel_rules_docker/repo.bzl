# @io_bazel_rules_kotlin
# See https://github.com/bazelbuild/rules_docker

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def rules_docker_repo(version, sha256):
    http_archive(
        name = "io_bazel_rules_docker",
        sha256 = sha256,
        strip_prefix = "rules_docker-" + version,
        urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v{version}/rules_docker-v{version}.tar.gz".format(version = version)],
    )
