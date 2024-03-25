load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file", "http_jar")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def archive_dependencies(third_party):
    return [
        # Needed for @grpc_java//compiler:grpc_java_plugin.
        {
            "name": "io_grpc_grpc_java",
            "sha256": "5d617856c295d863307f4036a1b1e93f9eeaf6da41424d2de7c9b330a810fc3b",
            "strip_prefix": "grpc-java-1.62.2",
            "urls": ["https://github.com/grpc/grpc-java/archive/v1.62.2.zip"],
            # Bzlmod: Waiting for https://github.com/bazelbuild/bazel-central-registry/issues/353
        },
    ]

def buildfarm_dependencies(repository_name = "build_buildfarm"):
    """
    Define all 3rd party archive rules for buildfarm

    Args:
      repository_name: the name of the repository
    """
    third_party = "@%s//third_party" % repository_name
    for dependency in archive_dependencies(third_party):
        params = {}
        params.update(**dependency)
        name = params.pop("name")
        maybe(http_archive, name, **params)