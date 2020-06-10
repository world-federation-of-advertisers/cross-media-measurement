load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    _repositories = "repositories",
)

def rules_docker_deps():
    _repositories()
