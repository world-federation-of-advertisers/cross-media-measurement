_URL_PATTERN_PREFIX = "https://github.com/bazelbuild/bazel/releases/download/{version}/bazel-{version}-"

def _get_platform_target(os_name):
    if os_name.startswith("mac os"):
        return "darwin-x86_64"
    if os_name.startswith("windows"):
        return "windows-x86_64.exe"
    return "linux-x86_64"

def _bazel_binary_impl(rctx):
    version = rctx.attr.version

    os_name = rctx.os.name.lower()
    platform_target = _get_platform_target(os_name)
    url_pattern = _URL_PATTERN_PREFIX + platform_target
    url = url_pattern.format(version = version)

    rctx.download(url, executable = True, output = "bazel")
    rctx.template("BUILD.bazel", Label("//build/bazel:BUILD.external"), executable = False)

_bazel_binary = repository_rule(
    attrs = {
        "version": attr.string(mandatory = True),
    },
    implementation = _bazel_binary_impl,
)

def bazel_binary(name, version):
    """Repository rule for Bazel binary."""
    _bazel_binary(
        name = name,
        version = version,
    )
