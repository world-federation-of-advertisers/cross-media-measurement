def _cloud_spanner_emulator_impl(rctx):
    version = rctx.attr.version
    sha256 = rctx.attr.sha256

    url = "https://storage.googleapis.com/cloud-spanner-emulator/releases/{version}/cloud-spanner-emulator_linux_amd64-{version}.tar.gz".format(version = version)

    rctx.download_and_extract(
        url = url,
        sha256 = sha256,
    )
    rctx.template("BUILD.bazel", Label("//build/cloud_spanner_emulator:BUILD.external"), executable = False)

cloud_spanner_emulator_binaries = repository_rule(
    implementation = _cloud_spanner_emulator_impl,
    attrs = {"version": attr.string(mandatory = True), "sha256": attr.string()},
)
