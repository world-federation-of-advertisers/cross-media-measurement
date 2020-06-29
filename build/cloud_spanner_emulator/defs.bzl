def _cloud_spanner_emulator_impl(rctx):
    commit = rctx.attr.commit
    sha256 = rctx.attr.sha256
    archive_output = "external_workspace"

    rctx.download_and_extract(
        url = "https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/archive/%s.zip" % commit,
        sha256 = sha256,
        stripPrefix = "cloud-spanner-emulator-" + commit,
        output = archive_output,
    )
    workspace_path = rctx.path(archive_output)
    print(workspace_path.realpath)
    rctx.file("BUILD.bazel")
    rctx.file("defs.bzl", content = """
def workspace_path():
    return "{workspace_path}"
    """.format(workspace_path = workspace_path))

cloud_spanner_emulator = repository_rule(
    implementation = _cloud_spanner_emulator_impl,
    attrs = {"commit": attr.string(mandatory = True), "sha256": attr.string()},
)
