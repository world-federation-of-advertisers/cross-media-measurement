# Copyright 2026 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Image digests and the pinned signing tool for CI publication."""

def _image_manifest_impl(ctx):
    manifest = ctx.actions.declare_file(ctx.label.name + ".json")
    args = ctx.actions.args()
    args.add(manifest)
    for repository, digest in zip(ctx.attr.repositories, ctx.files.digests):
        args.add(json.encode(ctx.expand_make_variables("repositories", repository, {})))
        args.add(digest)
        args.add("false" if repository in ctx.attr.unsigned_repositories else "true")
    ctx.actions.run_shell(
        inputs = ctx.files.digests,
        outputs = [manifest],
        arguments = [args],
        command = """
output="$1"
shift
printf '[' > "$output"
separator=''
while [ "$#" -gt 0 ]; do
  printf '%s{"repository":%s,"digest":"%s","sign":%s}' \\
    "$separator" "$1" "$(cat "$2")" "$3" >> "$output"
  separator=','
  shift 3
done
printf ']\\n' >> "$output"
""",
        mnemonic = "ImageManifest",
    )
    cosign = ctx.actions.declare_file(ctx.label.name + ".cosign")
    ctx.actions.symlink(
        output = cosign,
        target_file = ctx.toolchains["@rules_oci//cosign:toolchain_type"].cosign_info.binary,
        is_executable = True,
    )
    return [DefaultInfo(files = depset([manifest, cosign]))]

_image_manifest = rule(
    implementation = _image_manifest_impl,
    attrs = {
        "digests": attr.label_list(allow_files = True, mandatory = True),
        "repositories": attr.string_list(mandatory = True),
        "unsigned_repositories": attr.string_list(),
    },
    toolchains = ["@rules_oci//cosign:toolchain_type"],
)

def image_manifest(name, images, unsigned_repositories = [], **kwargs):
    """Exports images keyed by repository, preserving the existing signing scope."""
    repositories = sorted(images.keys())
    _image_manifest(
        name = name,
        repositories = repositories,
        digests = [str(native.package_relative_label(images[repository])) + ".digest" for repository in repositories],
        unsigned_repositories = unsigned_repositories,
        **kwargs
    )
