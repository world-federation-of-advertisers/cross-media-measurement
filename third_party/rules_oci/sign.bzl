# Copyright 2022 The Bazel Contrib Authors
# Copyright 2025 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Sign an oci_image using cosign binary at a remote registry. It signs the image by its digest determined beforehand. """

# Modified copy of https://github.com/bazel-contrib/rules_oci/blob/v2.2.1/cosign/private/sign.bzl to allow make variable expansion.
# It may need to be updated whenever the version of rules_oci used in MODULE.bazel is updated.

_attrs = {
    "image": attr.label(allow_single_file = True, mandatory = True, doc = "Label to an oci_image"),
    "repository_url": attr.string(mandatory = True, doc = "Image repository url subject to make variable expansion"),
    # WIP - local template for debugging. "_sign_sh_tpl": attr.label(default = "@rules_oci//cosign/private:sign.sh.tpl", allow_single_file = True),
    "_sign_sh_tpl": attr.label(default = "//third_party/rules_oci:sign.sh.tpl", allow_single_file = True),
}

def _compute_repository(ctx):
    # handle make variable expansion
    return ctx.expand_make_variables("repository_url", ctx.attr.repository_url, {})

def _cosign_sign_impl(ctx):
    cosign = ctx.toolchains["@rules_oci//cosign:toolchain_type"]
    jq = ctx.toolchains["@aspect_bazel_lib//lib:jq_toolchain_type"]

    executable = ctx.actions.declare_file("cosign_sign_{}.sh".format(ctx.label.name))
    ctx.actions.expand_template(
        template = ctx.file._sign_sh_tpl,
        output = executable,
        is_executable = True,
        substitutions = {
            "{{cosign_path}}": cosign.cosign_info.binary.short_path,
            "{{jq_path}}": jq.jqinfo.bin.short_path,
            "{{image_dir}}": ctx.file.image.short_path,
            "{{fixed_args}}": " ".join(["--repository", _compute_repository(ctx)]),
        },
    )

    runfiles = ctx.runfiles(files = [ctx.file.image])
    runfiles = runfiles.merge(ctx.attr.image[DefaultInfo].default_runfiles)
    runfiles = runfiles.merge(jq.default.default_runfiles)
    runfiles = runfiles.merge(cosign.default.default_runfiles)

    return DefaultInfo(executable = executable, runfiles = runfiles)

cosign_sign = rule(
    implementation = _cosign_sign_impl,
    attrs = _attrs,
    executable = True,
    toolchains = [
        "@rules_oci//cosign:toolchain_type",
        "@aspect_bazel_lib//lib:jq_toolchain_type",
    ],
)
