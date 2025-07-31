# Copyright 2025 The Cross-Media Measurement Authors
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

""" Image signing wrapper to apply Make variable expansion """

load("//build:variables.bzl", "IMAGE_REPOSITORY_SETTINGS")
load("@rules_oci//cosign:defs.bzl", "_cosign_sign_impl")

def _wrap_cosign_sign_impl(ctx):
    registry = ctx.expand_make_variables("registry", IMAGE_REPOSITORY_SETTINGS.container_registry, {})
    repository = ctx.expand_make_variables("repository", ctx.attr.image_spec_repository_template, {})
    
    return _cosign_sign_impl(
        ctx,
        name = ctx.label.name,
        image = ctx.attr.image,
        repository = "{registry}/{repository}".format(registry=registry, repository=repository),
        )

wrap_cosign_sign = rule(
    implementation = _wrap_cosign_sign_impl,
    attrs = {
        "image": attr.label(allow_single_file = True, mandatory = True, doc = "Label to an oci_image"),
        "image_spec_repository_template": attr.string(mandatory = True),
    },
    # WIP - solve KMS keys
    args = ["--tlog-upload=false", "--key", "gcpkms://projects/halo-cmm-poc/locations/global/keyRings/key-ring-signed-builds/cryptoKeys/key-signed-builds/cryptoKeyVersions/3"],
    executable = True,
    toolchains = [
        "@rules_oci//cosign:toolchain_type",
        "@aspect_bazel_lib//lib:jq_toolchain_type",
    ],    
)