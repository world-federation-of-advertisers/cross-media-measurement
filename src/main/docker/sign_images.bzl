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

""" Image signing scripts """

load("//build:variables.bzl", "IMAGE_REPOSITORY_SETTINGS")
load("//src/main/docker/panel_exchange_client:images.bzl", "ALL_IMAGES")
load(":images.bzl", "IMAGES_TO_SIGN")

def _compute_image_list(ctx):
    registry = ctx.expand_make_variables("registry", IMAGE_REPOSITORY_SETTINGS.container_registry, {})
    tag = ctx.expand_make_variables("tag", IMAGE_REPOSITORY_SETTINGS.image_tag, {})

    def image_ref(image_spec):
        repository = ctx.expand_make_variables("repository", image_spec.repository, {})
        return "%s/%s:%s" % (registry, repository, tag)

    return [image_ref(i) for i in IMAGES_TO_SIGN] + [image_ref(i) for i in ALL_IMAGES]

def _sign_images_impl(ctx):
    image_refs = _compute_image_list(ctx)
    images_file = ctx.actions.declare_file(ctx.label.name + "-images.txt")
    ctx.actions.write(
        output = images_file,
        content = "\n".join(image_refs),
    )
    script = ctx.actions.declare_file("%s-sign-script" % ctx.label.name)
    ctx.actions.expand_template(
        template = ctx.file._template,
        output = script,
        substitutions = {
            "{{images_file}}": images_file.short_path,
        },
        is_executable = True,
    )
    runfiles = ctx.runfiles(files = [images_file])
    return [DefaultInfo(executable = script, runfiles = runfiles)]

sign_images = rule(
    implementation = _sign_images_impl,
    attrs = {
        "_template": attr.label(
            default = ":sign_images.sh.template",
            allow_single_file = True,
        ),
    },
    executable = True,
)
