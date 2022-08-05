# Copyright 2020 The Cross-Media Measurement Authors
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

"""Build defs for Kubernetes (K8s)."""

load("@bazel_skylib//lib:shell.bzl", "shell")

ImageImportInfo = provider(
    doc = "Information about importing container images.",
    fields = ["image_ref"],
)

def _get_image_name(image_archive_label):
    return "{repo}:{label}".format(
        repo = image_archive_label.package,
        label = image_archive_label.name.rsplit(".", 1)[0],
    )

def _kind_load_image_impl(ctx):
    image_archive = ctx.file.image_archive
    runfiles = [image_archive]
    image_name = _get_image_name(ctx.attr.image_archive.label)

    command = "kind load image-archive {archive_path}".format(
        archive_path = image_archive.short_path,
    )

    output = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.write(output, command, is_executable = True)

    return [
        DefaultInfo(
            executable = output,
            runfiles = ctx.runfiles(files = runfiles),
        ),
        ImageImportInfo(
            image_ref = "docker.io/" + image_name,
        ),
    ]

kind_load_image = rule(
    doc = "Executable that loads an image archive into KiND.",
    implementation = _kind_load_image_impl,
    attrs = {
        "image_archive": attr.label(
            doc = "Container image archive.",
            mandatory = True,
            allow_single_file = True,
        ),
    },
    executable = True,
    provides = [DefaultInfo, ImageImportInfo],
)

def _merge_runfiles(runfiles, runfiles_list):
    """Polyfill for runfiles.merge_all.

    TODO(@SanjayVas): Drop this once we're using Bazel 5.0.0+.
    """
    for other in runfiles_list:
        runfiles = runfiles.merge(other)
    return runfiles

def _get_import_commands(import_targets):
    commands = []
    runfiles_list = []
    for import_target in import_targets:
        default_info = import_target[DefaultInfo]
        runfiles_list.append(default_info.default_runfiles)
        commands.append(default_info.files_to_run.executable.short_path)
    return commands, runfiles_list

def _kind_load_images_impl(ctx):
    commands, runfiles_list = _get_import_commands(ctx.attr.deps)

    output = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.write(output, " && ".join(commands), is_executable = True)

    runfiles = _merge_runfiles(ctx.runfiles(), runfiles_list)
    return DefaultInfo(executable = output, runfiles = runfiles)

kind_load_images = rule(
    doc = "Executable that loads multiple image archives into KiND.",
    attrs = {
        "deps": attr.label_list(
            doc = "kind_load_image targets",
            providers = [ImageImportInfo, DefaultInfo],
            cfg = "target",
            mandatory = True,
            allow_empty = False,
        ),
    },
    implementation = _kind_load_images_impl,
)

def _k8s_apply_impl(ctx):
    commands, runfiles_list = _get_import_commands(ctx.attr.imports)
    if ctx.attr.delete_selector:
        commands.append("kubectl delete pods,jobs,services,deployments,networkpolicies,ingresses --selector={selector}".format(
            selector = shell.quote(ctx.attr.delete_selector),
        ))
    commands.append("kubectl apply -f {manifest_file}".format(
        manifest_file = shell.quote(ctx.file.src.short_path),
    ))

    output = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.write(output, " && ".join(commands), is_executable = True)

    runfiles = _merge_runfiles(
        ctx.runfiles(files = ctx.files.src),
        runfiles_list,
    )
    return DefaultInfo(executable = output, runfiles = runfiles)

k8s_apply = rule(
    doc = "Executable that applies a Kubernetes manifest using kubectl.",
    implementation = _k8s_apply_impl,
    attrs = {
        "src": attr.label(
            doc = "A single Kubernetes manifest",
            mandatory = True,
            allow_single_file = [".yaml"],
        ),
        "imports": attr.label_list(
            doc = "kind_load targets of images to import",
            providers = [DefaultInfo, ImageImportInfo],
            cfg = "target",
            allow_empty = True,
        ),
        # TODO(b/168034831): Consider splitting out separate k8s_delete rule
        # with attribute to specify resources.
        "delete_selector": attr.string(
            doc = "Kubernetes label selector to pass to kubectl delete command",
        ),
    },
    executable = True,
)
