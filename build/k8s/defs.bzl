# Copyright 2020 The Measurement System Authors
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
    fields = ["image_ref", "k8s_environment"],
)

def _get_image_name(image_archive_label):
    return "{repo}:{label}".format(
        repo = image_archive_label.package,
        label = image_archive_label.name.rsplit(".", 1)[0],
    )

def _k8s_import_impl(ctx):
    image_archive = ctx.file.image_archive
    runfiles = [image_archive]
    k8s_env = ctx.attr.k8s_environment
    image_name = _get_image_name(ctx.attr.image_archive.label)

    command = ""
    if k8s_env == "kind":
        command = "kind load image-archive {archive_path}".format(
            archive_path = image_archive.short_path,
        )
    elif k8s_env == "usernetes-containerd":
        usernetes_run = ctx.executable._usernetes_run
        runfiles.append(usernetes_run)

        command = "{usernetes_run} ctr images import {archive_path}".format(
            usernetes_run = usernetes_run.short_path,
            archive_path = image_archive.short_path,
        )
    else:
        fail("Unhandled k8s environment " + k8s_env)

    output = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.write(output, command, is_executable = True)

    return [
        DefaultInfo(
            executable = output,
            runfiles = ctx.runfiles(files = runfiles),
        ),
        ImageImportInfo(
            image_ref = "docker.io/" + image_name,
            k8s_environment = k8s_env,
        ),
    ]

k8s_import = rule(
    doc = "Executable that imports an image archive into a container runtime.",
    implementation = _k8s_import_impl,
    attrs = {
        "image_archive": attr.label(
            doc = "Container image archive.",
            mandatory = True,
            allow_single_file = True,
        ),
        "k8s_environment": attr.string(
            doc = "Which Kubernetes environment to use.",
            values = ["kind", "usernetes-containerd"],
            default = "kind",
        ),
        "_usernetes_run": attr.label(
            doc = "Executable tool for running commands in the Usernetes namespace.",
            default = "//build/k8s:usernetes_run",
            executable = True,
            cfg = "target",
        ),
    },
    executable = True,
    provides = [DefaultInfo, ImageImportInfo],
)

def _k8s_apply_impl(ctx):
    if len(ctx.attr.imports) == 0:
        fail("No imports specified")

    k8s_env = ctx.attr.imports[0][ImageImportInfo].k8s_environment
    runfiles = ctx.runfiles(files = ctx.files.src)
    commands = []
    for import_target in ctx.attr.imports:
        if import_target[ImageImportInfo].k8s_environment != k8s_env:
            fail("k8s_environment must be the same for all imports")
        runfiles = runfiles.merge(import_target[DefaultInfo].default_runfiles)

    commands = [import_executable.short_path for import_executable in ctx.files.imports]
    if ctx.attr.delete_selector:
        commands.append("kubectl delete pods,jobs,services --selector={selector}".format(
            selector = shell.quote(ctx.attr.delete_selector),
        ))
    commands.append("kubectl apply -f {manifest_file}".format(
        manifest_file = shell.quote(ctx.file.src.short_path),
    ))

    output = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.write(output, " && ".join(commands), is_executable = True)

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
            doc = "k8s_import targets of images to import",
            providers = [DefaultInfo, ImageImportInfo],
            cfg = "target",
        ),
        # TODO(b/168034831): Consider splitting out separate k8s_delete rule
        # with attribute to specify resources.
        "delete_selector": attr.string(
            doc = "Kubernetes label selector to pass to kubectl delete command",
        ),
    },
    executable = True,
)
