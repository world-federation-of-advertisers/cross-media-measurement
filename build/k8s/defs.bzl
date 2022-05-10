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
load("@rules_pkg//pkg:mappings.bzl", "pkg_filegroup", "pkg_files")
load("@rules_pkg//pkg:pkg.bzl", "pkg_tar")
load("@wfa_common_jvm//build:defs.bzl", "to_label")

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
    executable = True,
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
            allow_empty = False,
        ),
        # TODO(b/168034831): Consider splitting out separate k8s_delete rule
        # with attribute to specify resources.
        "delete_selector": attr.string(
            doc = "Kubernetes label selector to pass to kubectl delete command",
        ),
    },
    executable = True,
)

KustomizationDirInfo = provider(
    doc = "Information about a Kustomization dir.",
    fields = {
        "path": "String path of Kustomization dir within the archive",
        "archive": "Tar archive File containing Kustomization dir",
        "deps": "depset of KustomizationDirInfo elements",
    },
)

def _kustomization_apply_impl(ctx):
    if len(ctx.attr.srcs) != 1:
        fail("Expecting exactly one kustomization_dir in srcs")
    dir_info = ctx.attr.srcs[0][KustomizationDirInfo]
    archives = [dir_info.archive] + [
        dep.archive
        for dep in dir_info.deps.to_list()
    ]

    commands = [
        "tar -xf " + archive.short_path
        for archive in archives
    ] + ["kubectl apply -k " + dir_info.path]

    output = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.write(output, " && ".join(commands), is_executable = True)

    return [
        DefaultInfo(
            executable = output,
            runfiles = ctx.runfiles(files = archives),
        ),
    ]

kustomization_apply = rule(
    implementation = _kustomization_apply_impl,
    attrs = {
        "srcs": attr.label_list(
            doc = "A single kustomization_dir target.",
            allow_empty = False,
            providers = [KustomizationDirInfo],
        ),
    },
    executable = True,
)

def _get_kustomization_path(label):
    parts = []
    if label.workspace_name:
        parts.append(label.workspace_name)
    parts.append(label.package)
    parts.append(label.name)
    return "/".join(parts)

def _get_kustomization_dir_deps(deps):
    dir_infos = [dep[KustomizationDirInfo] for dep in deps]
    return depset(
        direct = dir_infos,
        transitive = [dir_info.deps for dir_info in dir_infos],
    )

def _kustomization_dir_impl(ctx):
    if len(ctx.files.srcs) != 1:
        fail("Expecting exactly one kustomization archive in srcs")
    archive = ctx.files.srcs[0]
    deps = _get_kustomization_dir_deps(ctx.attr.deps)

    return [KustomizationDirInfo(
        path = ctx.attr.path,
        archive = archive,
        deps = deps,
    )]

_kustomization_dir = rule(
    implementation = _kustomization_dir_impl,
    attrs = {
        "srcs": attr.label_list(
            doc = "A single tar archive containing a kustomization directory",
            allow_files = [".tar"],
            allow_empty = False,
        ),
        "deps": attr.label_list(
            doc = "Additional kustomization_dir targets.",
            providers = [KustomizationDirInfo],
        ),
        "path": attr.string(
            doc = "Path of Kustomization dir within archive.",
            mandatory = True,
        ),
    },
    provides = [KustomizationDirInfo],
)

def kustomization_dir(
        name,
        srcs,
        deps = None,
        renames = None,
        visibility = None,
        **kwargs):
    """K8s Kustomization directory.

    Implicit targets:
        * **name**.tar: A tar archive containing the Kustomization directory.

          The Kustomization directory path is based on the target label. For
          example, a label "@foo//bar:baz" will result in an archive containing
          a Kustomization directory at "foo/bar/baz".

    Args:
        name: Target name.
        srcs: Source files that the Kustomization directory should contain.
        deps: Other `kustomization_dir` target dependencies
        renames: Map of label to path of overrides.
        visibility: Standard attribute.
        **kwargs: Keyword arguments.
    """
    archive = name + ".tar"
    package_dir = _get_kustomization_path(to_label(name))
    files_name = name + "-files"
    group_name = name + "-file_group"

    pkg_files(
        name = files_name,
        srcs = srcs,
        renames = renames,
        visibility = ["//visibility:private"],
        **kwargs
    )
    pkg_filegroup(
        name = group_name,
        srcs = [files_name],
        visibility = ["//visibility:private"],
        **kwargs
    )

    pkg_tar(
        name = name + "_tar",
        out = archive,
        package_dir = package_dir,
        srcs = [group_name],
        visibility = ["//visibility:private"],
        **kwargs
    )

    _kustomization_dir(
        name = name,
        deps = deps,
        path = package_dir,
        srcs = [archive],
        visibility = visibility,
        **kwargs
    )

def _relative_path(target, src):
    """Returns the relative path from one path to another.

    Args:
        src: Source path that result should be relative to.
        target: Target path.

    TODO(bazelbuild/bazel-skylib#44): Replace with function from Skylib once merged.
    """
    src_segments = src.split("/")
    target_segments = target.split("/")

    common_part_len = 0
    for tp, rp in zip(target_segments, src_segments):
        if tp == rp:
            common_part_len += 1
        else:
            break

    result = [".."] * (len(src_segments) - common_part_len)
    result += target_segments[common_part_len:]

    return "/".join(result) if len(result) > 0 else "."

def _kustomization_file_impl(ctx):
    content_lines = ["resources:"] + [
        "- " + resource.basename
        for resource in ctx.files.srcs
    ]

    if ctx.attr.deps:
        dir_path = _get_kustomization_path(ctx.label)
        content_lines.extend([
            "- " + _relative_path(
                base[KustomizationDirInfo].path,
                dir_path,
            )
            for base in ctx.attr.deps
        ])

    kustomization = ctx.actions.declare_file(ctx.label.name + ".yaml")
    ctx.actions.write(
        output = kustomization,
        content = "\n".join(content_lines),
    )
    return [DefaultInfo(files = depset(direct = [kustomization]))]

_kustomization_file = rule(
    implementation = _kustomization_file_impl,
    attrs = {
        "srcs": attr.label_list(
            doc = "Resource manifests.",
            allow_files = [".yaml"],
        ),
        "deps": attr.label_list(
            doc = "kustomization_dir targets representing bases.",
            providers = [KustomizationDirInfo],
        ),
    },
)

def kustomization_resources_dir(
        name,
        srcs = None,
        deps = None,
        visibility = None,
        **kwargs):
    """kustomization_dir with generated kustomization.yaml for resources.

    Args:
        name: Target name.
        srcs: Manifests to include in Kustomization resources.
        deps: `kustomization_dir` target dependencies to include as bases.
        renames: Map of label to override path.
        visibility: Standard attribute.
        **kwargs: Keyword arguments.
    """
    if not srcs:
        srcs = []

    kustomization_name = name + "-kustomization"
    _kustomization_file(
        name = kustomization_name,
        srcs = srcs,
        deps = deps,
        visibility = ["//visibility:private"],
        **kwargs
    )

    kustomization_dir(
        name = name,
        srcs = srcs + [kustomization_name],
        renames = {
            kustomization_name: "kustomization.yaml",
        },
        deps = deps,
        visibility = visibility,
        **kwargs
    )
