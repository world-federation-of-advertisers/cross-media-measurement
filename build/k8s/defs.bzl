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
load(
    "@rules_pkg//pkg:providers.bzl",
    "PackageArtifactInfo",
    "PackageFilegroupInfo",
)
load("@wfa_common_jvm//build:defs.bzl", "to_label")

ImageImportInfo = provider(
    doc = "Information about importing container images.",
    fields = ["image_ref"],
)

KustomizationDirInfo = provider(
    doc = "Information about a Kustomization dir.",
    fields = {
        "archive": "PackageArtifactInfo containing Kustomization dir TAR",
        "path": "String path of Kustomization dir within the archive",
    },
)

def _get_image_name(image_archive_label):
    """Returns the image ref from the Bazel label.

    Note that this assumes the image is using the default repo and tag name.
    """
    return "{repo}:{tag_name}".format(
        repo = image_archive_label.package,
        tag_name = image_archive_label.name.rsplit(".", 1)[0],
    )

def _kind_load_image_impl(ctx):
    image_archive = ctx.file.image_archive
    runfiles = [image_archive]
    image_name = _get_image_name(ctx.attr.image_archive.label)

    command = "kind load image-archive {archive_path}".format(
        archive_path = image_archive.short_path,
    )
    if ctx.attr.cluster_name:
        command += " --name=" + ctx.attr.cluster_name

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
        "cluster_name": attr.string(
            doc = "Cluster context name",
            mandatory = False,
        ),
    },
    executable = True,
    provides = [DefaultInfo, ImageImportInfo],
)

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

    runfiles = ctx.runfiles().merge_all(runfiles_list)
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
    if len(ctx.attr.srcs) != 1:
        fail("Exactly one item expected in `srcs`")
    src = ctx.attr.srcs[0]
    runfiles = []

    commands, runfiles_list = _get_import_commands(ctx.attr.imports)
    if ctx.attr.delete_selector:
        commands.append("kubectl delete pods,jobs,services,deployments,networkpolicies,ingresses --selector={selector}".format(
            selector = shell.quote(ctx.attr.delete_selector),
        ))

    if KustomizationDirInfo in src:
        dir_info = src[KustomizationDirInfo]
        archive_file = dir_info.archive.file

        commands.extend([
            "tar -xf {archive}".format(archive = archive_file.short_path),
            "kubectl apply -k {dir_path}".format(dir_path = dir_info.path),
        ])

        runfiles.append(archive_file)
    else:
        config_file = ctx.files.srcs[0]
        commands.append("kubectl apply -f {config_file}".format(
            config_file = shell.quote(config_file.short_path),
        ))

        runfiles.append(config_file)

    output = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.write(output, " && ".join(commands), is_executable = True)

    return DefaultInfo(
        executable = output,
        runfiles = ctx.runfiles(files = runfiles).merge_all(runfiles_list),
    )

k8s_apply = rule(
    doc = "Executable that applies a K8s object configuration using kubectl.",
    implementation = _k8s_apply_impl,
    attrs = {
        "srcs": attr.label_list(
            doc = "Single object configuration file or Kustomization to apply",
            providers = [
                [DefaultInfo],
                [KustomizationDirInfo],
            ],
            allow_files = [".yaml", ".json"],
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

def _get_kustomization_path(label):
    parts = []
    if label.workspace_name:
        parts.append(label.workspace_name)
    parts.append(label.package)
    parts.append(label.name)
    return "/".join(parts)

def _kustomization_dir_impl(ctx):
    if len(ctx.attr.srcs) != 1:
        fail("Expecting exactly one pkg_filegroup in srcs")
    filegroup = ctx.attr.srcs[0]
    archive_info = ctx.attr.archive[PackageArtifactInfo]

    return [
        KustomizationDirInfo(
            archive = archive_info,
            path = ctx.attr.path,
        ),
        filegroup[PackageFilegroupInfo],
        filegroup[DefaultInfo],
    ]

_kustomization_dir = rule(
    implementation = _kustomization_dir_impl,
    attrs = {
        "srcs": attr.label_list(
            doc = "pkg_filegroup target that makes up this dir and its deps",
            providers = [PackageFilegroupInfo, DefaultInfo],
            allow_empty = False,
        ),
        "archive": attr.label(
            doc = "pkg_tar archive this dir",
            providers = [PackageArtifactInfo],
            mandatory = True,
        ),
        "path": attr.string(
            doc = "String path of Kustomization dir within archive",
            mandatory = True,
        ),
    },
    provides = [KustomizationDirInfo, PackageFilegroupInfo, DefaultInfo],
)

def kustomization_dir(
        name,
        srcs = None,
        deps = None,
        generate_kustomization = False,
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
        srcs: Files that the Kustomization dir should contain
        deps: `kustomization_dir` dependencies
        generate_kustomization: Whether to generate a kustomization.yaml file
            listing all srcs and deps as resources.
        renames: Map of label to path of overrides.
        visibility: Standard attribute.
        **kwargs: Keyword arguments.
    """
    archive_name = name + "_tar"
    path = _get_kustomization_path(to_label(name))
    files_name = name + "-pkg_files"
    group_name = name + "-pkg_filegroup"

    if generate_kustomization:
        if not srcs:
            srcs = []

        # Generate a kustomization.yaml file listing the resources.
        kustomization_name = name + "-kustomization"
        _kustomization_file(
            name = kustomization_name,
            srcs = srcs + (deps or []),
            visibility = ["//visibility:private"],
            **kwargs
        )

        if not renames:
            renames = {}
        renames[kustomization_name] = "kustomization.yaml"

        srcs.append(kustomization_name)

    pkg_files(
        name = files_name,
        srcs = srcs,
        prefix = path,
        renames = renames,
        visibility = ["//visibility:private"],
        **kwargs
    )
    pkg_filegroup(
        name = group_name,
        srcs = [files_name] + (deps or []),
        visibility = ["//visibility:private"],
        **kwargs
    )
    pkg_tar(
        name = archive_name,
        out = name + ".tar",
        srcs = [group_name],
        visibility = ["//visibility:private"],
        **kwargs
    )

    _kustomization_dir(
        name = name,
        srcs = [group_name],
        archive = archive_name,
        path = path,
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
    dir_path = _get_kustomization_path(ctx.label)

    resources = []
    for src in ctx.attr.srcs:
        if KustomizationDirInfo in src:
            dir_info = src[KustomizationDirInfo]
            resources.append(_relative_path(dir_info.path, dir_path))
        else:
            resources.append(src.files.to_list()[0].basename)

    content_lines = ["resources:"] + [
        "- " + resource
        for resource in resources
    ]

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
            doc = "Items to list in resources",
            allow_files = [".yaml"],
            providers = [
                [DefaultInfo],
                [KustomizationDirInfo],
            ],
        ),
    },
)

def _k8s_kustomize_impl(ctx):
    if len(ctx.attr.srcs) != 1:
        fail("Expecting exactly one kustomization_dir in srcs")
    dir_info = ctx.attr.srcs[0][KustomizationDirInfo]
    archive_file = dir_info.archive.file

    name = ctx.label.name
    config_file = ctx.actions.declare_file(name + ".yaml")

    commands = [
        "tar -xf {archive}".format(archive = archive_file.path),
        "kubectl kustomize {dir} --output={config_file}".format(
            dir = dir_info.path,
            config_file = config_file.path,
        ),
    ]
    ctx.actions.run_shell(
        mnemonic = "Kustomize",
        command = " && ".join(commands),
        inputs = [archive_file],
        outputs = [config_file],
    )

    return [DefaultInfo(files = depset(direct = [config_file]))]

k8s_kustomize = rule(
    doc = "Builds a K8s object configuration file from a Kustomization dir.",
    implementation = _k8s_kustomize_impl,
    attrs = {
        "srcs": attr.label_list(
            doc = "A single kustomization_dir",
            providers = [KustomizationDirInfo],
        ),
    },
)
