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

"""Build defs for rules_oci."""

load("@rules_oci//oci:defs.bzl", "oci_image")
load("@rules_pkg//pkg:mappings.bzl", "pkg_files")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
load("//build:variables.bzl", "MEASUREMENT_SYSTEM_REPO")

DEFAULT_PY_IMAGE_BASE = Label("@py_image_base")

def py_image(
        name,
        binary,
        base = None,
        labels = None,
        visibility = None,
        **kwargs):
    """Python container image.

    For standard attributes, see
    https://bazel.build/reference/be/common-definitions

    Args:
      name: name of the resulting oci_image target
      binary: label of py_binary target
      base: label of base Python oci_image
      labels: dictionary of labels for the image config
      visibility: standard attribute
      **kwargs: other args to pass to the resulting target
    """
    labels = labels or {}
    labels.update({
        "org.opencontainers.image.source": MEASUREMENT_SYSTEM_REPO,
    })

    pyzip_name = "{name}_pyzip".format(name = name)
    native.filegroup(
        name = pyzip_name,
        srcs = [binary],
        output_group = "python_zip_file",
        visibility = ["//visibility:private"],
    )

    pyzip_file_name = pyzip_name + ".zip"
    pyzip_label = ":" + pyzip_name
    files_name = "{name}_files".format(name = name)
    pkg_files(
        name = files_name,
        srcs = [pyzip_label],
        renames = {
            pyzip_label: pyzip_file_name,
        },
        visibility = ["//visibility:private"],
    )

    layer_name = "{name}_layer".format(name = name)
    pkg_tar(
        name = layer_name,
        srcs = [":" + files_name],
        extension = "tar.gz",
        visibility = ["//visibility:private"],
    )

    oci_image(
        name = name,
        base = base or DEFAULT_PY_IMAGE_BASE,
        tars = [":" + layer_name],
        entrypoint = ["python3", "/" + pyzip_file_name],
        labels = labels,
        visibility = visibility,
        **kwargs
    )
