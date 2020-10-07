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

def _cue_string_field_impl(ctx):
    args = ctx.actions.args()
    args.add(ctx.attr.package)
    args.add(ctx.attr.identifier)
    args.add(ctx.file.src)
    args.add(ctx.outputs.output)

    ctx.actions.run(
        outputs = [ctx.outputs.output],
        inputs = [ctx.file.src],
        executable = ctx.executable.tool,
        arguments = [args],
    )

_cue_string_field = rule(
    implementation = _cue_string_field_impl,
    attrs = {
        "src": attr.label(
            allow_single_file = True,
            mandatory = True,
        ),
        "identifier": attr.string(
            mandatory = True,
        ),
        "package": attr.string(
            mandatory = True,
        ),
        "output": attr.output(mandatory = True),
        "tool": attr.label(
            executable = True,
            mandatory = True,
            cfg = "exec",
        ),
    },
)

def cue_string_field(name, src, identifier, package = None, **kwargs):
    """Generates a CUE file with a string field containing file contents.

    Output: **name**.cue

    Args:
        src: Input file whose contents should be the field value.
        identifier: The CUE identifier for the string field.
        package: The CUE package. Defaults to the Bazel package name separated
            by underscores.
    """
    if not package:
        package = native.package_name().replace("/", "_")

    _cue_string_field(
        name = name,
        src = src,
        identifier = identifier,
        package = package,
        output = name + ".cue",
        tool = "//build/cue:gen-cue-string-field",
        **kwargs
    )
