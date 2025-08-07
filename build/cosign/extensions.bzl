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

"extensions for bzlmod"

load("@rules_oci//cosign:repositories.bzl", "cosign_register_toolchains")

def _cosign_extension(module_ctx):
    cosign_register_toolchains(name = "cosign", register = False)
    return module_ctx.extension_metadata()

cosign = module_extension(
    implementation = _cosign_extension,
    tag_classes = {
        "toolchains": tag_class(),
    },
)
