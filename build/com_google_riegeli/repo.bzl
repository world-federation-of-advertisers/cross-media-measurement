# Copyright 2021 The Cross-Media Measurement Authors
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

"""
Repository rule for Riegeli.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def com_google_riegeli_repo():
    if "com_google_riegeli" in native.existing_rules():
        return

    http_archive(
        name = "com_google_riegeli",
        sha256 = "3f45985164c7f782ffca2feb2e15d06ee96d4687ffdc6bad7adea2a17d087629",
        strip_prefix = "riegeli-63eec0727969205d01dacfd4468ba203d7c8559b",
        url = "https://github.com/google/riegeli/archive/63eec0727969205d01dacfd4468ba203d7c8559b.tar.gz",
    )

    http_archive(
        name = "org_brotli",
        sha256 = "d9f020d178fa45b9c20290334543032c8446d6d86077937db23efcb0d2fd8715",
        strip_prefix = "brotli-8376f72ed6a8ca01548aad1a4f4f1df33094d3e0",
        url = "https://github.com/google/brotli/archive/8376f72ed6a8ca01548aad1a4f4f1df33094d3e0.zip",  # 2021-11-09
    )
