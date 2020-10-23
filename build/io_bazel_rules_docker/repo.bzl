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

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def rules_docker_repo(sha256, version = None, commit = None, name = "io_bazel_rules_docker"):
    """Repository rule for rules_docker.

    Args:
        name: Target name.
        version: Release version, e.g. "0.14.4". Mutually exclusive with commit.
        commit: Commit hash. Mutually exclusive with version.
        sha256: SHA256 hash of the source.

    See https://github.com/bazelbuild/rules_docker
    """
    suffix = version if version else commit
    basename = "v" + version if version else commit

    http_archive(
        name = name,
        sha256 = sha256,
        strip_prefix = "rules_docker-" + suffix,
        urls = ["https://github.com/bazelbuild/rules_docker/archive/{basename}.tar.gz".format(basename = basename)],
    )
