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

# @io_bazel_rules_kotlin
# See https://github.com/bazelbuild/rules_docker

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def rules_docker_repo(version, sha256, name = "io_bazel_rules_docker"):
    http_archive(
        name = name,
        sha256 = sha256,
        strip_prefix = "rules_docker-" + version,
        urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v{version}/rules_docker-v{version}.tar.gz".format(version = version)],
    )
