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

"""World Federation of Advertisers (WFA) GitHub repo macros."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

_RELEASE_URL_TEMPLATE = "https://github.com/world-federation-of-advertisers/{repo}/archive/refs/tags/v{version}.tar.gz"
_COMMIT_URL_TEMPLATE = "https://github.com/world-federation-of-advertisers/{repo}/archive/{commit}.tar.gz"
_PREFIX_TEMPLATE = "{repo}-{suffix}"

def wfa_repo_archive(name, repo, sha256, version = None, commit = None):
    """Adds a WFA repository archive target.

    Args:
      name: target name
      repo: name of repository in world-federation-of-advertisers organization
      sha256: SHA256 hash of archive
      version: release version number. Either this or commit must be specified.
      commit: commit hash. Either this or version must be specified.
    """
    if version:
        suffix = version
        url = _RELEASE_URL_TEMPLATE.format(repo = repo, version = version)
    elif commit:
        suffix = commit
        url = _COMMIT_URL_TEMPLATE.format(repo = repo, commit = commit)
    else:
        fail("version or commit must be specified")

    http_archive(
        name = name,
        urls = [url],
        strip_prefix = _PREFIX_TEMPLATE.format(repo = repo, suffix = suffix),
        sha256 = sha256,
    )
