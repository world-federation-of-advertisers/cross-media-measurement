#!/bin/bash
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

set -e
set -u
set -o pipefail

declare -g HAVE_LINT_ERRORS=0

run_linter() {
  local -r linter="$1"
  local -r pattern="$2"

  # We only lint the files that have been changed, so not to spam pull requests
  # with unrelated lint issues.
  local -r modified_files="$(git diff --name-only --diff-filter=ACMRTUXB HEAD~ \
    | grep -E "${pattern}" \
    | tr '\n' ' ' \
    || true)"
  
  if [[ -n "${modified_files}" ]]; then
    echo "${linter} ${modified_files}"
    ${linter} ${modified_files} || HAVE_LINT_ERRORS=1
    echo
  fi
}

main() {
  local -r LINT_KOTLIN="ktlint --experimental --relative"
  run_linter "${LINT_KOTLIN}" '\.kt$'

  local -r LINT_BAZEL="buildifier -mode=check -lint=warn"
  run_linter "${LINT_BAZEL}" '(^|/)BUILD.bazel$|\.bzl$'

  local -r LINT_CC="clang-format --Werror --style=Google --dry-run"
  run_linter "${LINT_CC}" '\.(cc|h)$'
  return ${HAVE_LINT_ERRORS}
}

main "$@"
