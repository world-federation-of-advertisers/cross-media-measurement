# Copyright 2025 The Cross-Media Measurement Authors
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

#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <MILESTONE> <ISSUE_SEARCH>" >&2
  exit 1
fi

MILESTONE=$1
ISSUE_SEARCH=$2

notes_json_array=$(
  gh issue list \
    --milestone "${MILESTONE}" \
    --search "${ISSUE_SEARCH}" \
    --json number,title \
    --jq 'map("\(.title). See [Issue #\(.number)]")'
)

printf 'json=%s\n' "$(jq -c . <<< "${notes_json_array}")"
