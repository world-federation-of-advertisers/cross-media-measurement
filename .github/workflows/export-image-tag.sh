#!/usr/bin/env bash
# Copyright 2022 The Cross-Media Measurement Authors
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

#set -eu -o pipefail

DEFAULT_BRANCH="${DEFAULT_BRANCH:-main}"

declare tag
if [[ "$GITHUB_EVENT_NAME" == 'release' ]]; then
  tag="${GITHUB_REF_NAME#v}"
elif [[ "$GITHUB_REF_NAME" == "$DEFAULT_BRANCH" ]]; then
  tag="latest"
else
  tag="$GITHUB_SHA"
fi

echo "IMAGE_TAG=${tag}" >> "$GITHUB_ENV"
