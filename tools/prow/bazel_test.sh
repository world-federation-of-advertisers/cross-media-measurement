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

# Copy the logs to the ARTIFACTS directory when finished, so that prow can make
# them available to the user. 
copyLogs() {
  local test_log_dir="$(bazel info bazel-testlogs)"
  cp -Lr "${test_log_dir}" "${ARTIFACTS}/"
}

trap copyLogs EXIT

# Change sso:// references in the WORKSPACE file to https://
sed -i -e 's%sso://team/%https://team.googlesource.com/%' WORKSPACE

# Use gcloud for git and docker authentication.
git config --global credential.helper gcloud.sh
gcloud auth configure-docker

# Run the tests.
# TODO(kmillar): add --config=remote
# TODO(kmillar): add --config=results
bazel test \
  -k \
  //... 
