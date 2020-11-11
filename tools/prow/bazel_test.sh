#!/usr/bin/env bash

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

set -eEu -o pipefail

readonly BAZEL="${BAZEL:-bazel}"

# Copies Bazel test logs to the ARTIFACTS directory so that Prow can make them
# available to the user.
copy_test_logs() {
  local bazel_testlogs
  bazel_testlogs="$($BAZEL info bazel-testlogs)"

  cp -Lr "${bazel_testlogs}" "${ARTIFACTS}/"
}

configure_auth() {
  # Use gcloud auth for Docker.
  gcloud auth configure-docker
}

# Configure access for Google-hosted Git.
configure_google_git() {
  # Use gcloud for Git auth.
  git config --global credential.helper gcloud.sh

  # Change sso:// references in the WORKSPACE file to https://
  sed -i -e 's%sso://team/%https://team.googlesource.com/%' WORKSPACE
}

main() {
  local -i failed=0
  configure_auth
  configure_google_git

  # Build all targets.
  $BAZEL --nohome_rc build --keep_going //... || failed=1

  # Run all tests.
  $BAZEL --nohome_rc test --keep_going --test_output=errors //... || failed=1
  copy_test_logs

  ! ((failed))
}

main "$@"
