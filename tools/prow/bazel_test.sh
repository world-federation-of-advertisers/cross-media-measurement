#!/bin/bash

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
