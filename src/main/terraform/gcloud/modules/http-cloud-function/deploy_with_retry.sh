#!/usr/bin/env bash
# Copyright 2026 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail
# Keep retries inside this provisioner: the saved Terraform plan may be stale
# after other resources have already changed during a failed apply.
deploy_log=$(mktemp)
trap 'rm -f "$deploy_log"' EXIT

for attempt in 1 2 3; do
  if gcloud "$@" > "$deploy_log" 2>&1; then
    cat "$deploy_log"
    exit 0
  else
    deploy_status=$?
  fi
  cat "$deploy_log"

  # EXPIRED is a terminal Cloud Build queue timeout, before build execution.
  # Compilation, configuration, and permission errors remain fatal.
  if (( attempt == 3 )) || ! grep -Fq 'Build failed with status: EXPIRED' "$deploy_log"; then
    exit "$deploy_status"
  fi
  retry_delay=$((attempt * 30))
  printf 'Cloud Build expired; retrying deployment in %s seconds (attempt %s of 3).\n' "$retry_delay" "$((attempt + 1))"
  sleep "$retry_delay"
done
