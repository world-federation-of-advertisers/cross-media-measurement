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
build_url_pattern='https://console[.]cloud[.]google[.]com/cloud-build/builds;region=([a-z0-9-]+)/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})\?project=([a-z0-9-]+)'

for attempt in 1 2 3; do
  if gcloud "$@" 2>&1 | tee "$deploy_log"; then
    exit 0
  else
    pipeline_status=("${PIPESTATUS[@]}")
  fi
  deploy_status=${pipeline_status[0]}
  if (( pipeline_status[1] != 0 )); then
    printf 'Could not capture deployment output; stopping retries.\n' >&2
    exit "$((deploy_status == 0 ? pipeline_status[1] : deploy_status))"
  fi
  if (( attempt == 3 )); then
    exit "$deploy_status"
  fi

  # Identify this attempt's build, then confirm its terminal status through the API.
  if ! build_url=$(grep -Eo "$build_url_pattern" "$deploy_log" | sort -u) \
      || [[ "$build_url" == *$'\n'* ]] \
      || ! [[ "$build_url" =~ $build_url_pattern ]]; then
    printf 'Cannot identify a unique Cloud Build for this deployment; stopping retries.\n' >&2
    exit "$deploy_status"
  fi
  build_region=${BASH_REMATCH[1]}
  build_id=${BASH_REMATCH[2]}
  build_project=${BASH_REMATCH[3]}
  build_name="projects/$build_project/locations/$build_region/builds/$build_id"
  # Console links can contain project numbers, which --project does not accept.
  if ! build_status=$(gcloud builds describe "$build_name" \
      "--billing-project=$build_project" '--format=value(status)' --quiet); then
    printf 'Cannot confirm Cloud Build status; stopping retries.\n' >&2
    exit "$deploy_status"
  fi
  # EXPIRED means the build never started. Other failures remain fatal.
  if [[ "$build_status" != EXPIRED ]]; then
    exit "$deploy_status"
  fi
  retry_delay=$((attempt * 30))
  printf 'Cloud Build expired; retrying deployment in %s seconds (attempt %s of 3).\n' "$retry_delay" "$((attempt + 1))"
  sleep "$retry_delay"
done
