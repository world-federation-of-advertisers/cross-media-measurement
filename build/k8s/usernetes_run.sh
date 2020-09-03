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

systemctl --user --quiet is-active u7s.target || {
  echo 'Usernetes is not running. Check status using:' >&2
  echo '  systemctl --user status u7s.target' >&2
  false
}

readonly USERNETES_DIR="${USERNETES_DIR:-"$HOME/usernetes"}"
readonly USERNETES_RUNTIME="$XDG_RUNTIME_DIR/usernetes"
readonly ROOTLESSKIT_CHILD_PIDFILE="$USERNETES_RUNTIME/rootlesskit/child_pid"

child_pid="$(<"$ROOTLESSKIT_CHILD_PIDFILE")"

command="$1"
shift 1

export CONTAINERD_ADDRESS="$USERNETES_RUNTIME/containerd/containerd.sock"
export CONTAINERD_SNAPSHOTTER="fuse-overlayfs"
export CONTAINER_RUNTIME_ENDPOINT="unix://$CONTAINERD_ADDRESS"
export CONTAINERD_NAMESPACE='k8s.io'
export IMAGE_SERVICE_ENDPOINT="unix://$CONTAINERD_ADDRESS"
export PATH="$USERNETES_DIR/bin:$PATH"
exec nsenter -U --preserve-credentials -m -n -t "${child_pid}" --wd="$PWD" \
  "${command}" "$@"