#!/usr/bin/env bash
# Thin wrapper to run the reach/frequency offline tool via Bazel.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BAZEL_BIN="${BAZEL_BIN:-bazel}"

TARGET="//src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller:ReachFrequency"

cd "${REPO_ROOT}"
"${BAZEL_BIN}" run "${TARGET}" -- "$@"
