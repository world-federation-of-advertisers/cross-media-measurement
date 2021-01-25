#!/usr/bin/env bash
# Copyright 2020 The Cross-Media Measurement Authors
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

# Generates diagrams from PlantUML source. Assumes optipng is in PATH.

# --- begin runfiles.bash initialization v2 ---
# Copy-pasted from the Bazel Bash runfiles library v2.
set -uo pipefail; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v2 ---

set -eEu -o pipefail

declare -g PLANTUML_JAR

plantuml() {
  java -jar "${PLANTUML_JAR}" "$@"
}

optimize_png() {
  optipng --clobber --strip=all "$@"
}

generate_diagram() {
  local -r src="$1"
  local -r png="${src/%.pu/.png}"

  plantuml "${src}"
  optimize_png "${png}"
}


main() {
  PLANTUML_JAR="$(rlocation plantuml/file/plantuml.jar)"
  readonly PLANTUML_JAR

  local srcs
  srcs="$(find . -name '*.pu')"
  local src
  for src in ${srcs}; do
    generate_diagram "${src}"
  done
}

main "$@"
