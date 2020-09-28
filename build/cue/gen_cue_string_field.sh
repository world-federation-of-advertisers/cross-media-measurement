#!/usr/bin/env bash

output_content() {
  echo "package ${package}"
  echo
  echo "${identifier}: ###\"\"\""
  cat "${infile}"
  echo '"""###'
}

main() {
  local -r package="$1"
  local -r identifier="$2"
  local -r infile="$3"
  local -r outfile="$4"

  output_content > "$outfile"
}

main "$@"