#!/usr/bin/env bash
#
# Build script for Cloud Spanner Emulator.

# Outputs the specified message to stderr and exits with an error status.
#
# If $? is non-zero, exits with status value. Otherwise, exits with 1.
error() {
  local -i status="$?" && (( status )) || status=1; readonly status
  local -r message="$1"
  echo "${message}" >&2
  exit "${status}"
}

# Returns whether the specified command exists in $PATH.
check_command_exists() {
  local -r command="$1"
  hash "${command}" || error "${command} not found in \$PATH"
}

build() {
  local -r prefix="$1"
  local -r input="$2"
  local -r output="$3"

  # Write Bazel output to a subdirectory of $ruledir so it's cached and gets
  # cleaned up when enclosing workspace is cleaned.
  local -r output_user_root="bazel-user-output"
  mkdir -p "${output_user_root}" || return

  zipper x "${input}" || return
  (
    cd "${prefix}" || exit
    local bazel_output
    bazel_output="$(bazel --output_user_root="../${output_user_root}" \
      build //binaries:emulator_main 2>&1)" || error "${bazel_output}"

    # Copy built binary to $output.
    cp bazel-bin/binaries/emulator_main "${output}" || exit
  )
  local -r status="$?"

  # Clean up extracted files.
  if [[ -d "${prefix}" ]]; then
     chmod -R u+rwx "${prefix}" && rm -r "${prefix}"
  fi

  return "${status}"
}

main() {
  check_command_exists readlink
  check_command_exists mkdir
  check_command_exists bazel

  # Declare arguments, using `readlink` to get absolute paths as we change the
  # shell working directory later.
  local -r ruledir="$1"
  local zipper && zipper="$(readlink -f "$2")" && readonly zipper || return
  local -r prefix="$3"
  local input && input="$(readlink -f "$4")" && readonly input || return
  local output && output="$(readlink -f "$5")" && readonly output || return

  hash -p "${zipper}" zipper

  # Ensure any filesystem writes end up in $ruledir.
  cd "${ruledir}" || return

  build "${prefix}" "${input}" "${output}"
}

main "$@"