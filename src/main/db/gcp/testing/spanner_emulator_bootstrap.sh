#!/usr/bin/env bash
# spanner_emulator_bootstrap.sh
#
# Script to start the Cloud Spanner Emulator and then run a test binary.
#
# You probably don't want to run this script directly.
# See spanner_emulator_test in //src/main/db/gcp/testing/macros.bzl
#
# Args:
#   Path to the Cloud Spanner Emulator binary.
#   Path to the test binary. The test binary must respect the
#     SPANNER_EMULATOR_HOST environment variable to indicate the address of the
#     running Cloud Spanner Emulator. This is usually a kt_jvm_binary.
#   Remaining args are passed to the test binary, e.g. `--wrapper_script_flag`.

# Ensure that we don't leave subprocesses running.
trap "kill 0" EXIT

# Outputs the specified message to stderr and exits with an error status.
#
# If $? is non-zero, exits with status value. Otherwise, exits with 1.
error() {
  local status="$?"
  (( status )) || status=1
  local -r message="$1"
  echo "${message}" >&2
  exit "${status}"
}

# Exits if the specified command is not found in $PATH.
check_command_exists() {
  local -r command="$1"
  hash "${command}" || error "${command} not found in \$PATH"
}

# Exits if dependencies are not found. Outputs a command to list sockets.
check_dependencies() {
  check_command_exists uname
  check_command_exists awk
  check_command_exists grep
  check_command_exists timeout

  local system && system="$(uname -s)" && readonly system || return
  case "${system}" in
    Linux)
      check_command_exists ss
      echo "ss --listening --tcp --numeric --no-header | awk '{print \$4}'"
      ;;
    Darwin)
      check_command_exists netstat
      echo "netstat -n -a -p=tcp | grep LISTEN | awk '{print \$4}'"
      ;;
    *)
      error "System ${system} not supported"
      ;;
  esac
}

# Returns wheter the specified TCP port is in use.
#
# Globals:
#   list_sockets_command
port_in_use() {
  local -r port="$1"
  bash -c "${list_sockets_command}" | grep --quiet ":${port}\$"
}

# Outputs an unused TCP port.
get_unused_port() {
  local -ri MAX_PORT=65535
  local -i port=49152
  while port_in_use "${port}"; do
    (( port++ ))
    (( port > MAX_PORT )) && return 1
  done
  echo "${port}"
}

# Returns whether the process with the specified PID is running.
process_running() {
  local -r pid="$1"
  kill -0 "${pid}"  # Doesn't kill, just checks whether signal is deliverable.
}

# Sleep for the specified number of floating point seconds.
sleep_float() {
  local -r seconds="$1"
  local _

  # Not all implementations of the `sleep` command accept floating point values,
  # so we use the `read` builtin. Note that sleeping is skipped if there's any
  # input.
  read -rp '' -t "${seconds}" _
}

main() {
  local -r emulator="$1"
  local -r test_binary="$2"
  shift 2

  declare -g list_sockets_command &&
    list_sockets_command="$(check_dependencies)" &&
    readonly list_sockets_command || return

  local port && port="$(get_unused_port)" && readonly port || return

  # Start emulator with chosen port.
  timeout 10s "${emulator}" --host_port="localhost:${port}" &
  local -r emulator_pid="$!"

  # Wait until emulator has taken port.
  echo 'Waiting for Cloud Spanner Emulator to start'
  while ! port_in_use "${port}"; do
    process_running "${emulator_pid}" || error 'Emulator not running'
    sleep_float 0.1
  done

  # Run test binary, setting environment variable to let it know the port.
  SPANNER_EMULATOR_HOST="localhost:${port}" ${test_binary} "$@"
  local -r test_status="$?"

  # Send SIGTERM signal to emulator process and wait for it to end.
  kill -s SIGTERM "${emulator_pid}"
  wait "${emulator_pid}"

  # Return status code of test binary. Bazel uses this to determine if tests
  # passed.
  return "${test_status}"
}

main "$@"
