#!/bin/bash
# spanner_emulator_bootstrap.sh
#
# Script to start a docker container running a GCP Spanner Emulator, which is
# useful for unit tests that interact with a spanner database.
#
# You probably don't want to run this script directly.
# See spanner_emulator_test in //src/main/db/gcp/testing/macros.bzl
#
# Args:
#  $1 - kotlin_jvm_binary that runs tests with the BazelTestRunner
#  $2 - class path of test class

# Make sure external tools are in $PATH
type -p docker || exit 100
type -p sed || exit 100

# Run the docker containter with the spanner emulator.
# The container is launched detached from the shell, with a an avalable port
# from the local machine forwarded to the gRPC port of the emulator (9010).
# The script will clean up after itself and remove the container, but just
# in case, the container is set to timeout and be stopped after five minute
CONTAINERNAME=$(docker run --detach -p 0:9010 --stop-timeout=300  --label kotlin_test_with_spanner_emulator gcr.io/cloud-spanner-emulator/emulator)
echo "Started docker container ${CONTAINERNAME:?}"

# Cleanup the docker image upon exit of the shell.
function cleanup_docker() {
  docker stop "${CONTAINERNAME:?}"
  docker rm "${CONTAINERNAME:?}"
  echo "Removed docker container ${CONTAINERNAME:?}"
}
trap "cleanup_docker" EXIT

# Wait until the container is running.
until [ "$(docker inspect -f {{.State.Running}} ${CONTAINERNAME:?})" == "true" ]; do
    sleep 0.1;
done;

# Figure out which port was assigned to the container i.e. what port was chosen
# as an empty port when we bound zero earlier.
BOUND_PORT=$(docker port ${CONTAINERNAME:?} | sed 's/.*://')
# Set an env variable to say which port to use when talking to the emulator.
export SPANNER_EMULATOR_HOST="localhost:${BOUND_PORT:?}"
"$1" --wrapper_script_flag=--jvm_flag=-Dbazel.test_suite="$2"
# The exit code of the test script determines the status of the tests.
# Exit this script with that status code so bazel knows if they passed.
exit "$?"