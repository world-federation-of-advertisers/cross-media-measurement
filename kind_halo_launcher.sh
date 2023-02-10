#!/bin/bash
#
# kind_halo_launcher.sh
#
# Launches a deployment of the Halo system in Kind.  Kind is a kubernetes
# implementation that runs on a user's local machine.  The Kind deployment
# of Halo allows one to launch a full Halo implementation, including the Kingdom,
# Duchies and EDPs, all running in a local environment.
#
# This script can operate in two modes.  In standalone mode, the script launches
# a Halo deployment in Kind and then enables port forwarding to the Kingdom.
# Port forwarding occurs on port 8443, which is the port used for RPCs.
# You can then perform queries against this deployment, for example by using
# the CLI tool at
#   src/main/kotlin/org/wfanet/measurement/api/v2alpha/tools/SimpleReport.kt.
#
# The other mode of operating is the end-to-end test mode.  In this mode, the
# script launches the FrontEndSimulator and displays the logging output to the
# console.  This can be used to verify that the Halo system functions properly
# in a Kubernetes cluster.
#
# Usage:
#
#   kind_halo_launcher.sh [--end-to-end-test]
#
# Where:
#
#  --end-to-end-test:  If present, indicates that the deployment should be
#    launched in end-to-end test mode.  The end-to-end test takes about 20
#    minutes to run.
#
# Note that this script must be run from the cross-media-measurement
# directory.

# BAZEL_OPTS="-c opt"

export HALO_PRINCIPAL_MAP=/tmp/authority_key_identifier_to_principal_map.textproto
export REPORTING_KEY_PAIR=/tmp/encryption_key_pair_config.textproto
export REPORTING_MC_CONFIG=/tmp/measurement_consumer_config.textproto
export DATASET_PATH=/usr/local/google/home/riemanli/Data/benchmarking/event_logs/synthetic-labelled-events.csv

function wait_for() {
  timeout=24
  pod=$1
  state=$2
  until [ $timeout -le 0 ] || (pod_is_in_state $pod $state); do
    echo waiting for $pod to enter $state state
    sleep 5
    timeout=$(( timeout - 1 ))
  done
  if [ $timeout -le 0 ]; then
    return 1
  fi
  echo $pod is $state
}

function pod_is_in_state() {
  pod=$1
  state=$2
  k8s_status=`kubectl get pods | grep $pod | awk '{print $2 $3;}' | head -1`
  if [ "${k8s_status}" = "$state" ]; then
    return 0;
  else
    return 1;
  fi
}

function get_akid {
  local CERT=$1
  openssl x509 -noout -text -in $CERT | \
    sed -n '/Authority Key/,+1p' | \
    sed '1d' | \
    sed 's@^ *@\\x@' | \
    sed 's@:@\\x@g'
}

function add_principal_line {
  local FILE=$1
  local CERT=$2
  local RSN=$3

  local AKID="$(get_akid $CERT)"
  echo "entries {" >> $FILE
  echo "  authority_key_identifier: \"${AKID}\"" >> $FILE
  echo "  principal_resource_name: \"${RSN}\"" >> $FILE
  echo "}" >> $FILE

}

function build_principal_map {
  cat >$HALO_PRINCIPAL_MAP <<HERE
# proto-file: src/main/proto/wfa/measurement/config/authority_key_to_principal_map.proto
# proto-message: AuthorityKeyToPrincipalMap
HERE

  local BASEDIR=src/main/k8s/testing/secretfiles
  add_principal_line $HALO_PRINCIPAL_MAP "$BASEDIR/edp1_root.pem" "${HALO_DATAPROVIDERS[0]}"
  add_principal_line $HALO_PRINCIPAL_MAP "$BASEDIR/edp2_root.pem" "${HALO_DATAPROVIDERS[1]}"
  add_principal_line $HALO_PRINCIPAL_MAP "$BASEDIR/edp3_root.pem" "${HALO_DATAPROVIDERS[2]}"
  add_principal_line $HALO_PRINCIPAL_MAP "$BASEDIR/edp4_root.pem" "${HALO_DATAPROVIDERS[3]}"
  add_principal_line $HALO_PRINCIPAL_MAP "$BASEDIR/edp5_root.pem" "${HALO_DATAPROVIDERS[4]}"
  add_principal_line $HALO_PRINCIPAL_MAP "$BASEDIR/edp6_root.pem" "${HALO_DATAPROVIDERS[5]}"
  add_principal_line $HALO_PRINCIPAL_MAP "$BASEDIR/mc_root.pem" "${HALO_MC}"
}

function initialize_cluster {
  echo
  echo INITIALIZING CLUSTER
  echo
  
  kind delete cluster
  kind create cluster --image=kindest/node:v1.24.2
  export HALO_SECRETNAME=`kubectl get secrets | grep 'certs-and-configs' | awk '{ print $1; }'`
  echo HALO_SECRETNAME=$HALO_SECRETNAME
  rm -f /tmp/authority_key_identifier_to_principal_map.textproto
  touch /tmp/authority_key_identifier_to_principal_map.textproto
  kubectl create configmap config-files --from-file=/tmp/authority_key_identifier_to_principal_map.textproto
  echo Delaying before launching emulators so Kind can catch up ...
  sleep 10
  
  echo
  echo DEPLOYING SPANNER EMULATOR AND FAKE STORAGE SERVER ...
  echo
  bazel run //src/main/k8s/local:emulators_kind
  wait_for spanner-emulator 1/1Running
  wait_for fake-storage-server 1/1Running

  echo
  echo DEPLOYING KINGDOM
  echo
  bazel run //src/main/k8s/local:kingdom_kind
  wait_for system-api-server 1/1Running
  wait_for v2alpha-public-api 1/1Running
  wait_for gcp-kingdom-data 1/1Running

  echo
  echo STARTING RESOURCE SETUP JOB
  echo
  bazel run //src/main/k8s/local:resource_setup_kind
  wait_for resource-setup-job 0/1Completed
}

function get_resource_names {
  export HALO_SECRETNAME=`kubectl get secrets | grep 'certs-and-configs' | awk '{ print $1; }'`
  export HALO_DATAPROVIDERS=(`kubectl logs jobs/resource-setup-job -c resource-setup-container | grep 'Successfully created data provider' | awk '{print $6;}'`)
  export HALO_AGGREGATORCERT=`kubectl logs jobs/resource-setup-job -c resource-setup-container | grep 'Successfully created certificate duchies/aggregator' | awk '{print $5;}'`
  export HALO_WORKER1CERT=`kubectl logs jobs/resource-setup-job -c resource-setup-container | grep 'Successfully created certificate duchies/worker1' | awk '{print $5;}'`
  export HALO_WORKER2CERT=`kubectl logs jobs/resource-setup-job -c resource-setup-container | grep 'Successfully created certificate duchies/worker2' | awk '{print $5;}'`
  export HALO_MC=`kubectl logs jobs/resource-setup-job -c resource-setup-container | grep 'Successfully created measurement consumer:' | awk '{print $6;}'`
  export HALO_MC_APIKEY=`kubectl logs jobs/resource-setup-job -c resource-setup-container | grep 'API key for measurement consumer' | awk '{print $8;}'`
  export HALO_MC_SIGNING_CERT=`kubectl logs jobs/resource-setup-job -c resource-setup-container | grep 'Successfully created measurement consumer signing certificate' | awk '{print $8;}'`
  
  export DB_AUTH_SECRETNAME=`kubectl get secrets | grep 'db-auth' | awk '{ print $1; }'`
  export MC_CONFIG_SECRETNAME=`kubectl get secrets | grep 'mc-config' | awk '{ print $1; }'`
  
  echo
  export -p | grep HALO
  echo
}

function redeploy_kingdom {
  # bazel run //src/main/k8s/local:build_authority_key_identifier_to_principal_map
  echo
  echo REDEPLOYING KINGDOM
  echo
  
  kubectl create configmap config-files --output=yaml --dry-run=client \
    --from-file=$HALO_PRINCIPAL_MAP \
    | kubectl replace -f -

  kubectl rollout restart deployments/v2alpha-public-api-server-deployment
  wait_for v2alpha-public-api 1/1Running
}

function deploy_duchies {
  echo
  echo DEPLOYING DUCHIES
  echo
  
  # bazel run //src/main/k8s/local:duchies_kind \
  tools/bazel-container-run //src/main/k8s/local:duchies_kind \
    --define=aggregator_cert_name=$HALO_AGGREGATORCERT \
    --define=worker1_cert_name=$HALO_WORKER1CERT \
    --define=worker2_cert_name=$HALO_WORKER2CERT

  wait_for aggregator-async-computation-control-server 1/1Running
  wait_for aggregator-computation-control-server 1/1Running
  wait_for aggregator-herald-daemon 1/1Running
  wait_for aggregator-liquid-legions-v2-mill-daemon 1/1Running
  wait_for aggregator-requisition-fulfillment-server 1/1Running
  wait_for aggregator-spanner-computations-server 1/1Running

  wait_for worker1-async-computation-control-server 1/1Running
  wait_for worker1-computation-control-server 1/1Running
  wait_for worker1-herald-daemon 1/1Running
  wait_for worker1-liquid-legions-v2-mill-daemon 1/1Running
  wait_for worker1-requisition-fulfillment-server 1/1Running
  wait_for worker1-spanner-computations-server 1/1Running

  wait_for worker2-async-computation-control-server 1/1Running
  wait_for worker2-computation-control-server 1/1Running
  wait_for worker2-herald-daemon 1/1Running
  wait_for worker2-liquid-legions-v2-mill-daemon 1/1Running
  wait_for worker2-requisition-fulfillment-server 1/1Running
  wait_for worker2-spanner-computations-server 1/1Running
}

function wait_for_edp() {
  timeout=24
  edp_id=$1
  state=$2

  until [ $timeout -le 0 ] || (edp_is_in_state $edp_id "$state"); do
    echo waiting for edp$edp_id to enter state Executing requisitionFulfillingWorkflow
    sleep 5
    timeout=$((timeout - 1))
  done

  if [ $timeout -le 0 ]; then
    return 1
  fi

  echo edp$edp_id is in state Executing requisitionFulfillingWorkflow
}

function edp_is_in_state() {
  edp_id=$1
  state=$2
  edp_pod_id=$(kubectl get pods | grep edp$edp_id | awk '{print $1}')
  edp_status=$(kubectl logs $edp_pod_id -c edp$edp_id-simulator-container| tail -1)

  if [ "$edp_status" = "$state" ]; then
    return 0
  else
    return 1
  fi
}

function deploy_edp_simulators {
  echo
  echo DEPLOYING EDP SIMULATORS
  echo
  
#   tools/bazel-container-run //src/main/k8s/local:edp_simulators_kind \
  tools/bazel-container-run //src/main/k8s/local:edp_simulators_csv_kind \
    --define=mc_name=$HALO_MC \
    --define=edp1_name=${HALO_DATAPROVIDERS[0]} \
    --define=edp2_name=${HALO_DATAPROVIDERS[1]} \
    --define=edp3_name=${HALO_DATAPROVIDERS[2]} \
    --define=edp4_name=${HALO_DATAPROVIDERS[3]} \
    --define=edp5_name=${HALO_DATAPROVIDERS[4]} \
    --define=edp6_name=${HALO_DATAPROVIDERS[5]}

  wait_for edp1-simulator 1/1Running
  wait_for edp2-simulator 1/1Running
  wait_for edp3-simulator 1/1Running
  wait_for edp4-simulator 1/1Running
  wait_for edp5-simulator 1/1Running
  wait_for edp6-simulator 1/1Running

  sleep 5
  echo Copying CSV file from $DATASET_PATH for edp1...
  edp_pod_name=$(kubectl get pods | grep edp1 | awk '{print $1}' | head -1)
  kubectl cp $DATASET_PATH $edp_pod_name:/data/csvfiles/synthetic-labelled-events.csv -c edp1-simulator-container
  echo Copying CSV file for edp2...
  edp_pod_name=$(kubectl get pods | grep edp2 | awk '{print $1}' | head -1)
  kubectl cp $DATASET_PATH $edp_pod_name:/data/csvfiles/synthetic-labelled-events.csv -c edp2-simulator-container
  echo Copying CSV file for edp3...
  edp_pod_name=$(kubectl get pods | grep edp3 | awk '{print $1}' | head -1)
  kubectl cp $DATASET_PATH $edp_pod_name:/data/csvfiles/synthetic-labelled-events.csv -c edp3-simulator-container
  echo Copying CSV file for edp4...
  edp_pod_name=$(kubectl get pods | grep edp4 | awk '{print $1}' | head -1)
  kubectl cp $DATASET_PATH $edp_pod_name:/data/csvfiles/synthetic-labelled-events.csv -c edp4-simulator-container
  echo Copying CSV file for edp5...
  edp_pod_name=$(kubectl get pods | grep edp5 | awk '{print $1}' | head -1)
  kubectl cp $DATASET_PATH $edp_pod_name:/data/csvfiles/synthetic-labelled-events.csv -c edp5-simulator-container
  echo Copying CSV file for edp6...
  edp_pod_name=$(kubectl get pods | grep edp6 | awk '{print $1}' | head -1)
  kubectl cp $DATASET_PATH $edp_pod_name:/data/csvfiles/synthetic-labelled-events.csv -c edp6-simulator-container

  wait_for_edp 1 "INFO: Executing requisitionFulfillingWorkflow..."
  wait_for_edp 2 "INFO: Executing requisitionFulfillingWorkflow..."
  wait_for_edp 3 "INFO: Executing requisitionFulfillingWorkflow..."
  wait_for_edp 4 "INFO: Executing requisitionFulfillingWorkflow..."
  wait_for_edp 5 "INFO: Executing requisitionFulfillingWorkflow..."
  wait_for_edp 6 "INFO: Executing requisitionFulfillingWorkflow..."
}

function do_front_end_correctness_test {
  echo
  echo STARTING FRONT-END SIMULATOR CORRECTNESS TEST
  echo
  
  tools/bazel-container-run //src/main/k8s/local:mc_frontend_simulator_kind \
    --define=mc_name=$HALO_MC \
    --define=mc_api_key=$HALO_MC_APIKEY \

  wait_for frontend-simulator 1/1Running

  echo
  echo The front-end simulator correctness test is now running.  The logs will be displayed below.
  echo
  
  kubectl logs -f jobs/frontend-simulator-job
}

function setup_port_forwarding {
  halo_port_forwarding_active=`ps ax | grep "kubectl port-forward v2alpha-public-api-server" | grep -v grep | wc -l`
  echo halo_port_forwarding_active=$halo_port_forwarding_active
  if [ $halo_port_forwarding_active -eq 1 ]; then
    echo It appears that port forwarding for Halo is already active.
  else
    echo Turning on port forwarding ...
    kubectl port-forward `kubectl get pods | grep v2alpha-public-api-server | awk '{ print $1; }'` 8443:8443 &
    sleep 1
  fi
}

function check_working_directory {
  local DIR=`pwd | sed "s@.*/@@"`
  if [ "$DIR" != "cross-media-measurement" ] ; then
    echo This script must be run from the cross-media-measurement directory.
    exit 1
  fi
}

function check_command_line_arguments {
  if [ "$1" == "" ] ; then
    return
  elif [ "$1" == "--end-to-end-test" ] ; then
    return
  else
    echo "Usage: kind_halo_launcher [--end-to-end-test]"
    exit 1
  fi
}

check_working_directory
check_command_line_arguments $1
initialize_cluster
get_resource_names
build_principal_map
redeploy_kingdom
deploy_duchies
deploy_edp_simulators

if [ "$1" == "--end-to-end-test" ] ; then
  do_front_end_correctness_test
else
  setup_port_forwarding
fi
