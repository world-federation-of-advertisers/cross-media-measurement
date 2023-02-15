#!/bin/sh
#
# This script provides an example of how to submit a request to a Kingdom
# that is running in Kind using the Benchmark CLI.

HALO_MC=`kubectl logs jobs/resource-setup-job -c resource-setup-container | grep 'Successfully created measurement consumer' | awk '{print $6;}' | head -1`
HALO_MC_APIKEY=`kubectl logs jobs/resource-setup-job -c resource-setup-container | grep 'API key for measurement consumer' | awk '{print $8;}'`
OUTPUT_DIR="/usr/local/google/home/riemanli/Data/benchmarking/outputs"

echo
echo HALO_MC=$HALO_MC
echo HALO_MC_APIKEY=$HALO_MC_APIKEY
echo

EDPS=()
EVENT_GROUPS=()
DATA_PROVIDERS=()

for EDP in edp1 edp2 edp3 edp4 edp5 edp6
do
  E=`kubectl get pods | grep ${EDP}-simulator | awk '{print $1}'`
  EG=`kubectl logs $E ${EDP}-simulator-container| grep "Successfully created eventGroup" | awk '{print $5}' | tr -d .`
  DP=`echo $EG | sed "s@/eventGroups.*@@"`
  EDPS+=("$EDP")
  EVENT_GROUPS+=("$EG")
  DATA_PROVIDERS+=("$DP")
done

echo "EDPS = ${EDPS[@]}"
echo
echo "EVENT_GROUPS = ${EVENT_GROUPS[@]}"
echo
echo "DATA_PROVIDERS = ${DATA_PROVIDERS[@]}"
echo
  
bazel build src/main/kotlin/org/wfanet/measurement/api/v2alpha/tools:Benchmark

cur_time="$(date +"%T-%D")"
echo starting time is ${cur_time}

# Reach-only example
bazel-bin/src/main/kotlin/org/wfanet/measurement/api/v2alpha/tools/Benchmark \
  --tls-cert-file src/main/k8s/testing/secretfiles/mc_tls.pem \
  --tls-key-file src/main/k8s/testing/secretfiles/mc_tls.key \
  --cert-collection-file src/main/k8s/testing/secretfiles/kingdom_root.pem \
  --kingdom-public-api-target \
  localhost:8443 \
  --api-key ${HALO_MC_APIKEY} \
  --measurement-consumer ${HALO_MC} \
  --private-key-der-file=src/main/k8s/testing/secretfiles/mc_cs_private.der \
  --encryption-private-key-file=src/main/k8s/testing/secretfiles/mc_enc_private.tink \
  --output-file="${OUTPUT_DIR}/benchmark-results-kind-large-memory-RF-4pubs-noVidSampling-decayRate=1.01-sketchSize=1M.csv" \
  --timeout=36000 \
  --reach-and-frequency \
  --reach-privacy-epsilon=0.0033 \
  --reach-privacy-delta=0.0000000001 \
  --frequency-privacy-epsilon=0.115 \
  --frequency-privacy-delta=0.0000000001 \
  --vid-sampling-start=0.0 \
  --vid-sampling-width=1.0 \
  --vid-bucket-count=50 \
  --max-frequency-for-reach=10 \
  --repetition-count=1 \
  --data-provider "${DATA_PROVIDERS[0]}" \
  --event-group "${EVENT_GROUPS[0]}" \
  --event-start-time=2022-06-30T00:00:01.000Z \
  --event-end-time=2022-07-02T23:59:59.000Z \
  --event-filter="person.gender.value in [0, 1, 2]" \
  --data-provider "${DATA_PROVIDERS[1]}" \
  --event-group "${EVENT_GROUPS[1]}" \
  --event-start-time=2022-06-30T01:00:00.000Z \
  --event-end-time=2022-07-02T05:00:00.000Z \
  --event-filter="person.gender.value in [0, 1, 2]" \
  --data-provider "${DATA_PROVIDERS[2]}" \
  --event-group "${EVENT_GROUPS[2]}" \
  --event-start-time=2022-06-30T01:00:00.000Z \
  --event-end-time=2022-07-02T05:00:00.000Z \
  --event-filter="person.gender.value in [0, 1, 2]" \
  --data-provider "${DATA_PROVIDERS[3]}" \
  --event-group "${EVENT_GROUPS[3]}" \
  --event-start-time=2022-06-30T01:00:00.000Z \
  --event-end-time=2022-07-02T05:00:00.000Z \
  --event-filter="person.gender.value in [0, 1, 2]"

end_time="$(date +"%T-%D")"
echo ending time is ${end_time}