#!/bin/bash

# Configuration variables
HOST="v2alpha.kingdom.dev.halo-cmm.org"
PORT="8443"
API_KEY="A7Hs-aAQ6OQ"
MEASUREMENT_CONSUMER="measurementConsumers/VCTqwV_vFXw"

# EDP7 (first data provider)
DATA_PROVIDER_1="dataProviders/T5RryPMNong"
EVENT_GROUP_1="dataProviders/T5RryPMNong/eventGroups/A_7BvNsr9iQ"

# EDPA_META (second data provider - edpa-eg-reference-id-2)
DATA_PROVIDER_2="dataProviders/J3-pzhqS9Lo"
EVENT_GROUP_2="dataProviders/J3-pzhqS9Lo/eventGroups/OtjcAsm3ukY"

# Time range
EVENT_START_TIME="2025-01-01T00:00:00.000Z"
EVENT_END_TIME="2025-03-30T00:00:00.000Z"
OUTPUT_FILE="benchmark-results-2edp.csv"

# Privacy parameters
MAX_FREQUENCY=5
RF_REACH_EPSILON=1.0
RF_REACH_DELTA=1e-15
RF_FREQUENCY_EPSILON=1.0
RF_FREQUENCY_DELTA=1e-15

# Sampling parameters
VID_SAMPLING_START=0.1
VID_SAMPLING_WIDTH=0.2

# Event filters (applied to both EDPs)
EVENT_FILTER_1="person.age_group == 1 && person.gender == 1"
EVENT_FILTER_2="person.age_group == 2 && person.gender == 1"
EVENT_FILTER_3="person.age_group == 3 && person.gender == 1"
EVENT_FILTER_4="person.age_group == 1 && person.gender == 2"
EVENT_FILTER_5="person.age_group == 2 && person.gender == 2"
EVENT_FILTER_6="person.age_group == 3 && person.gender == 2"

REPORT_ID="measurementConsumers/VCTqwV_vFXw/reports/benchmark-2edp-$(date +%s)-$RANDOM"
MODEL_LINE=modelProviders/Wt5MH8egH4w/modelSuites/NrAN9F9SunM/modelLines/Esau8aCtQ78

# Build the JAR first to avoid bazel timeout issues
echo "Building Benchmark JAR..."
bazel build //src/main/kotlin/org/wfanet/measurement/api/v2alpha/tools:Benchmark_deploy.jar

# Run the JAR
echo "Running benchmark..."
java -jar bazel-bin/src/main/kotlin/org/wfanet/measurement/api/v2alpha/tools/Benchmark_deploy.jar \
	--tls-cert-file=$SECRETS_DIR/mc_tls.pem \
	--tls-key-file=$SECRETS_DIR/mc_tls.key \
	--cert-collection-file=$SECRETS_DIR/kingdom_root.pem \
	--kingdom-public-api-target=$HOST:$PORT \
	--api-key=$API_KEY \
	--measurement-consumer=$MEASUREMENT_CONSUMER \
	--cumulative \
	--reach-and-frequency \
	--max-frequency=$MAX_FREQUENCY \
	--rf-reach-privacy-epsilon=$RF_REACH_EPSILON \
	--rf-reach-privacy-delta=$RF_REACH_DELTA \
	--rf-frequency-privacy-epsilon=$RF_FREQUENCY_EPSILON \
	--rf-frequency-privacy-delta=$RF_FREQUENCY_DELTA \
	--vid-sampling-start=$VID_SAMPLING_START \
	--vid-sampling-width=$VID_SAMPLING_WIDTH \
	--direct-vid-sampling-width=1.0 \
	--private-key-der-file=$SECRETS_DIR/mc_cs_private.der \
	--encryption-private-key-file=$SECRETS_DIR/mc_enc_private.tink \
	--event-data-provider=$DATA_PROVIDER_1 \
	--event-group=$EVENT_GROUP_1 \
	--event-filter="$EVENT_FILTER_1" \
	--event-filter="$EVENT_FILTER_2" \
	--event-filter="$EVENT_FILTER_3" \
	--event-filter="$EVENT_FILTER_4" \
	--event-filter="$EVENT_FILTER_5" \
	--event-filter="$EVENT_FILTER_6" \
	--event-start-time=$EVENT_START_TIME \
	--event-end-time=$EVENT_END_TIME \
	--event-data-provider=$DATA_PROVIDER_2 \
	--event-group=$EVENT_GROUP_2 \
	--event-filter="$EVENT_FILTER_1" \
	--event-filter="$EVENT_FILTER_2" \
	--event-filter="$EVENT_FILTER_3" \
	--event-filter="$EVENT_FILTER_4" \
	--event-filter="$EVENT_FILTER_5" \
	--event-filter="$EVENT_FILTER_6" \
	--event-start-time=$EVENT_START_TIME \
	--event-end-time=$EVENT_END_TIME \
	--report=$REPORT_ID \
	--model-line=$MODEL_LINE \
	--output-file=$OUTPUT_FILE
