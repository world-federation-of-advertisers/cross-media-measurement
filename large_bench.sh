#!/bin/bash

# Large scale benchmark script using 10M VIDs and 1M events
# Get the absolute path of the current directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bazel run --jvmopt=-Xmx20g //src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller:ResultsFulfillerPipeline \
	-- \
	--start-date 2025-01-01 \
	--end-date 2025-03-30 \
	--population-spec-resource-path "${SCRIPT_DIR}/large_population_spec.textproto" \
	--data-spec-resource-path "${SCRIPT_DIR}/large_data_spec.textproto" \
	--use-parallel-pipeline
# --jvm_flags=-agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849 \