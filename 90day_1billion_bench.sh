#!/bin/bash

# 90-day 1 billion events benchmark script using 360M VIDs
# Get the absolute path of the current directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bazel run --jvmopt=-Xmx20g //src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller:ResultsFulfillerPipeline \
	-- \
	--start-date 2025-01-01 \
	--end-date 2025-03-30 \
	--population-spec-resource-path "${SCRIPT_DIR}/src/main/proto/wfa/measurement/loadtest/dataprovider/360m_population_spec.textproto" \
	--data-spec-resource-path "${SCRIPT_DIR}/src/main/proto/wfa/measurement/loadtest/dataprovider/90day_1billion_data_spec.textproto" \
	--use-parallel-pipeline
# --jvm_flags=-agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849 \