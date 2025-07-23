#!/bin/bash

# 90-day 1 billion events benchmark script using 60M VIDs
# Get the absolute path of the current directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bazel run --jvmopt=-Xmx300g //src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller:ResultsFulfillerPipeline \
	-- \
	--start-date 2025-01-01 \
	--end-date 2025-03-30 \
	--event-source-type STORAGE \
	--parallel-batch-size 256 \
	--impressions-storage-root /storage/impressions \
	--impression-dek-storage-root /storage/impressions \
	--labeled-impressions-dek-prefix gs://halotest \
	--event-group-reference-ids edpa-eg-reference-id-1 \
	--population-spec-resource-path "${SCRIPT_DIR}/src/main/proto/wfa/measurement/loadtest/dataprovider/360m_population_spec.textproto" \
	--use-parallel-pipeline
# --jvm_flags=-agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849 \
	# --data-spec-resource-path "${SCRIPT_DIR}/src/main/proto/wfa/measurement/loadtest/dataprovider/90day_60m_data_spec.textproto" \
