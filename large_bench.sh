#!/bin/bash

# Large scale benchmark script using 10M VIDs and 1M events
# Get the absolute path of the current directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bazel run --jvmopt=-Xmx20g //src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller:ResultsFulfillerPipeline \
	-- \
	--event-source-type STORAGE \
	--start-date 2025-01-01 \
	--end-date 2025-02-28 \
	--parallel-batch-size 256 \
	--impressions-storage-root /storage/impressions \
	--impression-dek-storage-root /storage/impressions \
	--labeled-impressions-dek-prefix gs://halotest \
	--event-group-reference-ids edpa-eg-reference-id-1 \
	--use-parallel-pipeline

# --jvm_flags=-agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849 \
