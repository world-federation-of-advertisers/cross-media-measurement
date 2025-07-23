#!/bin/bash

# Example script to run the parallel event processing pipeline
# This demonstrates processing multiple days of events in parallel

# Build the target
echo "Building event processing CLI..."
bazel build //src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller:event_processing_cli

# Run with parallel pipeline enabled
echo "Running parallel pipeline..."
bazel-bin/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/event_processing_cli \
  --start-date 2025-01-01 \
  --end-date 2025-01-07 \
  --batch-size 10000 \
  --channel-capacity 100 \
  --use-parallel-pipeline true \
  --parallel-batch-size 50000 \
  --parallel-workers 0 \
  --thread-pool-size 0 \
  --zone-id UTC \
  --population-spec-resource-path "$1" \
  --data-spec-resource-path "$2"

# Notes:
# --parallel-workers 0: Uses all available CPU cores
# --thread-pool-size 0: Uses 8x CPU cores for the thread pool
# --parallel-batch-size: Larger batch size for parallel processing
# 
# The pipeline will:
# 1. Generate events for each day (Jan 1-7) in parallel
# 2. Process date shards concurrently on multiple threads
# 3. Batch events and distribute them to workers
# 4. Apply CEL filters in parallel
# 5. Aggregate results in frequency vectors