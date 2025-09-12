/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller

/**
 * Configuration for the event processing pipeline.
 *
 * This data class encapsulates all configuration parameters needed to run the pipeline, making it
 * easier to pass configuration between components.
 *
 * @property batchSize Number of events to process in each batch. Batching reduces processing
 *   overhead and improves throughput. Typical values range from 128 to 1024 events per batch.
 * @property channelCapacity Per-worker channel capacity in number of batches. Provides backpressure
 *   to prevent unbounded memory growth. With default capacity of 128 batches and ~256 KiB per batch
 *   (assuming ~1 KiB per event and 256 events/batch), each worker can buffer ~32 MiB.
 * @property threadPoolSize Size of the thread pool for the coroutine dispatcher. Should be tuned
 *   based on available CPU cores and expected workload characteristics.
 * @property workers Number of parallel worker coroutines for processing batches. Workers process
 *   assigned batches in parallel via round-robin distribution. For CPU-bound workloads, optimal
 *   performance is typically achieved with workers equal to or slightly above CPU core count (e.g.,
 *   CPU cores + 1 or 2).
 */
data class PipelineConfiguration(
  val batchSize: Int,
  val channelCapacity: Int,
  val threadPoolSize: Int,
  val workers: Int,
) {

  /**
   * Validates the configuration parameters.
   *
   * @throws IllegalArgumentException if any parameter is invalid
   */
  fun validate() {
    require(batchSize > 0) { "Batch size must be positive" }
    require(channelCapacity > 0) { "Channel capacity must be positive" }
    require(workers > 0) { "Workers must be positive" }
    require(threadPoolSize > 0) { "Thread pool size must be positive" }
  }
}
