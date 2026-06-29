/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.rawimpressions

import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile

/**
 * Partitions an upload's [RawImpressionUploadFile]s into Phase-2 VID-labeling batches.
 *
 * Phase 2 partitions work by **whole file**, not by an integer fingerprint shard: an output blob's
 * `entity_keys` and event-group attribution must stay intact, and fingerprint-sharding would
 * scatter one event group's impressions across N output blobs. Each returned batch becomes one
 * `VidLabelingJob` / Phase-2 WorkItem.
 *
 * Shared by the producers that fan out Phase 2 so they partition identically: the Phase-1
 * last-job-out fan-out (`VidRankBuilder`, memoized) and the non-memoized dispatcher
 * (`VidLabelingDispatcher`).
 */
object RawImpressionFileBinPacker {
  /**
   * Bin-packs [files] into batches whose total `size_bytes` stays within [maxFileBatchSizeBytes],
   * using first-fit-decreasing: files are processed largest-first (ties broken by resource name),
   * and each is placed into the first existing batch it still fits in, opening a new batch only
   * when none has room. A single file whose `size_bytes` meets or exceeds the limit fits in no
   * batch and lands in its own (best-effort) batch.
   *
   * The result is a pure function of [files] and [maxFileBatchSizeBytes] and is **deterministic**
   * regardless of input order — so a job's batch-index-keyed `request_id` / `work_item_id` stays
   * stable across redeliveries even if the upload's file set is re-listed.
   *
   * @param files the upload's `RawImpressionUploadFile`s to pack; each `size_bytes` must be
   *   populated (the value the producer reads from storage metadata).
   * @param maxFileBatchSizeBytes the bin-packing threshold: the maximum total `size_bytes` per
   *   batch; must be `> 0`.
   * @return batches of `RawImpressionUploadFile` resource names, in deterministic order.
   */
  fun pack(files: List<RawImpressionUploadFile>, maxFileBatchSizeBytes: Long): List<List<String>> {
    require(maxFileBatchSizeBytes > 0) {
      "maxFileBatchSizeBytes must be > 0, got $maxFileBatchSizeBytes"
    }
    val batchFiles = mutableListOf<MutableList<String>>()
    val batchBytes = mutableListOf<Long>()
    val ordered =
      files.sortedWith(
        compareByDescending<RawImpressionUploadFile> { it.sizeBytes }.thenBy { it.name }
      )
    for (file in ordered) {
      val fit = batchBytes.indexOfFirst { it + file.sizeBytes <= maxFileBatchSizeBytes }
      if (fit >= 0) {
        batchFiles[fit].add(file.name)
        batchBytes[fit] += file.sizeBytes
      } else {
        // No open batch has room (includes a file that alone meets/exceeds the cap): start a new
        // one.
        batchFiles.add(mutableListOf(file.name))
        batchBytes.add(file.sizeBytes)
      }
    }
    return batchFiles
  }
}
