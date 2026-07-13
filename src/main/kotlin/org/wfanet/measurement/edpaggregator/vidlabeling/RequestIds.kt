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

package org.wfanet.measurement.edpaggregator.vidlabeling

import java.security.MessageDigest
import java.util.UUID

/**
 * Single source of truth for the AIP-155 `request_id`s used across VID labeling resource creation.
 *
 * Every key is a deterministic UUID derived from a resource-type-prefixed string. The type prefix
 * prevents cross-resource collisions, and including the full parent context (e.g. `uploadName`)
 * ensures two logically distinct resources never share a `request_id` — otherwise the second create
 * would return the first's cached resource. Stable across retries: the same inputs always produce
 * the same id, so Pub/Sub redelivery is idempotent.
 */
object RequestIds {
  /** `request_id` for creating a `RawImpressionUpload` from a done blob. */
  fun forRawImpressionUpload(doneBlobPath: String, generation: Long): String =
    fromKey("rawImpressionUpload:$doneBlobPath:$generation")

  /**
   * `request_id` for creating a `RawImpressionUploadFile`.
   *
   * Includes [uploadName] so the same `fileBlobUri` landing in two different uploads yields two
   * distinct request IDs (and therefore two distinct files parented to the correct upload).
   */
  fun forRawImpressionUploadFile(uploadName: String, fileBlobUri: String): String =
    fromKey("rawImpressionUploadFile:$uploadName:$fileBlobUri")

  /** `request_id` for creating a `RawImpressionUploadModelLine`. */
  fun forRawImpressionUploadModelLine(uploadName: String, modelLineName: String): String =
    fromKey("rawImpressionUploadModelLine:$uploadName:$modelLineName")

  /**
   * `request_id` for creating a Phase-0 `PoolAssignmentJob`.
   *
   * Keyed on [uploadName], [modelLineName], and [shardIndex] so each (upload, model line, shard)
   * row is created at most once: a redelivered `BatchCreatePoolAssignmentJobs` returns the existing
   * rows instead of duplicating them per shard.
   */
  fun forPoolAssignmentJob(uploadName: String, modelLineName: String, shardIndex: Int): String =
    fromKey("poolAssignmentJob:$uploadName:$modelLineName:$shardIndex")

  /**
   * `request_id` for creating a non-memoized Phase-2 `VidLabelingJob`.
   *
   * Keyed on [uploadName], the **sorted** [modelLineNames] bundled into the job, and [batchIndex]
   * so a redelivered bundle reuses the existing rows, while a later-added model line (a different
   * bundle set) creates its own jobs instead of colliding with the existing bundle's rows.
   */
  fun forVidLabelingJob(uploadName: String, modelLineNames: List<String>, batchIndex: Int): String =
    fromKey("vidLabelingJob:$uploadName:${modelLineNames.sorted().joinToString(",")}:$batchIndex")

  /**
   * `request_id` for marking a `VidLabelingJob` SUCCEEDED.
   *
   * Keyed on [vidLabelingJobName] so a Pub/Sub redelivery of the same job's completion reuses the
   * same id and the server returns the cached result instead of re-transitioning the resource.
   */
  fun forMarkVidLabelingJobSucceeded(vidLabelingJobName: String): String =
    fromKey("markVidLabelingJobSucceeded:$vidLabelingJobName")

  /**
   * `request_id`s for the five `MarkRawImpressionUploadModelLine*` transitions.
   *
   * Keyed on the operation and [modelLineName] so a Pub/Sub redelivery of the same transition
   * reuses the same id: the server replays it as an idempotent no-op (returning the already-marked
   * row) instead of surfacing `FAILED_PRECONDITION`. Distinct per operation so each transition
   * stamps its own per-mark request-id column.
   */
  fun forMarkRawImpressionUploadModelLinePoolAssigning(modelLineName: String): String =
    fromKey("markRawImpressionUploadModelLinePoolAssigning:$modelLineName")

  fun forMarkRawImpressionUploadModelLineRanking(modelLineName: String): String =
    fromKey("markRawImpressionUploadModelLineRanking:$modelLineName")

  fun forMarkRawImpressionUploadModelLineLabeling(modelLineName: String): String =
    fromKey("markRawImpressionUploadModelLineLabeling:$modelLineName")

  fun forMarkRawImpressionUploadModelLineCompleted(modelLineName: String): String =
    fromKey("markRawImpressionUploadModelLineCompleted:$modelLineName")

  fun forMarkRawImpressionUploadModelLineFailed(modelLineName: String): String =
    fromKey("markRawImpressionUploadModelLineFailed:$modelLineName")

  private fun fromKey(key: String): String {
    // Render the deterministic (name-based) id in the RFC 4122 version-4 layout so the value
    // conforms to the `(google.api.field_info).format = UUID4` annotation shared by every
    // request_id
    // field across the EDPA gRPC APIs. A random v4 UUID can't be reproduced on retry, so the 16
    // bytes come from a SHA-256 hash of [key] with the version and variant bits set to 4 / IETF.
    val bytes = MessageDigest.getInstance("SHA-256").digest(key.toByteArray())
    bytes[6] = ((bytes[6].toInt() and 0x0f) or 0x40).toByte() // version 4
    bytes[8] = ((bytes[8].toInt() and 0x3f) or 0x80).toByte() // IETF variant
    var msb = 0L
    var lsb = 0L
    for (i in 0 until 8) msb = (msb shl 8) or (bytes[i].toLong() and 0xff)
    for (i in 8 until 16) lsb = (lsb shl 8) or (bytes[i].toLong() and 0xff)
    return UUID(msb, lsb).toString()
  }
}
