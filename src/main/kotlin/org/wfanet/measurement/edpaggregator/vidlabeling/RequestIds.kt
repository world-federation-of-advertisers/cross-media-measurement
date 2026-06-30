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
   * WorkItem resource id for a memoized SubpoolAssigner WorkItem. Must satisfy RFC 1034
   * (<=63 chars, letter-start, [a-zA-Z0-9-], no '_'): Kingdom model-line ids contain '_' and
   * uploads use long UUIDs, so derive a bounded letter-prefixed deterministic UUID from the key.
   */
  fun forSubpoolAssignerWorkItem(
    uploadName: String,
    modelLineName: String,
    shardIndex: Int,
  ): String = "sa-" + fromKey("subpoolAssignerWorkItem:$uploadName:$modelLineName:$shardIndex")

  /** WorkItem resource id for a non-memoized VidLabeling WorkItem (see [forSubpoolAssignerWorkItem]). */
  fun forVidLabelingWorkItem(vidLabelingJobName: String): String =
    "vl-" + fromKey("vidLabelingWorkItem:$vidLabelingJobName")

  private fun fromKey(key: String): String = UUID.nameUUIDFromBytes(key.toByteArray()).toString()
}
