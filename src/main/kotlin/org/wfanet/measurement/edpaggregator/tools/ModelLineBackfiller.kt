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

package org.wfanet.measurement.edpaggregator.tools

import java.util.UUID
import java.util.logging.Logger
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadModelLineRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine

/**
 * Adds a new model line to existing (already COMPLETED) `RawImpressionUpload`s so it is labeled
 * over historical data, without asking the data provider to re-upload — Backfill Path B.
 *
 * For each upload it creates a `RawImpressionUploadModelLine` in `CREATED` state for the model line
 * (skipping uploads that already carry it). Creating the model line atomically reactivates a
 * COMPLETED parent upload (`COMPLETED -> ACTIVE`) in the same transaction, so the
 * `VidLabelingMonitorFunction` dispatches the new work. Adding a model line to a FAILED upload is
 * rejected server-side (backfilling a failed upload is unsupported).
 *
 * @param modelLinesStub stub for `RawImpressionUploadModelLineService`.
 */
class ModelLineBackfiller(
  private val modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub
) {
  /** Outcome of a [backfill] run. */
  data class BackfillResult(
    /** Resource names of the model lines created this run (excludes ones that already existed). */
    val createdModelLines: List<String>
  )

  /**
   * Backfills [cmmsModelLine] onto each of [rawImpressionUploads].
   *
   * Idempotent: an upload that already carries the model line is not re-created. Creating the model
   * line reactivates a COMPLETED parent; the server rejects the create if the parent is FAILED.
   *
   * @throws IllegalArgumentException if [rawImpressionUploads] is empty.
   */
  suspend fun backfill(cmmsModelLine: String, rawImpressionUploads: List<String>): BackfillResult {
    require(rawImpressionUploads.isNotEmpty()) { "at least one upload is required" }

    val created = mutableListOf<String>()
    for (uploadName in rawImpressionUploads) {
      val existing = modelLinesStub.findModelLine(uploadName, cmmsModelLine)
      if (existing != null) {
        logger.info(
          "$cmmsModelLine already present under $uploadName (${existing.name}); skipping."
        )
        continue
      }
      // Creating the model line atomically reactivates a COMPLETED parent (and rejects a FAILED
      // one).
      val modelLine =
        modelLinesStub.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            parent = uploadName
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              this.cmmsModelLine = cmmsModelLine
            }
            requestId = backfillRequestId(uploadName, cmmsModelLine)
          }
        )
      created.add(modelLine.name)
      logger.info("Created ${modelLine.name}.")
    }
    return BackfillResult(created)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(ModelLineBackfiller::class.java.name)

    /** Deterministic Create request id so a re-run of the backfill is idempotent. */
    private fun backfillRequestId(uploadName: String, cmmsModelLine: String): String =
      UUID.nameUUIDFromBytes("backfill:$uploadName:$cmmsModelLine".toByteArray()).toString()
  }
}
