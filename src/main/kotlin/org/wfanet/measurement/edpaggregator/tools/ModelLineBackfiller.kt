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
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadModelLineRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadActiveRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine

/**
 * Adds a new model line to existing (already COMPLETED) `RawImpressionUpload`s so it is labeled
 * over historical data, without asking the data provider to re-upload — Backfill Path B.
 *
 * For each upload it creates a `RawImpressionUploadModelLine` in `CREATED` state for the model line
 * (skipping uploads that already carry it) and then reactivates the parent upload (`COMPLETED ->
 * ACTIVE`) so the `VidLabelingMonitorFunction` dispatches it. The create precedes the reactivation
 * so the new work exists before the upload becomes ACTIVE.
 *
 * @param modelLinesStub stub for `RawImpressionUploadModelLineService`.
 * @param uploadsStub stub for `RawImpressionUploadService` (used to reactivate the parent).
 */
class ModelLineBackfiller(
  private val modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val uploadsStub: RawImpressionUploadServiceCoroutineStub,
) {
  /** Outcome of a [backfill] run. */
  data class BackfillResult(
    /** Resource names of the model lines created this run (excludes ones that already existed). */
    val createdModelLines: List<String>,
    /** Resource names of the uploads reactivated this run. */
    val reactivatedUploads: List<String>,
  )

  /**
   * Backfills [cmmsModelLine] onto each of [uploadIds] under [dataProvider].
   *
   * Idempotent: an upload that already carries the model line is not re-created, and reactivating
   * an already-ACTIVE upload is a no-op.
   *
   * @throws IllegalArgumentException if [uploadIds] is empty.
   */
  suspend fun backfill(
    dataProvider: String,
    cmmsModelLine: String,
    uploadIds: List<String>,
  ): BackfillResult {
    require(uploadIds.isNotEmpty()) { "at least one upload id is required" }

    val created = mutableListOf<String>()
    val reactivated = mutableListOf<String>()
    for (uploadId in uploadIds) {
      val uploadName = "$dataProvider/rawImpressionUploads/$uploadId"

      val existing = modelLinesStub.findModelLine(uploadName, cmmsModelLine)
      if (existing != null) {
        logger.info(
          "$cmmsModelLine already present under $uploadName (${existing.name}); skipping."
        )
      } else {
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

      // Reactivate the parent so the Monitor dispatches the new (CREATED) model line.
      uploadsStub.markRawImpressionUploadActive(
        markRawImpressionUploadActiveRequest { name = uploadName }
      )
      reactivated.add(uploadName)
      logger.info("Reactivated $uploadName.")
    }
    return BackfillResult(created, reactivated)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(ModelLineBackfiller::class.java.name)

    /** Deterministic Create request id so a re-run of the backfill is idempotent. */
    private fun backfillRequestId(uploadName: String, cmmsModelLine: String): String =
      UUID.nameUUIDFromBytes("backfill:$uploadName:$cmmsModelLine".toByteArray()).toString()
  }
}
