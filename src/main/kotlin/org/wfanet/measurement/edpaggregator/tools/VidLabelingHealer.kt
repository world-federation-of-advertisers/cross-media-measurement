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

import java.util.logging.Logger
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineFailedRequest

/**
 * Control-plane recovery actions for the VID labeling pipeline, invoked by the operator via
 * [VidLabelingHeal].
 *
 * These operate purely over the EDP-Aggregator public API (Get/List/Mark RPCs); they do not touch
 * encrypted rank-index blobs (the TEE-backed `heal-rank-index` / `evict-uploads` recovery paths are
 * added separately). Marking is deliberately reserved for cases that need operator judgment — the
 * `VidLabelingMonitorFunction` owns automated recovery (stuck-phase advancement, dispatch
 * sequencing).
 *
 * @param modelLinesStub stub for the `RawImpressionUploadModelLineService` public API.
 */
class VidLabelingHealer(
  private val modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub
) {
  /**
   * Force-marks the `(upload, model line)` `FAILED`, recording [reason] as its `error_message`.
   *
   * This is the recovery action for a stuck/hanging dispatch that the Monitor only alerts on (a
   * hung TEE processor, or an upload stale beyond its SLA). Marking the model line terminal lets
   * the Monitor's serial-dispatch guard release the next queued upload for the same `(DataProvider,
   * model line)`; the parent `RawImpressionUpload` state cascades from this child mark.
   *
   * Idempotent: a model line already in `FAILED` is left unchanged.
   *
   * @param dataProvider the `DataProvider` resource name (e.g. `dataProviders/{data_provider}`).
   * @param uploadId the `RawImpressionUpload` id segment.
   * @param cmmsModelLine the CMMS `ModelLine` resource name identifying the model line to fail.
   * @param reason the operator diagnosis recorded as `error_message`.
   * @return the `FAILED` [RawImpressionUploadModelLine].
   * @throws IllegalArgumentException if no matching model line exists under the upload.
   */
  suspend fun markFailed(
    dataProvider: String,
    uploadId: String,
    cmmsModelLine: String,
    reason: String,
  ): RawImpressionUploadModelLine {
    val uploadName = "$dataProvider/rawImpressionUploads/$uploadId"
    val modelLine =
      modelLinesStub.findModelLine(uploadName, cmmsModelLine)
        ?: throw IllegalArgumentException(
          "No RawImpressionUploadModelLine for $cmmsModelLine under $uploadName"
        )

    if (modelLine.state == RawImpressionUploadModelLine.State.FAILED) {
      logger.info("${modelLine.name} is already FAILED; nothing to do.")
      return modelLine
    }

    return modelLinesStub.markRawImpressionUploadModelLineFailed(
      markRawImpressionUploadModelLineFailedRequest {
        name = modelLine.name
        errorMessage = reason
        etag = modelLine.etag
      }
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(VidLabelingHealer::class.java.name)
  }
}
