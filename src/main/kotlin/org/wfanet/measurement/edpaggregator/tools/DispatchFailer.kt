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
 * Force-fails a stuck or hanging `RawImpressionUpload` — the operator recovery for a dispatch that
 * the Monitor only alerts on (a hung TEE processor, or an upload stale beyond its SLA).
 *
 * Marking is deliberately reserved for cases that need operator judgment; the
 * `VidLabelingMonitorFunction` owns automated recovery (stuck-phase advancement, dispatch
 * sequencing).
 *
 * @param modelLinesStub stub for the `RawImpressionUploadModelLineService` public API.
 */
class DispatchFailer(private val modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub) {
  /**
   * Marks every non-terminal child model line of [rawImpressionUpload] `FAILED`, recording [reason]
   * as its `error_message`.
   *
   * Only model lines in a stuck/in-progress state (`CREATED`, `POOL_ASSIGNING`, `RANKING`,
   * `LABELING`) are failed; `COMPLETED` (terminal, succeeded) and already-`FAILED` model lines are
   * left untouched — a successful model line is not "stuck" and must not be resurrected as FAILED.
   * The parent `RawImpressionUpload` is set to `FAILED` automatically by the child's state cascade,
   * which also unblocks the Monitor's serial-dispatch guard for queued uploads.
   *
   * @param rawImpressionUpload the `RawImpressionUpload` resource name
   *   (`dataProviders/{data_provider}/rawImpressionUploads/{raw_impression_upload}`).
   * @param reason the operator diagnosis recorded as `error_message`.
   * @return the resource names of the model lines that were marked `FAILED`.
   */
  suspend fun failUpload(rawImpressionUpload: String, reason: String): List<String> {
    val failed = mutableListOf<String>()
    for (modelLine in modelLinesStub.listModelLines(rawImpressionUpload)) {
      if (modelLine.state !in STUCK_STATES) {
        logger.info("${modelLine.name} is ${modelLine.state} (not stuck); leaving as-is.")
        continue
      }
      // TODO(world-federation-of-advertisers/cross-media-measurement#4211): once #4211 makes
      // request_id REQUIRED on the Mark* RPCs, set
      // requestId = RequestIds.forMarkRawImpressionUploadModelLineFailed(modelLine.name) so an
      // operator re-run hits the AIP-155 replay short-circuit instead of failing INVALID_ARGUMENT.
      modelLinesStub.markRawImpressionUploadModelLineFailed(
        markRawImpressionUploadModelLineFailedRequest {
          name = modelLine.name
          errorMessage = reason
          etag = modelLine.etag
        }
      )
      failed.add(modelLine.name)
      logger.info("Marked ${modelLine.name} FAILED.")
    }
    return failed
  }

  companion object {
    private val logger: Logger = Logger.getLogger(DispatchFailer::class.java.name)

    /** Non-terminal model-line states that a stuck/hanging dispatch can be in. */
    private val STUCK_STATES =
      setOf(
        RawImpressionUploadModelLine.State.CREATED,
        RawImpressionUploadModelLine.State.POOL_ASSIGNING,
        RawImpressionUploadModelLine.State.RANKING,
        RawImpressionUploadModelLine.State.LABELING,
      )
  }
}
