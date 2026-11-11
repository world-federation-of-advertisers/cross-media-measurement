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

package org.wfanet.measurement.edpaggregator.vidlabeling.healing

import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingHealingControllerServiceGrpcKt.VidLabelingHealingControllerServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.purgeRawImpressionUploadCorrectionCandidatesRequest

/** Drains expired candidates through the protected controller machine API. */
class SystemApiCorrectionCandidateCleaner(
  private val machineStub: VidLabelingHealingControllerServiceCoroutineStub,
  private val rpcThrottlers: VidLabelingRpcThrottlers,
  private val pageSize: Int = 100,
) : CorrectionCandidateCleaner {
  init {
    require(pageSize in 1..1000) { "pageSize must be between 1 and 1000" }
  }

  override suspend fun purge(dataProvider: String) {
    var count: Int
    do {
      count =
        rpcThrottlers.metadataWrite.onReady {
          machineStub
            .purgeRawImpressionUploadCorrectionCandidates(
              purgeRawImpressionUploadCorrectionCandidatesRequest {
                parent = dataProvider
                this.pageSize = this@SystemApiCorrectionCandidateCleaner.pageSize
              }
            )
            .purgedCount
        }
      check(count in 0..pageSize) { "Cleanup returned an invalid page count" }
    } while (count == pageSize)
  }
}
