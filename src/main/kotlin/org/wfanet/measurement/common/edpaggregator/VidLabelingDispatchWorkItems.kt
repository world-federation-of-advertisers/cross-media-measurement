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

package org.wfanet.measurement.common.edpaggregator

import java.security.MessageDigest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.UploadHealingOperationKey
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParams
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem

/** Dispatch payload invariants and identity shared by intake and execution. */
object VidLabelingDispatchWorkItems {
  const val QUEUE = "vid-labeling-dispatch-queue"

  fun normalize(params: VidLabelingDispatchWorkItemParams): VidLabelingDispatchWorkItemParams {
    val provider =
      requireNotNull(DataProviderKey.fromName(params.dataProvider)) {
        "data_provider must identify a DataProvider"
      }
    require(params.hasSource()) { "Missing dispatch source" }
    require(
      params.source.dataPath.isNotEmpty() &&
        params.source.hasGeneration() &&
        params.source.generation > 0 &&
        params.source.hasCreateTime() &&
        params.source.eventType == WorkItem.WorkItemParams.DataPathParams.StorageEventType.FINALIZED
    ) {
      "Dispatch source must identify a finalized object version"
    }
    if (!params.hasRecovery()) return params
    val recovery = params.recovery
    require(recovery.overrideModelLinesCount > 0) { "Recovery requires model lines" }
    require(recovery.overrideModelLinesList.all { ModelLineKey.fromName(it) != null }) {
      "Recovery model lines must identify CMMS ModelLines"
    }
    require(recovery.overrideModelLinesList.distinct().size == recovery.overrideModelLinesCount) {
      "Recovery model lines must not contain duplicates"
    }
    val sourceUpload =
      requireNotNull(RawImpressionUploadKey.fromName(recovery.sourceUpload)) {
        "Recovery source must identify a RawImpressionUpload"
      }
    require(sourceUpload.parentKey == provider) { "Recovery source must belong to data_provider" }
    val operation =
      requireNotNull(UploadHealingOperationKey.fromName(recovery.uploadHealingOperation)) {
        "Recovery operation must identify an UploadHealingOperation"
      }
    require(operation.parentKey == provider) { "Recovery operation must belong to data_provider" }
    return params.copy {
      this.recovery =
        recovery.copy {
          overrideModelLines.clear()
          overrideModelLines += recovery.overrideModelLinesList.sorted()
        }
    }
  }

  /** Separates approved replay from ordinary dispatch of the same storage generation. */
  fun intentId(params: VidLabelingDispatchWorkItemParams): String =
    MessageDigest.getInstance("SHA-256").digest(normalize(params).toByteArray()).joinToString("") {
      (it.toInt() and 0xff).toString(16).padStart(2, '0')
    }
}
