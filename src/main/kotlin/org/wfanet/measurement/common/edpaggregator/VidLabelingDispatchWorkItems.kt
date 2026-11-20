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

import com.google.protobuf.Any
import java.security.MessageDigest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.UploadHealingOperationKey
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams

/** Dispatch payload invariants and identity shared by intake and execution. */
object VidLabelingDispatchWorkItems {
  const val QUEUE = "vid-labeling-dispatch-queue"

  fun normalize(params: VidLabelingDispatchWorkItemParams): VidLabelingDispatchWorkItemParams {
    val provider =
      requireNotNull(DataProviderKey.fromName(params.dataProvider)) {
        "data_provider must identify a DataProvider"
      }
    if (!params.hasRecovery()) return params
    val recovery = params.recovery
    require(recovery.hasSource()) { "Missing recovery source" }
    validateSource(recovery.source)
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
    return params
  }

  fun source(
    params: VidLabelingDispatchWorkItemParams,
    ordinarySource: WorkItem.WorkItemParams.DataPathParams?,
  ): WorkItem.WorkItemParams.DataPathParams {
    val selected =
      if (params.hasRecovery()) {
        require(ordinarySource == null) { "Recovery source must be application-owned" }
        require(params.recovery.hasSource()) { "Missing recovery source" }
        params.recovery.source
      } else requireNotNull(ordinarySource) { "Missing ordinary source" }
    validateSource(selected)
    return selected
  }

  private fun validateSource(source: WorkItem.WorkItemParams.DataPathParams) {
    require(
      source.dataPath.isNotEmpty() &&
        source.hasGeneration() &&
        source.generation > 0 &&
        source.hasCreateTime() &&
        source.eventType == WorkItem.WorkItemParams.DataPathParams.StorageEventType.FINALIZED
    ) {
      "Dispatch source must identify a finalized object version"
    }
  }

  /** Identity includes one authoritative source, provider and optional recovery references. */
  fun intentId(
    params: VidLabelingDispatchWorkItemParams,
    ordinarySource: WorkItem.WorkItemParams.DataPathParams? = null,
  ): String {
    val normalized = normalize(params)
    val selected = source(normalized, ordinarySource)
    val identity = workItemParams {
      appParams = Any.pack(normalized)
      if (!normalized.hasRecovery()) dataPathParams = selected
    }
    return MessageDigest.getInstance("SHA-256").digest(identity.toByteArray()).joinToString("") {
      (it.toInt() and 0xff).toString(16).padStart(2, '0')
    }
  }
}
