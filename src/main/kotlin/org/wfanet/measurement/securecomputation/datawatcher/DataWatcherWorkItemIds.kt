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

package org.wfanet.measurement.securecomputation.datawatcher

import java.security.MessageDigest
import org.wfanet.measurement.common.edpaggregator.VidLabelingDispatchWorkItems
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams.DataPathParams

/** Deterministic identity of work admitted by a watched-path configuration. */
object DataWatcherWorkItemIds {
  fun idempotencyKey(
    identifier: String,
    source: DataPathParams,
    dispatchParams: VidLabelingDispatchWorkItemParams? = null,
  ): String {
    val sourceKey = "$identifier\u0000${source.dataPath}\u0000${source.generation}"
    if (dispatchParams == null) return sourceKey
    val intent =
      VidLabelingDispatchWorkItems.intentId(
        dispatchParams,
        source.takeUnless { dispatchParams.hasRecovery() },
      )
    return "$sourceKey\u0000$intent"
  }

  fun workItemId(idempotencyKey: String): String {
    val digest =
      MessageDigest.getInstance("SHA-256").digest(idempotencyKey.toByteArray(Charsets.UTF_8))
    val hex =
      digest.joinToString(separator = "") { byte ->
        (byte.toInt() and 0xff).toString(16).padStart(2, '0')
      }
    return "dw-${hex.take(60)}"
  }
}
