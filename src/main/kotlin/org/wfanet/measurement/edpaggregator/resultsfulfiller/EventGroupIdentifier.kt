/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression

/** Selectors from an [EventBatch]'s blob metadata, normalized across storage formats. */
data class EventGroupIdentifier(
  /** Reference ID that identifies the whole blob, or `null` when unavailable or ambiguous. */
  val eventGroupReferenceId: String?,
  /** Entity keys summarized by the blob metadata. */
  val entityKeys: Set<LabeledImpression.EntityKey>,
  /** IDs for exhaustive entity types. All IDs of one type must match to identify the whole blob. */
  val wholeBlobEntityIdsByType: Map<String, Set<String>>,
) {
  init {
    val entityKeyPairs: Set<Pair<String, String>> =
      entityKeys.map { Pair(it.entityType, it.entityId) }.toSet()
    require(
      wholeBlobEntityIdsByType.all { (entityType, entityIds) ->
        entityIds.isNotEmpty() && entityIds.all { Pair(entityType, it) in entityKeyPairs }
      }
    ) {
      "Whole-blob entity IDs must be non-empty and present in the blob's entity keys"
    }
  }
}
