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

import org.wfanet.measurement.edpaggregator.v1alpha.EntityKeyGroup
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression

/** Selectors from an [EventBatch]'s blob metadata, normalized across storage formats. */
data class EventGroupIdentifier(
  /** Reference ID that identifies the whole blob, or `null` when unavailable or ambiguous. */
  val eventGroupReferenceId: String?,
  /** Entity keys summarized by the blob metadata. */
  val entityKeys: Set<LabeledImpression.EntityKey>,
  /** Canonical groups for exhaustive entity types. A complete group identifies the whole blob. */
  val wholeBlobEntityKeyGroups: Set<EntityKeyGroup>,
) {
  init {
    require(wholeBlobEntityKeyGroups.all { it.entityType.isNotBlank() }) {
      "Whole-blob entity types must not be blank"
    }
    require(
      wholeBlobEntityKeyGroups.map { it.entityType }.toSet().size == wholeBlobEntityKeyGroups.size
    ) {
      "Whole-blob entity types must be unique"
    }
    require(wholeBlobEntityKeyGroups.all { it.entityIdsList.isNotEmpty() }) {
      "Whole-blob entity-key groups must not be empty"
    }
    require(wholeBlobEntityKeyGroups.all { group -> group.entityIdsList.all { it.isNotBlank() } }) {
      "Whole-blob entity IDs must not be blank"
    }
    require(
      wholeBlobEntityKeyGroups.all { group ->
        group.entityIdsList.toSet().size == group.entityIdsCount
      }
    ) {
      "Whole-blob entity IDs must be unique within each entity type"
    }
    require(
      wholeBlobEntityKeyGroups.all { group -> group.entityIdsList == group.entityIdsList.sorted() }
    ) {
      "Whole-blob entity IDs must be in canonical order"
    }
    val entityKeyPairs: Set<Pair<String, String>> =
      entityKeys.map { Pair(it.entityType, it.entityId) }.toSet()
    require(
      wholeBlobEntityKeyGroups.all { group ->
        group.entityIdsList.all { Pair(group.entityType, it) in entityKeyPairs }
      }
    ) {
      "Whole-blob entity keys must be present in the blob's entity keys"
    }
  }
}
