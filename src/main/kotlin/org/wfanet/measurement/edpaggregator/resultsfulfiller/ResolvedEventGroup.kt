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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.type.Interval
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt

/** A requisition event group normalized for both storage lookup and event filtering. */
data class ResolvedEventGroup(
  val collectionIntervals: List<Interval>,
  val eventGroupReferenceId: String?,
  val entityKey: LabeledImpression.EntityKey?,
) {
  init {
    require(
      eventGroupReferenceId?.isNotBlank() == true || entityKey?.entityId?.isNotBlank() == true
    ) {
      "Event-group reference ID or entity key must be present"
    }
  }
}

/** Normalized event groups and the entity type that can identify a whole blob. */
data class ResolvedEventGroups(
  val eventGroupsByKey: Map<EventGroupKey, ResolvedEventGroup>,
  val mappedEntityType: String?,
) {
  companion object {
    /** Resolves native and equivalent selectors once for all downstream consumers. */
    fun fromEventGroupMapEntries(
      entries: List<GroupedRequisitions.EventGroupMapEntry>,
      mapping: EventGroupReferenceIdEntityKeyMapping?,
    ): ResolvedEventGroups {
      val hasEntityKeys: List<Boolean> = entries.map { detailsHasEntityKey(it.details) }
      if (mapping == null && hasEntityKeys.any { it }) {
        require(hasEntityKeys.all { it }) {
          "Cannot mix entity-key and event-group-reference-ID selectors without a mapping"
        }
      }

      val eventGroupsByKey: Map<EventGroupKey, ResolvedEventGroup> =
        entries.associate { entry ->
          val key: EventGroupKey =
            requireNotNull(EventGroupKey.fromName(entry.eventGroup)) {
              "Invalid event-group resource name: ${entry.eventGroup}"
            }
          key to resolveEventGroup(entry.details, mapping)
        }
      require(eventGroupsByKey.size == entries.size) { "Event-group keys must be unique" }
      return ResolvedEventGroups(eventGroupsByKey, mapping?.entityType)
    }

    private fun resolveEventGroup(
      details: GroupedRequisitions.EventGroupDetails,
      mapping: EventGroupReferenceIdEntityKeyMapping?,
    ): ResolvedEventGroup {
      if (detailsHasEntityKey(details)) {
        val detailsEntityKey: GroupedRequisitions.EventGroupDetails.EntityKey = details.entityKey
        val entityKey: LabeledImpression.EntityKey =
          LabeledImpressionKt.entityKey {
            entityType = detailsEntityKey.entityType
            entityId = detailsEntityKey.entityId
          }
        return ResolvedEventGroup(
          collectionIntervals = details.collectionIntervalsList,
          eventGroupReferenceId =
            mapping?.toEventGroupReferenceId(entityKey.entityType, entityKey.entityId),
          entityKey = entityKey,
        )
      }

      val referenceId: String? = details.eventGroupReferenceId.takeIf { it.isNotBlank() }
      val entityKey: LabeledImpression.EntityKey? =
        if (mapping == null || referenceId == null) {
          null
        } else {
          mapping.toEntityId(referenceId)?.let { entityId ->
            LabeledImpressionKt.entityKey {
              entityType = mapping.entityType
              this.entityId = entityId
            }
          }
        }
      return ResolvedEventGroup(
        collectionIntervals = details.collectionIntervalsList,
        eventGroupReferenceId = referenceId,
        entityKey = entityKey,
      )
    }

    private fun detailsHasEntityKey(details: GroupedRequisitions.EventGroupDetails): Boolean {
      return details.hasEntityKey() && details.entityKey.entityId.isNotBlank()
    }
  }
}
