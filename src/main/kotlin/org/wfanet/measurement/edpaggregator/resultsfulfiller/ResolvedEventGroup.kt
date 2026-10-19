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

/** Native and equivalent selectors for one requisition event group. */
data class EventGroupSelector(
  val eventGroupReferenceId: String?,
  val entityKey: LabeledImpression.EntityKey?,
) {
  init {
    require(
      eventGroupReferenceId?.isNotBlank() == true || entityKey?.entityId?.isNotBlank() == true
    ) {
      "At least one event-group selector must be present"
    }
  }
}

/** A requisition event group normalized for both storage lookup and event filtering. */
data class ResolvedEventGroup(
  val key: EventGroupKey,
  val collectionIntervals: List<Interval>,
  val selector: EventGroupSelector,
)

/** Normalized event groups and the entity type that can identify a whole blob. */
data class ResolvedEventGroups(
  val eventGroups: List<ResolvedEventGroup>,
  val mappedEntityType: String?,
) {
  val byKey: Map<EventGroupKey, ResolvedEventGroup> = eventGroups.associateBy { it.key }

  init {
    require(byKey.size == eventGroups.size) { "Event-group keys must be unique" }
  }

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

      return ResolvedEventGroups(
        eventGroups =
          entries.map { entry ->
            val key: EventGroupKey =
              requireNotNull(EventGroupKey.fromName(entry.eventGroup)) {
                "Invalid event-group resource name: ${entry.eventGroup}"
              }
            ResolvedEventGroup(
              key = key,
              collectionIntervals = entry.details.collectionIntervalsList,
              selector = resolveSelector(entry.details, mapping),
            )
          },
        mappedEntityType = mapping?.entityType,
      )
    }

    private fun resolveSelector(
      details: GroupedRequisitions.EventGroupDetails,
      mapping: EventGroupReferenceIdEntityKeyMapping?,
    ): EventGroupSelector {
      val nativeEntityKey: LabeledImpression.EntityKey? =
        if (detailsHasEntityKey(details)) {
          val key: GroupedRequisitions.EventGroupDetails.EntityKey = details.entityKey
          LabeledImpressionKt.entityKey {
            entityType = key.entityType
            entityId = key.entityId
          }
        } else {
          null
        }
      val nativeReferenceId: String? =
        if (nativeEntityKey == null) {
          details.eventGroupReferenceId.takeIf { it.isNotBlank() }
        } else {
          null
        }
      val referenceId: String? =
        nativeReferenceId
          ?: if (mapping == null || nativeEntityKey == null) {
            null
          } else {
            mapping.toEventGroupReferenceId(nativeEntityKey.entityType, nativeEntityKey.entityId)
          }
      val entityKey: LabeledImpression.EntityKey? =
        nativeEntityKey
          ?: if (mapping == null || nativeReferenceId == null) {
            null
          } else {
            mapping.toEntityId(nativeReferenceId)?.let { entityId ->
              LabeledImpressionKt.entityKey {
                entityType = mapping.entityType
                this.entityId = entityId
              }
            }
          }
      return EventGroupSelector(eventGroupReferenceId = referenceId, entityKey = entityKey)
    }

    private fun detailsHasEntityKey(details: GroupedRequisitions.EventGroupDetails): Boolean {
      return details.hasEntityKey() && details.entityKey.entityId.isNotBlank()
    }
  }
}
