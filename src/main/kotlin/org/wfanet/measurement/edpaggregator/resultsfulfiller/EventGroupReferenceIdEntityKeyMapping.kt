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

/**
 * Maps equivalent event-group reference IDs and entity IDs for one entity type.
 *
 * @property entityType entity type whose IDs correspond to event-group reference IDs.
 * @property eventGroupReferenceIdPrefix prefix on the reference-ID representation.
 * @property entityIdPrefix prefix on the entity-ID representation.
 */
data class EventGroupReferenceIdEntityKeyMapping(
  val entityType: String,
  val eventGroupReferenceIdPrefix: String,
  val entityIdPrefix: String,
) {
  init {
    require(entityType.isNotBlank()) { "entityType must not be blank" }
  }

  /**
   * Returns the event-group reference ID equivalent to [entityId], or `null` if unmappable.
   *
   * @param entityType type of [entityId].
   * @param entityId ID to convert.
   */
  fun toEventGroupReferenceId(entityType: String, entityId: String): String? {
    if (entityType != this.entityType) {
      return null
    }
    return replacePrefix(entityId, entityIdPrefix, eventGroupReferenceIdPrefix)
  }

  /**
   * Returns the entity ID equivalent to [eventGroupReferenceId], or `null` if unmappable.
   *
   * @param eventGroupReferenceId reference ID to convert.
   */
  fun toEntityId(eventGroupReferenceId: String): String? {
    return replacePrefix(eventGroupReferenceId, eventGroupReferenceIdPrefix, entityIdPrefix)
  }

  private fun replacePrefix(value: String, sourcePrefix: String, targetPrefix: String): String? {
    if (!value.startsWith(sourcePrefix)) {
      return null
    }
    return (targetPrefix + value.removePrefix(sourcePrefix)).takeIf { it.isNotBlank() }
  }
}
