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

package org.wfanet.measurement.edpaggregator

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

  /** Returns the equivalent reference ID, throwing [IllegalArgumentException] if unmappable. */
  fun requireEventGroupReferenceId(entityType: String, entityId: String): String {
    validateEntityId(entityType, entityId)
    return eventGroupReferenceIdPrefix + entityId.substring(entityIdPrefix.length)
  }

  /** Returns the equivalent entity ID, throwing [IllegalArgumentException] if unmappable. */
  fun requireEntityId(eventGroupReferenceId: String): String {
    validateEventGroupReferenceId(eventGroupReferenceId)
    return entityIdPrefix + eventGroupReferenceId.substring(eventGroupReferenceIdPrefix.length)
  }

  /**
   * Validates [entityId] without constructing its equivalent reference ID.
   *
   * @throws IllegalArgumentException if the ID is blank or unmappable.
   */
  fun validateEntityId(entityType: String, entityId: String) {
    require(entityId.isNotBlank()) {
      "Entity ID must not be blank under event_group_reference_id_entity_key_mapping"
    }
    require(
      entityType == this.entityType &&
        canReplacePrefix(entityId, entityIdPrefix, eventGroupReferenceIdPrefix)
    ) {
      "Entity ID $entityId of type $entityType cannot be converted to a non-blank reference ID " +
        "by event_group_reference_id_entity_key_mapping (entity_id_prefix: $entityIdPrefix)"
    }
  }

  /**
   * Validates [eventGroupReferenceId] without constructing its equivalent entity ID.
   *
   * @throws IllegalArgumentException if the ID is blank or unmappable.
   */
  fun validateEventGroupReferenceId(eventGroupReferenceId: String) {
    require(eventGroupReferenceId.isNotBlank()) {
      "Event-group reference ID must not be blank under event_group_reference_id_entity_key_mapping"
    }
    require(canReplacePrefix(eventGroupReferenceId, eventGroupReferenceIdPrefix, entityIdPrefix)) {
      "Event-group reference ID $eventGroupReferenceId cannot be converted to a non-blank " +
        "entity ID by event_group_reference_id_entity_key_mapping " +
        "(event_group_reference_id_prefix: $eventGroupReferenceIdPrefix)"
    }
  }

  private fun canReplacePrefix(value: String, sourcePrefix: String, targetPrefix: String): Boolean {
    if (sourcePrefix.isEmpty()) {
      // Removing an empty prefix cannot make an already non-blank ID blank.
      return true
    }
    if (!value.startsWith(sourcePrefix)) {
      return false
    }
    if (targetPrefix.isNotBlank()) {
      return true
    }
    for (index in sourcePrefix.length until value.length) {
      if (!value[index].isWhitespace()) {
        return true
      }
    }
    return false
  }

  private fun replacePrefix(value: String, sourcePrefix: String, targetPrefix: String): String? {
    if (!value.startsWith(sourcePrefix)) {
      return null
    }
    return (targetPrefix + value.removePrefix(sourcePrefix)).takeIf { it.isNotBlank() }
  }
}
