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

package org.wfanet.measurement.edpaggregator.vidlabeler

import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt.entityKey
import org.wfanet.measurement.storage.ParquetValue

/**
 * Builds a raw-impression row's [LabeledImpression.EntityKey]s from dedicated per-impression
 * Parquet columns, using the model line's entity-key mappings (`entity_type` -> column name).
 *
 * Two mappings with different enforcement:
 * * [requiredFieldMapping]: every impression MUST have a non-null, non-empty value in each column —
 *   a null/empty cell is a producer bug and throws. Lets an EDP mark an entity type (e.g. person)
 *   as always-attributable so a missing id fails loud instead of silently dropping the impression
 *   from that entity's downstream measurement.
 * * [optionalFieldMapping]: an impression MAY have a value; a NULL/`KIND_NOT_SET` cell — or an
 *   empty string — means the impression is not associated with that `entity_type`, and is skipped.
 *
 * Each mapped column holds one entity id (a scalar STRING column, since a Parquet row cannot
 * represent a repeated column). A non-null, non-string cell is a producer bug and throws. The two
 * mappings' keys MUST be disjoint (validated at construction), and at least one entity key is
 * required per impression, so [map] throws if a row resolves to none (propagating out of
 * `processBatch`, NACK-ing the WorkItem for retry / dead-lettering).
 *
 * @property requiredFieldMapping `entity_type` -> column for entity types that must be populated.
 * @property optionalFieldMapping `entity_type` -> column for entity types that may be absent.
 */
class EntityKeyMapper(
  private val requiredFieldMapping: Map<String, String>,
  private val optionalFieldMapping: Map<String, String>,
) {
  init {
    val overlap = requiredFieldMapping.keys intersect optionalFieldMapping.keys
    require(overlap.isEmpty()) {
      "entity_type present in both required and optional entity-key mappings: $overlap"
    }
    require(requiredFieldMapping.isNotEmpty() || optionalFieldMapping.isNotEmpty()) {
      "at least one entity-key column mapping (required or optional) must be configured"
    }
  }

  /** Builds this row's entity keys from the mapped string columns. */
  fun map(row: Map<String, ParquetValue>): List<LabeledImpression.EntityKey> {
    // One pre-sized list filled required-then-optional (same order as the previous
    // `required + optional` concatenation), instead of two intermediate lists plus a concat.
    val entityKeys =
      ArrayList<LabeledImpression.EntityKey>(requiredFieldMapping.size + optionalFieldMapping.size)
    for ((entityType, column) in requiredFieldMapping) {
      val id = readStringOrNull(row, column, entityType)
      require(!id.isNullOrEmpty()) {
        "required entity-key column '$column' (entity_type '$entityType') is null/empty; every " +
          "impression must have a non-empty value for required entity types"
      }
      entityKeys.add(
        entityKey {
          this.entityType = entityType
          this.entityId = id
        }
      )
    }
    for ((entityType, column) in optionalFieldMapping) {
      // A NULL / unset / empty-string cell means the impression is not associated with this
      // entity_type (mirrors ResultsFulfiller.buildEventGroupEntityKeyMap dropping empty ids).
      val id = readStringOrNull(row, column, entityType)?.takeIf { it.isNotEmpty() } ?: continue
      entityKeys.add(
        entityKey {
          this.entityType = entityType
          this.entityId = id
        }
      )
    }
    require(entityKeys.isNotEmpty()) {
      "all entity-key columns are null/empty for this impression; at least one entity key is " +
        "required (required columns: ${requiredFieldMapping.values}, optional columns: " +
        "${optionalFieldMapping.values})"
    }
    return entityKeys
  }

  /** Reads [column] as a string; null when the cell is unset; errors on a non-string cell. */
  private fun readStringOrNull(
    row: Map<String, ParquetValue>,
    column: String,
    entityType: String,
  ): String? =
    when (val value = row[column]) {
      null -> null
      else ->
        when (value.kindCase) {
          ParquetValue.KindCase.KIND_NOT_SET -> null
          ParquetValue.KindCase.STRING_VALUE -> value.stringValue
          else ->
            error(
              "entity-key column '$column' (entity_type '$entityType') must be a STRING column; " +
                "got ${value.kindCase}"
            )
        }
    }
}
