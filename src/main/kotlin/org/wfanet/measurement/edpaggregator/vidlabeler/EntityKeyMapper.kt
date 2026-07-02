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
 * Parquet columns, using the model line's `entity_key_field_mapping` (`entity_type` -> column
 * name).
 *
 * This replaces the earlier file-level footer attribution ("Option Y"): entity keys are now read
 * per row so an impression carries only the entities it is actually associated with, restoring the
 * per-impression granularity the ResultsFulfiller already supports.
 *
 * Each mapped column holds one entity id (a scalar STRING column, since a Parquet row cannot
 * represent a repeated column). A `NULL`/`KIND_NOT_SET` cell — or an empty string — means the
 * impression is not associated with that `entity_type`. A non-null, non-string cell is a producer
 * bug and throws. Every mapped column being unset for a row is also a producer bug: at least one
 * entity key is required, so [map] throws (propagating out of `processBatch`, NACK-ing the WorkItem
 * for retry / dead-lettering).
 *
 * @property entityKeyFieldMapping `entity_type` -> raw-impression column name.
 */
class EntityKeyMapper(private val entityKeyFieldMapping: Map<String, String>) {
  /** Builds this row's entity keys from the mapped string columns. */
  fun map(row: Map<String, ParquetValue>): List<LabeledImpression.EntityKey> {
    val entityKeys =
      entityKeyFieldMapping.mapNotNull { (entityType, column) ->
        val value = row[column]
        when (value?.kindCase) {
          // A NULL / unset cell means the impression is not associated with this entity_type.
          null,
          ParquetValue.KindCase.KIND_NOT_SET -> null
          ParquetValue.KindCase.STRING_VALUE ->
            // Empty string is treated as unset (mirrors the downstream sentinel rule in
            // ResultsFulfiller.buildEventGroupEntityKeyMap, which drops empty entity_id).
            value.stringValue
              .takeIf { it.isNotEmpty() }
              ?.let { id ->
                entityKey {
                  this.entityType = entityType
                  this.entityId = id
                }
              }
          else ->
            error(
              "entity-key column '$column' (entity_type '$entityType') must be a STRING column; " +
                "got ${value.kindCase}"
            )
        }
      }
    require(entityKeys.isNotEmpty()) {
      "all entity-key columns are null/empty for this impression; at least one entity key is " +
        "required (columns: ${entityKeyFieldMapping.values})"
    }
    return entityKeys
  }
}
