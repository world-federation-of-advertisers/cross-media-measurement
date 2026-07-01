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

import com.google.gson.JsonParser
import java.time.LocalDate
import java.time.format.DateTimeParseException
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt.entityKey

/**
 * Entity keys (and the legacy event group reference id) for a single raw-impression file, read from
 * the file's plaintext Parquet footer key-value metadata ("Option Y").
 *
 * A raw-impression file is exactly one `EventGroup`, so it carries one [eventGroupReferenceId] and
 * the list of that group's [entityKeys]. The producer (EDP uploader / cloud-test data writer)
 * writes these into the footer; Phase-2 reads them directly when it opens each file, so entity keys
 * never travel through WorkItem params or require a dispatcher-side `EventGroup` resolution.
 *
 * @property eventGroupReferenceId reference id of the EventGroup that produced the file; still
 *   REQUIRED on output (`LabeledImpression.event_group_reference_id`).
 * @property entityKeys entity keys of that EventGroup; at least one, propagated to each
 *   `LabeledImpression.entity_keys` and aggregated into `BlobDetails.entity_keys`.
 * @property maxEventDate UTC date of the file's most-recent impression, used to place the labeled
 *   output under `model-line/<id>/<YYYY-MM-DD>/` so `DataAvailabilitySync` can classify it by date.
 */
data class FileEntityKeys(
  val eventGroupReferenceId: String,
  val entityKeys: List<LabeledImpression.EntityKey>,
  val maxEventDate: LocalDate,
) {
  companion object {
    /** Footer key holding the file's event-group reference id (a plain string). */
    const val EVENT_GROUP_REFERENCE_ID_KEY = "event_group_reference_id"

    /**
     * Footer key holding the file's entity keys as a JSON array, each element `{"entity_type":
     * <string>, "entity_id": <string>}` mirroring [LabeledImpression.EntityKey].
     */
    const val ENTITY_KEYS_KEY = "entity_keys"

    /** Footer key holding the file's most-recent impression date as an ISO `YYYY-MM-DD` (UTC). */
    const val MAX_EVENT_DATE_KEY = "max_event_date"

    private const val ENTITY_TYPE_FIELD = "entity_type"
    private const val ENTITY_ID_FIELD = "entity_id"

    /**
     * Parses [FileEntityKeys] from a raw-impression file's footer [metadata].
     *
     * Fails loudly if the entity-key attribution is missing or empty: every labeled impression must
     * be attributable to at least one entity and to an event group, so an unattributed input file
     * is a producer bug, not a per-row condition.
     */
    fun fromFooterMetadata(metadata: Map<String, String>): FileEntityKeys {
      val eventGroupReferenceId =
        requireNotNull(metadata[EVENT_GROUP_REFERENCE_ID_KEY]?.takeIf { it.isNotEmpty() }) {
          "raw-impression footer is missing the '$EVENT_GROUP_REFERENCE_ID_KEY' metadata entry; " +
            "the producer must write each file's event group reference id into its plaintext footer"
        }
      val entityKeysJson =
        requireNotNull(metadata[ENTITY_KEYS_KEY]) {
          "raw-impression footer is missing the '$ENTITY_KEYS_KEY' metadata entry; the producer " +
            "must write each file's entity keys into its plaintext footer"
        }
      val entityKeys =
        JsonParser.parseString(entityKeysJson).asJsonArray.map { element ->
          val obj = element.asJsonObject
          entityKey {
            entityType =
              requireNotNull(obj.get(ENTITY_TYPE_FIELD)?.asString) {
                "entity key is missing '$ENTITY_TYPE_FIELD' in footer '$ENTITY_KEYS_KEY': " +
                  entityKeysJson
              }
            entityId =
              requireNotNull(obj.get(ENTITY_ID_FIELD)?.asString) {
                "entity key is missing '$ENTITY_ID_FIELD' in footer '$ENTITY_KEYS_KEY': " +
                  entityKeysJson
              }
          }
        }
      require(entityKeys.isNotEmpty()) {
        "raw-impression footer '$ENTITY_KEYS_KEY' must list at least one entity key"
      }
      val maxEventDateString =
        requireNotNull(metadata[MAX_EVENT_DATE_KEY]?.takeIf { it.isNotEmpty() }) {
          "raw-impression footer is missing the '$MAX_EVENT_DATE_KEY' metadata entry; the producer " +
            "must write each file's most-recent impression date (ISO YYYY-MM-DD, UTC) into its " +
            "plaintext footer"
        }
      val maxEventDate =
        try {
          LocalDate.parse(maxEventDateString)
        } catch (e: DateTimeParseException) {
          throw IllegalArgumentException(
            "raw-impression footer '$MAX_EVENT_DATE_KEY' is not an ISO YYYY-MM-DD date: " +
              maxEventDateString,
            e,
          )
        }
      return FileEntityKeys(eventGroupReferenceId, entityKeys, maxEventDate)
    }
  }
}
