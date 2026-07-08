/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.loadtest.edpaggregator.testing

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Message
import java.util.logging.Logger
import kotlinx.coroutines.flow.flow
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.wfanet.measurement.loadtest.dataprovider.EntityKeyedLabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent
import org.wfanet.measurement.storage.ParquetEncryptionConfig
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.parquetRow
import org.wfanet.measurement.storage.parquetValue

/**
 * Canonical raw-impression Parquet column names, shared by this writer and the model-line config
 * that tells the VID Labeling pipeline how to read them back
 * (`VidLabelingConfig.ModelLineConfig` field mappings). Keep the two in lockstep: the mappings name
 * these columns and the reader (`RawImpressionSource` + `ParquetImpressionConverter` +
 * `EntityKeyMapper`) fails loud at file open if a mapped column is missing or retyped.
 */
object RawImpressionColumns {
  /** Per-impression unique id; the `RawImpressionSource.eventIdColumn` and `LabelerInput.event_id.id`. */
  const val EVENT_ID = "event_id"

  /** Event time as epoch microseconds; feeds `LabelerInput.timestamp_usec` (INT64 column). */
  const val EVENT_TIME_USEC = "event_time_usec"

  /**
   * The `person` entity id: doubles as the required entity key (`entity_type = "person"`) and the
   * model-facing identity (`profile_info.proprietary_id_space_1_user_info.user_id`).
   */
  const val PERSON_ID = "person_id"

  /** Person gender as the event-template enum value name (e.g. "MALE" / "FEMALE"). */
  const val PERSON_GENDER = "person_gender"

  /** Person age group as the event-template enum value name (e.g. "YEARS_18_TO_34"). */
  const val PERSON_AGE_GROUP = "person_age_group"

  /** The entity_type used for the person entity key. */
  const val ENTITY_TYPE_PERSON = "person"

  /**
   * The `creative-id` entity id: the per-impression creative id, matching the event group's own
   * `EntityKey` (`entity_type = "creative-id"`). Used by entity-key event groups so the
   * results-fulfiller matches the pipelined impressions to the event group by its entity key.
   */
  const val CREATIVE_ID = "creative_id"

  /** The entity_type used for the creative-id entity key. */
  const val ENTITY_TYPE_CREATIVE_ID = "creative-id"
}

/**
 * Writes **raw (unlabeled) impressions** as PME-encrypted Parquet, in exactly the shape the VID
 * Labeling pipeline reads: one flat column per labeling input, plus a plaintext footer carrying the
 * file's event-group reference id and event date.
 *
 * This is the pipelined-EDP counterpart of [ImpressionsWriter] (which writes already-labeled
 * impressions for the direct/ResultsFulfiller path). It reuses the same synthetic-data types
 * ([EntityKeyedLabeledEventDateShard], [LabeledEvent], entity keys) so a caller can feed the very
 * same generated events down either path — here we simply drop the `vid` (the pipeline assigns it)
 * and project each event onto raw-impression columns.
 *
 * ## On-disk contract (mirrors the reader)
 * * **Columns** — [RawImpressionColumns.EVENT_ID] (STRING), [RawImpressionColumns.EVENT_TIME_USEC]
 *   (INT64), one STRING column per required entity key (default `person` -> `person_id`), and the
 *   per-event columns returned by `eventColumns` (e.g. demographics). One row per impression. The
 *   schema is derived from the first row, so every row MUST carry the same columns.
 * * **Footer** — plaintext key-value metadata `event_group_reference_id` and `event_date`
 *   (ISO `YYYY-MM-DD`, UTC), read back by
 *   `org.wfanet.measurement.edpaggregator.vidlabeler.FileEntityKeys.fromFooterMetadata`. Entity keys
 *   are NOT in the footer (they are per-row columns).
 * * **Encryption (PME)** — parquet-mr native Parquet Modular Encryption, bridged to the EDP's Tink
 *   [kmsClient] via [ParquetEncryptionConfig]. The data columns are encrypted with [kekUri]; the
 *   footer is left plaintext (`parquet.encryption.plaintext.footer = true`) so the reader can read
 *   the footer metadata and schema without keys, exactly as the deployed TEE reader expects.
 *
 * @property eventGroupReferenceId reference id written into every file's footer; one writer serves a
 *   single event group.
 * @property kekUri the EDP's KMS key URI (GCP for the pipelined EDP), used as the PME master key id.
 * @property kmsClient the WFA Tink [KmsClient] resolving [kekUri] to an AEAD (same client the reader
 *   uses to decrypt).
 * @property storageConfiguration Hadoop [Configuration] selecting the storage backend (`file://` by
 *   default; set `fs.gs.impl` etc. for `gs://`). Copied per write, so the PME registration never
 *   leaks across writes.
 * @property rootPath root against which blob keys resolve (a `gs://bucket/...` or local path).
 * @property requiredEntityKeyColumns entity_type -> column for entity keys every impression must
 *   carry; defaults to the single `person` -> `person_id` mapping.
 */
class RawImpressionsWriter(
  private val eventGroupReferenceId: String,
  private val kekUri: String,
  private val kmsClient: KmsClient,
  private val storageConfiguration: Configuration,
  private val rootPath: Path,
  private val requiredEntityKeyColumns: Map<String, String> =
    mapOf(RawImpressionColumns.ENTITY_TYPE_PERSON to RawImpressionColumns.PERSON_ID),
) {
  init {
    require(eventGroupReferenceId.isNotEmpty()) { "eventGroupReferenceId must be non-empty" }
    require(kekUri.isNotEmpty()) { "kekUri must be non-empty" }
    require(requiredEntityKeyColumns.isNotEmpty()) {
      "at least one required entity-key column mapping must be configured"
    }
  }

  /**
   * Writes one raw-impression Parquet file per date in [shards] (dates with no events are skipped),
   * under `<blobKeyPrefix>/<event_date>/<event_group_reference_id>`, and returns the blob keys written (in
   * date order) so the caller can register them as `RawImpressionUploadFile`s.
   *
   * @param shards synthetic events grouped by date and entity key (reuse [ImpressionsWriter]'s
   *   input); the `vid` on each [LabeledEvent] is intentionally ignored — the pipeline assigns it.
   * @param blobKeyPrefix path prefix under [rootPath] for this upload's files.
   * @param eventColumns projects an event message onto its raw-impression columns (e.g. demographic
   *   columns). MUST return the same column set for every event so the derived schema is stable.
   */
  suspend fun <T : Message> writeRawImpressions(
    shards: Sequence<EntityKeyedLabeledEventDateShard<T>>,
    blobKeyPrefix: String,
    eventColumns: (T) -> Map<String, ParquetValue>,
  ): List<String> {
    val writtenBlobKeys = mutableListOf<String>()
    for (shard in shards) {
      val eventDate = shard.localDate.toString()
      var index = 0
      val rows =
        shard.entityKeysWithLabeledEvents.flatMap { group ->
          val entityColumns: Map<String, ParquetValue> = entityKeyColumns(group.entityKeys)
          group.labeledEvents.map { event ->
            buildRow(eventId = "$eventGroupReferenceId-$eventDate-${index++}", event, entityColumns, eventColumns)
          }
        }
          .toList()
      if (rows.isEmpty()) {
        logger.info("No raw impressions for $eventGroupReferenceId on $eventDate; skipping file.")
        continue
      }

      val blobKey = "$blobKeyPrefix/$eventDate/$eventGroupReferenceId"
      val footer =
        mapOf(FOOTER_EVENT_GROUP_REFERENCE_ID to eventGroupReferenceId, FOOTER_EVENT_DATE to eventDate)
      newEncryptingClient().writeBlob(
        blobKey,
        flow { rows.forEach { emit(it.toByteString()) } },
        footer,
      )
      logger.info("Wrote ${rows.size} raw impression(s) to $blobKey (event_date=$eventDate).")
      writtenBlobKeys.add(blobKey)
    }
    return writtenBlobKeys
  }

  /** Resolves the configured required entity-key columns from a group's entity keys. */
  private fun entityKeyColumns(
    entityKeys: List<org.wfanet.measurement.loadtest.dataprovider.EntityKey>
  ): Map<String, ParquetValue> =
    requiredEntityKeyColumns.entries.associate { (entityType, column) ->
      val id =
        requireNotNull(entityKeys.firstOrNull { it.entityType == entityType }?.entityId) {
          "event group is missing required entity key '$entityType' (columns need it non-empty)"
        }
      require(id.isNotEmpty()) { "required entity key '$entityType' has an empty id" }
      column to parquetValue { stringValue = id }
    }

  private fun <T : Message> buildRow(
    eventId: String,
    event: LabeledEvent<T>,
    entityColumns: Map<String, ParquetValue>,
    eventColumns: (T) -> Map<String, ParquetValue>,
  ) = parquetRow {
    columns[RawImpressionColumns.EVENT_ID] = parquetValue { stringValue = eventId }
    columns[RawImpressionColumns.EVENT_TIME_USEC] =
      parquetValue {
        int64Value = event.timestamp.epochSecond * 1_000_000L + event.timestamp.nano / 1_000L
      }
    for ((column, value) in entityColumns) columns[column] = value
    for ((column, value) in eventColumns(event.message)) columns[column] = value
  }

  /**
   * A fresh [ParquetStorageClient] that uniform-PME-encrypts every column (and the footer body,
   * kept plaintext for [readKeyValueMetadata]) with [kekUri]. Uses `parquet.encryption.uniform.key`
   * (a single property) rather than footer.key/column.keys so the Tink KEK URI (which contains ':')
   * is not misparsed as an id:columns mapping; the stored master-key id is then the full URI, which
   * the reader resolves by pass-through (no keyMapping needed). Uses a per-write copy of
   * [storageConfiguration] because the client mutates the config to register the KMS bridge.
   */
  private fun newEncryptingClient(): ParquetStorageClient {
    val conf = Configuration(storageConfiguration)
    conf.set(PARQUET_ENCRYPTION_UNIFORM_KEY, kekUri)
    conf.setBoolean(PARQUET_ENCRYPTION_PLAINTEXT_FOOTER, true)
    return ParquetStorageClient(
      conf,
      rootPath,
      encryptionConfig = ParquetEncryptionConfig(kmsProvider = { kmsClient }),
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    // Footer key-value metadata keys the reader requires; must equal
    // org.wfanet.measurement.edpaggregator.vidlabeler.FileEntityKeys.{EVENT_GROUP_REFERENCE_ID_KEY,
    // EVENT_DATE_KEY}.
    private const val FOOTER_EVENT_GROUP_REFERENCE_ID = "event_group_reference_id"
    private const val FOOTER_EVENT_DATE = "event_date"

    // parquet-mr native PME configuration keys (see ParquetStorageClient KDoc).
    private const val PARQUET_ENCRYPTION_UNIFORM_KEY = "parquet.encryption.uniform.key"
    private const val PARQUET_ENCRYPTION_PLAINTEXT_FOOTER = "parquet.encryption.plaintext.footer"
  }
}
