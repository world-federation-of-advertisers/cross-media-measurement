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

package org.wfanet.measurement.integration.k8s

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import java.io.File
import java.nio.file.Files
import java.time.LocalDate
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.integration.common.EventGroupConfig
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.EntityKeyedLabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.EntityKeysWithLabeledEvents
import org.wfanet.measurement.loadtest.dataprovider.LabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration
import org.wfanet.measurement.loadtest.edpaggregator.testing.RawImpressionColumns
import org.wfanet.measurement.loadtest.edpaggregator.testing.RawImpressionsWriter
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.parquetValue

/**
 * JUnit [TestRule] that seeds the pipelined EDP's (edp7) **raw impressions** so the deployed VID
 * Labeling pipeline has something to label, then drops a `done` marker per date to trigger it.
 *
 * Mirrors [the direct path][EdpAggregatorCorrectnessTest]'s `UploadEventGroup` + `CreateDoneBlobs`,
 * but for the raw path: it (re)generates the same synthetic events the direct path would produce and
 * writes them as **PME-encrypted Parquet** via [RawImpressionsWriter]. The `VidLabelingDispatcher`
 * (fired by the DataWatcher on the `done` blob) lists every non-`done` blob in the folder and
 * registers them as a `RawImpressionUpload` + `RawImpressionUploadFile`s, so no manual API
 * registration is needed here.
 *
 * ## Why write locally then upload
 * The Parquet is written to a **local** temp dir and the encrypted bytes are uploaded to GCS via the
 * google-cloud-storage SDK, instead of writing straight to `gs://` through the Parquet client's
 * Hadoop GCS connector. The connector (`gcs-connector` hadoop3) resolves credentials with the legacy
 * `google-api-client` `GoogleCredential`, which does not understand the CI runner's Workload Identity
 * Federation (`external_account`) credentials; the google-cloud-storage SDK and the Tink KMS client
 * both do. PME encryption happens during the local write (via the Tink [KmsClient]); the uploaded
 * bytes are the finished encrypted file, which the TEE reads and decrypts unchanged.
 *
 * Idempotent across runs: existing blobs under the prefix are deleted first, and each `done` marker
 * is rewritten (a new GCS generation yields a fresh `RawImpressionUpload` request id).
 *
 * Env-provided inputs (wired by the cloud-test workflow); the rule is a **no-op** unless both
 * [RAW_IMPRESSIONS_BUCKET] and [EDP7_KEK_URI] are resolvable, so unrelated runs are unaffected:
 * * `RAW_IMPRESSIONS_BUCKET` — GCS bucket the DataWatcher watches for raw-impression `done` blobs.
 * * `RAW_IMPRESSIONS_PREFIX` — path prefix under that bucket; default `raw-impressions/edp/edp7`.
 * * `EDP7_KEK_URI` — edp7's GCP KMS key URI (else resolved from [KEK_URI_BY_PROJECT] by project).
 * * `GOOGLE_CLOUD_PROJECT` — project id.
 *
 * @property populationSpec the synthetic population (VID -> demographics), shared with the direct
 *   path so both EDPs draw from the same universe.
 * @property eventGroupConfigs edp7's event-group configs keyed by the flattened id (legacy groups by
 *   `event_group_reference_id`; entity-key groups split so each entity key spec is its own entry,
 *   keyed `"${entityType}-${entityId}"`), edpa_meta excluded. One PME Parquet file is written per
 *   (entry, date). Each entry contributes exactly one entity key, matching the real entity key of
 *   the event group as written by the out-of-band days-15-20 data ([WriteReusedLabeledImpressionsRule]),
 *   so the pipelined day's output matches the fulfiller's per-event-group query.
 */
class SeedRawImpressionsRule(
  private val populationSpec: PopulationSpec,
  private val eventGroupConfigs: Map<String, EventGroupConfig>,
) : TestRule {

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        if (RAW_IMPRESSIONS_BUCKET.isEmpty() || EDP7_KEK_URI.isEmpty()) {
          logger.warning(
            "RAW_IMPRESSIONS_BUCKET/EDP7_KEK_URI unresolved; skipping edp7 raw-impression seeding."
          )
        } else {
          runBlocking { seed() }
        }
        base.evaluate()
      }
    }
  }

  private suspend fun seed() {
    val kmsClient: KmsClient = GcpKmsClient().withDefaultCredentials()
    val storage: Storage = StorageOptions.getDefaultInstance().service
    val localRoot: File = Files.createTempDirectory("raw-impressions").toFile()
    // Explicit file:// scheme so the Parquet client uses the local FS regardless of any
    // fs.defaultFS (e.g. gs://) on the classpath core-site.xml.
    val rootPath = Path(localRoot.toURI())

    deleteExistingRawImpressions(storage)

    val datesWritten = sortedSetOf<String>()
    for ((refId, config) in eventGroupConfigs) {
      val writer =
        RawImpressionsWriter(
          blobKeyId = refId,
          kekUri = EDP7_KEK_URI,
          kmsClient = kmsClient,
          storageConfiguration = Configuration(),
          rootPath = rootPath,
          requiredEntityKeyColumns = entityKeyColumnsFor(config),
        )
      val shards: Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> =
        SyntheticDataGeneration.generateEvents(
            TestEvent.getDefaultInstance(),
            populationSpec,
            resolveSpec(config),
          )
          // Only PIPELINED_DATES run through the deployed VID Labeling pipeline; the earlier days
          // use pre-labeled impressions already in storage (edp/edp7/<date>/), just like the direct
          // EDP. This keeps the full-window measurement intact while the pipeline processes only 2
          // uploads instead of 7, so the run fits the CI timeout.
          .filter { it.localDate in PIPELINED_DATES }
          .map { it.toEntityKeyed(config) }
      val blobKeys: List<String> =
        writer.writeRawImpressions(shards, RAW_IMPRESSIONS_PREFIX) { event -> testEventColumns(event) }
      for (blobKey in blobKeys) {
        uploadToGcs(storage, File(localRoot, blobKey), blobKey)
        datesWritten.add(blobKey.removePrefix("$RAW_IMPRESSIONS_PREFIX/").substringBefore("/"))
      }
      logger.info("Wrote + uploaded ${blobKeys.size} raw-impression file(s) for event group $refId.")
    }

    for (date in datesWritten) {
      val doneKey = "$RAW_IMPRESSIONS_PREFIX/$date/done"
      storage.create(BlobInfo.newBuilder(BlobId.of(RAW_IMPRESSIONS_BUCKET, doneKey)).build(), ByteArray(0))
    }
    logger.info("Seeded edp7 raw impressions for ${datesWritten.size} date(s); done markers written.")
  }

  /** Uploads a locally-written (already PME-encrypted) Parquet file to `gs://bucket/blobKey`. */
  private fun uploadToGcs(storage: Storage, localFile: File, blobKey: String) {
    storage.create(
      BlobInfo.newBuilder(BlobId.of(RAW_IMPRESSIONS_BUCKET, blobKey)).build(),
      localFile.readBytes(),
    )
  }

  /** The single [SyntheticEventGroupSpec] backing an event group entry. */
  private fun resolveSpec(config: EventGroupConfig): SyntheticEventGroupSpec =
    when (config) {
      is EventGroupConfig.LegacySpec -> config.spec
      is EventGroupConfig.MultiEntityKey -> config.entityKeySpecs.single().spec
    }

  /**
   * The entity-key columns the writer emits per impression for this event group entry (`entity_type`
   * -> Parquet column). Every file carries the `person` -> `person_id` column: `person_id` doubles as
   * the model identity (`profile_info...user_id`, a required labeler-input column present on every
   * file) and a `person` entity key. Entity-key groups additionally carry the `creative-id` ->
   * `creative_id` column so the results-fulfiller can match those impressions to the event group by
   * its `{creative-id, entity_id}` key. The legacy group carries only the `person` key, which the
   * fulfiller ignores (it matches legacy groups by `event_group_reference_id`) but which satisfies
   * the reader's requirement that every impression carry >=1 entity key.
   */
  private fun entityKeyColumnsFor(config: EventGroupConfig): Map<String, String> =
    when (config) {
      is EventGroupConfig.LegacySpec ->
        mapOf(RawImpressionColumns.ENTITY_TYPE_PERSON to RawImpressionColumns.PERSON_ID)
      is EventGroupConfig.MultiEntityKey ->
        mapOf(
          RawImpressionColumns.ENTITY_TYPE_PERSON to RawImpressionColumns.PERSON_ID,
          config.entityKeySpecs.single().entityKey.entityType to RawImpressionColumns.CREATIVE_ID,
        )
    }

  /**
   * Keys each impression with this event group entry's entity keys, matching the out-of-band
   * days-15-20 data ([WriteReusedLabeledImpressionsRule]) for the fulfiller-relevant keys:
   * * entity-key group -> the group's own `{entity_type, entity_id}` (e.g.
   *   `{creative-id, edpa-eg-creative-id-1}`), so the fulfiller matches the pipelined impressions to
   *   the event group by that key, PLUS a per-person `{person, person-<vid>}` key that carries the
   *   model identity column and never matches any event group (harmless).
   * * legacy group -> only the per-person `{person, person-<vid>}` key; the fulfiller matches legacy
   *   groups by `event_group_reference_id`, so its value is ignored downstream.
   *
   * The pipeline re-assigns the VID from the model; the direct-path expected VID and this one match
   * only when the model reproduces the synthetic assignment (validated by an end-to-end run).
   */
  private fun LabeledEventDateShard<TestEvent>.toEntityKeyed(
    config: EventGroupConfig
  ): EntityKeyedLabeledEventDateShard<TestEvent> =
    EntityKeyedLabeledEventDateShard(
      localDate = localDate,
      entityKeysWithLabeledEvents =
        labeledEvents.map { event ->
          EntityKeysWithLabeledEvents(
            entityKeys = entityKeysFor(config, event.vid),
            labeledEvents = sequenceOf(event),
          )
        },
    )

  /** The [EntityKey]s for one impression of this event group entry. */
  private fun entityKeysFor(config: EventGroupConfig, vid: Long): List<EntityKey> {
    val personKey = EntityKey(RawImpressionColumns.ENTITY_TYPE_PERSON, "person-$vid")
    return when (config) {
      is EventGroupConfig.LegacySpec -> listOf(personKey)
      is EventGroupConfig.MultiEntityKey ->
        listOf(personKey, config.entityKeySpecs.single().entityKey)
    }
  }

  /** Demographic columns projected from the event message (enum value names). */
  private fun testEventColumns(event: TestEvent): Map<String, ParquetValue> =
    mapOf(
      RawImpressionColumns.PERSON_GENDER to parquetValue { stringValue = event.person.gender.name },
      RawImpressionColumns.PERSON_AGE_GROUP to
        parquetValue { stringValue = event.person.ageGroup.name },
    )

  private fun deleteExistingRawImpressions(storage: Storage) {
    storage
      .list(RAW_IMPRESSIONS_BUCKET, Storage.BlobListOption.prefix("$RAW_IMPRESSIONS_PREFIX/"))
      .iterateAll()
      .forEach { blob -> storage.delete(RAW_IMPRESSIONS_BUCKET, blob.name) }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    // Date edp7 runs through the deployed VID Labeling pipeline. All other dates in the data spec
    // are served by pre-labeled impressions already staged in edp/edp7/<date>/ (like the direct
    // EDP), so the pipeline processes a single upload and the test fits the CI timeout. Kept to one
    // date deliberately: when several raw `done` blobs drop at once the dispatcher processes the
    // first and defers the rest, and nothing re-triggers the deferred ones (monitor re-dispatch is
    // unimplemented; uploads arrive sequentially in production). One upload still exercises Phase
    // 0/1/2 end to end.
    private val PIPELINED_DATES: Set<LocalDate> = setOf(LocalDate.parse("2021-03-21"))

    private fun env(name: String): String = System.getenv(name).orEmpty()

    private val PROJECT_ID: String = env("GOOGLE_CLOUD_PROJECT")

    // edp7's storage KEK per GCP project (KMS resource names, not secrets) — the key the deployed
    // VidLabeler TEE decrypts raw impressions with. Kept here rather than a per-env GitHub variable
    // (the env var limit is reached); the EDP7_KEK_URI env still overrides.
    private val KEK_URI_BY_PROJECT: Map<String, String> =
      mapOf(
        "halo-cmm-dev" to
          "gcp-kms://projects/halo-cmm-dev-edp/locations/global/keyRings/" +
            "halo-cmm-dev-edp-enc-kr/cryptoKeys/halo-cmm-dev-edp-enc-key-"
        // TODO(marcopremier): add halo-cmm-head and halo-cmm-qa edp7 KEK URIs.
      )
    private val EDP7_KEK_URI: String =
      env("EDP7_KEK_URI").ifEmpty { KEK_URI_BY_PROJECT[PROJECT_ID].orEmpty() }

    private val RAW_IMPRESSIONS_BUCKET: String = env("RAW_IMPRESSIONS_BUCKET")
    private val RAW_IMPRESSIONS_PREFIX: String =
      env("RAW_IMPRESSIONS_PREFIX").ifEmpty { "raw-impressions/edp/edp7" }.trimEnd('/')
  }
}
