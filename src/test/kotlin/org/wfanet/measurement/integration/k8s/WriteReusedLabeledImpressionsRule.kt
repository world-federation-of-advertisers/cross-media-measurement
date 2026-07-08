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
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import java.time.LocalDate
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.integration.common.ImpressionTestDataConfigs
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.EntityKeyedLabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.EntityKeysWithLabeledEvents
import org.wfanet.measurement.loadtest.dataprovider.LabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration
import org.wfanet.measurement.loadtest.edpaggregator.testing.ImpressionsWriter

/**
 * JUnit [TestRule] that makes the pre-staged out-of-band impressions available under the **memoized
 * model line** for the dates the deployed VID Labeling pipeline does not produce, so the
 * DataWatcher / DataAvailabilitySync register `ImpressionMetadata` for those dates under it.
 *
 * The correctness test measures the memoized model line
 * ([VidLabelerModelResourcesRule.memoizedModelLine]), but the pre-staged out-of-band impressions
 * under `edp/edp7/<date>/` and `edp/edpa_meta/<date>/` were stamped with the *default* line.
 * Without this rule the results-fulfiller finds no impressions for the memoized line on the
 * non-pipelined dates.
 *
 * The two EDPs use different storage KEKs, which dictates two different approaches:
 * * **edp7** encrypts with a **Google Cloud KMS** KEK the CI runner can wrap. This rule regenerates
 *   the same synthetic impressions the direct path produces and rewrites the whole blob
 *   (impressions + metadata) via [ImpressionsWriter], stamped with the memoized line, matching the
 *   flat-folder layout the results-fulfiller expects:
 *   ```
 *   edp/edp7/<date>/impressions               + metadata.binpb
 *   edp/edp7/<date>/impressions-creative      + metadata-creative.binpb
 *   edp/edp7/<date>/impressions-multi-creative + metadata-multi-creative.binpb
 *   ```
 * * **edpa_meta** encrypts with an **AWS KMS** KEK the CI runner (Google Cloud credentials)
 *   *cannot* wrap, so its impressions cannot be re-encrypted here. Instead this rule leaves the
 *   encrypted impressions blob and its `EncryptedDek`/KEK untouched and only rewrites the
 *   `model_line` field in the existing `edp/edpa_meta/<date>/metadata.binpb` `BlobDetails` sidecar
 *   to the memoized line. The results-fulfiller decrypts using the DEK/KEK recorded in that same
 *   sidecar, which is unchanged, so decryption and KEK consistency are preserved.
 *
 * It must run **after** [VidLabelerModelResourcesRule] (needs `memoizedModelLine`) and the event
 * group upload, and **before** `CreateDoneBlobs` so the `done` markers dropped there trigger
 * registration of this data.
 *
 * Dates handled:
 * * **edp7** — every non-pipelined date in [START_DATE]`..`[END_DATE] (i.e. excluding
 *   [EDP7_PIPELINED_DATES]); the pipeline produces the pipelined date under the memoized line.
 * * **edpa_meta** — every date in [START_DATE]`..`[END_DATE] (edpa_meta is not pipelined).
 *
 * edp7's KEK URI is a Google Cloud KMS resource name (not a secret) resolvable by the CI runner's
 * application-default credentials, reusing edp7's storage KEK — the storage KEK for the shared
 * secure-computation bucket. The rule is a **no-op** unless a memoized model line was provisioned;
 * the edp7 regeneration additionally no-ops when [edp7KekUri] is unresolved, so unrelated runs are
 * unaffected.
 *
 * @property config the parsed [ImpressionTestDataConfig]; supplies each event group's
 *   `output_key`, `output_base_path`, `edp_name`, and (entity-key) data specs.
 * @property populationSpec the synthetic population, shared with the direct path so both EDPs draw
 *   from the same universe.
 * @property bucket the storage bucket the results-fulfiller / DataAvailabilitySync read from.
 * @property memoizedModelLineProvider yields the memoized model line resource name to stamp; when
 *   it returns null (provisioning skipped) the rule is a no-op.
 */
class WriteReusedLabeledImpressionsRule(
  private val config: ImpressionTestDataConfig,
  private val populationSpec: PopulationSpec,
  private val bucket: String,
  private val memoizedModelLineProvider: () -> String?,
) : TestRule {

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        val memoizedModelLine = memoizedModelLineProvider()
        if (memoizedModelLine == null) {
          logger.warning(
            "No memoized model line provisioned; skipping memoized labeled-impression setup."
          )
        } else {
          runBlocking { write(memoizedModelLine) }
        }
        base.evaluate()
      }
    }
  }

  private suspend fun write(memoizedModelLine: String) {
    // ImpressionsWriter / EncryptedStorage generate a per-blob StreamingAEAD DEK
    // (AES128_GCM_HKDF_1MB) wrapped by the KMS AEAD KEK, so both Tink configs must be registered
    // first. register() is idempotent; mirrors InProcessEdpAggregatorComponents.
    AeadConfig.register()
    StreamingAeadConfig.register()
    // edp7 uses a Google Cloud KMS KEK the CI runner can wrap, so its impressions are regenerated
    // and re-encrypted stamped with the memoized line. edpa_meta uses an AWS KMS KEK the CI runner
    // cannot wrap, so only its metadata sidecar's model_line is rewritten (impressions + DEK/KEK
    // left untouched).
    regenerateEdp7Impressions(memoizedModelLine)
    restampEdpaMetaMetadata(memoizedModelLine)
  }

  /**
   * Regenerates edp7's impressions (impressions blob + metadata sidecar) via [ImpressionsWriter],
   * stamped with [memoizedModelLine], for every non-pipelined date. No-op when [edp7KekUri] is
   * unresolved so unrelated runs are unaffected.
   */
  private suspend fun regenerateEdp7Impressions(memoizedModelLine: String) {
    if (edp7KekUri.isEmpty()) {
      logger.warning("edp7 storage KEK URI unresolved; skipping edp7 memoized-impression write.")
      return
    }
    val kmsClient: KmsClient = GcpKmsClient().withDefaultCredentials()
    val datesToWrite: Set<LocalDate> = ALL_DATES - EDP7_PIPELINED_DATES
    for (eventGroup in config.eventGroupsList) {
      if (eventGroup.edpName != EDP7_NAME) continue
      writeEventGroup(eventGroup, datesToWrite, memoizedModelLine, kmsClient)
    }
  }

  /**
   * Writes one flat-folder impressions blob (`impressions${output_key_suffix}`) + metadata per date
   * for a single [event group][ImpressionTestDataConfig.SyntheticEventGroup], mirroring
   * [org.wfanet.measurement.loadtest.edpaggregator.tools.GenerateSyntheticData]'s writer wiring but
   * restricted to [datesToWrite] and stamped with [memoizedModelLine].
   */
  private suspend fun writeEventGroup(
    eventGroup: ImpressionTestDataConfig.SyntheticEventGroup,
    datesToWrite: Set<LocalDate>,
    memoizedModelLine: String,
    kmsClient: KmsClient,
  ) {
    val impressionWriter =
      ImpressionsWriter(
        eventGroupReferenceId = eventGroup.eventGroupReferenceId,
        eventGroupPath = "",
        kekUri = edp7KekUri,
        kmsClient = kmsClient,
        impressionsBucket = bucket,
        impressionsMetadataBucket = bucket,
        storagePath = null,
        schema = GCS_SCHEME,
        outputKey = eventGroup.outputKey,
      )
    val shards: Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> =
      generateShards(eventGroup).filter { it.localDate in datesToWrite }
    impressionWriter.writeLabeledImpressionData(
      shards,
      memoizedModelLine,
      flatOutputBasePath = eventGroup.outputBasePath,
    )
    logger.info(
      "Wrote memoized impressions for event group '${eventGroup.eventGroupReferenceId}' " +
        "(${datesToWrite.size} date(s)) under ${eventGroup.outputBasePath}."
    )
  }

  /**
   * Rewrites the `model_line` field to [memoizedModelLine] in each edpa_meta metadata sidecar,
   * leaving the encrypted impressions blob and its `EncryptedDek`/KEK untouched. The sidecar is a
   * single serialized [BlobDetails] message (as written by [ImpressionsWriter] and read by the
   * DataAvailabilitySync / results-fulfiller via `BlobDetails.parseFrom`); only the `model_line`
   * changes so the existing DEK/KEK still decrypts the impressions.
   *
   * edpa_meta has a single legacy event group (no `output_key`, no entity keys), so there is one
   * `metadata.binpb` per date. A missing sidecar (e.g. a date not pre-staged) is logged and skipped
   * rather than failing the run.
   */
  private fun restampEdpaMetaMetadata(memoizedModelLine: String) {
    val outputBasePaths: Set<String> =
      config.eventGroupsList
        .filter { it.edpName == EDPA_META_NAME }
        .map { it.outputBasePath }
        .toSet()
    if (outputBasePaths.isEmpty()) return

    val storage: Storage = StorageOptions.getDefaultInstance().service
    for (outputBasePath in outputBasePaths) {
      for (date in ALL_DATES) {
        val metadataKey = "$outputBasePath/$date/$METADATA_BLOB_NAME"
        val blobId = BlobId.of(bucket, metadataKey)
        val existing = storage.get(blobId)
        if (existing == null) {
          logger.warning(
            "edpa_meta metadata sidecar not found: gs://$bucket/$metadataKey; skipping."
          )
          continue
        }
        val restamped =
          BlobDetails.parseFrom(existing.getContent()).copy { modelLine = memoizedModelLine }
        storage.create(BlobInfo.newBuilder(blobId).build(), restamped.toByteArray())
        logger.info("Re-stamped model line on gs://$bucket/$metadataKey.")
      }
    }
  }

  /**
   * Generates the per-date entity-keyed shards for an event group. Legacy groups produce one
   * unkeyed group per date; entity-key groups coalesce each sub-spec's events for a date into a
   * single shard, stamping each with its own entity key (matching the direct-path layout where all
   * of an event group's entity keys share one blob).
   */
  private fun generateShards(
    eventGroup: ImpressionTestDataConfig.SyntheticEventGroup
  ): Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> {
    if (eventGroup.entityKeySpecsList.isEmpty()) {
      return SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          resolveSpec(eventGroup.dataSpecResourcePath),
        )
        .map { shard: LabeledEventDateShard<TestEvent> ->
          EntityKeyedLabeledEventDateShard(
            shard.localDate,
            sequenceOf(EntityKeysWithLabeledEvents(emptyList(), shard.labeledEvents)),
          )
        }
    }

    val shardsByDate =
      mutableMapOf<LocalDate, MutableList<EntityKeysWithLabeledEvents<TestEvent>>>()
    for (entityKeySpec in eventGroup.entityKeySpecsList) {
      val entityKey = EntityKey(entityKeySpec.entityType, entityKeySpec.entityId)
      val events =
        SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          resolveSpec(entityKeySpec.dataSpecResourcePath),
        )
      for (shard in events) {
        shardsByDate
          .getOrPut(shard.localDate) { mutableListOf() }
          .add(EntityKeysWithLabeledEvents(listOf(entityKey), shard.labeledEvents))
      }
    }
    return shardsByDate.entries
      .sortedBy { it.key }
      .asSequence()
      .map { (date, groups) -> EntityKeyedLabeledEventDateShard(date, groups.asSequence()) }
  }

  private fun resolveSpec(path: String): SyntheticEventGroupSpec =
    ImpressionTestDataConfigs.resolveSyntheticEventGroupSpec(path)

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val GCS_SCHEME = "gs://"
    private const val EDP7_NAME = "edp7"
    private const val EDPA_META_NAME = "edpa_meta"

    // Metadata sidecar name in the flat-folder layout (see ImpressionsWriter). edpa_meta's single
    // legacy event group has no output_key, so its sidecar is exactly "metadata.binpb".
    private const val METADATA_BLOB_NAME = "metadata.binpb"

    // MUST stay in sync with CreateDoneBlobs.START_DATE/END_DATE and SeedRawImpressionsRule.
    private val START_DATE: LocalDate = LocalDate.parse("2021-03-15")
    private val END_DATE: LocalDate = LocalDate.parse("2021-03-21")

    private val ALL_DATES: Set<LocalDate> =
      generateSequence(START_DATE) { it.plusDays(1) }
        .takeWhile { !it.isAfter(END_DATE) }
        .toSet()

    // edp7's pipelined date — MUST stay in sync with SeedRawImpressionsRule.PIPELINED_DATES and
    // CreateDoneBlobs.EDP7_PIPELINED_DATES. For this date the pipeline produces edp7's
    // memoized-line impressions, so the test must NOT pre-write them.
    private val EDP7_PIPELINED_DATES: Set<LocalDate> = setOf(LocalDate.parse("2021-03-21"))

    private fun env(name: String): String = System.getenv(name).orEmpty()

    private val PROJECT_ID: String = env("GOOGLE_CLOUD_PROJECT")

    // edp7's Google Cloud KMS storage KEK per project (KMS resource names, not secrets), used to
    // re-encrypt edp7's regenerated impressions. The CI runner can wrap this with its Google Cloud
    // credentials. edpa_meta's KEK is AWS KMS and is NOT used here — its impressions are left
    // encrypted and only its metadata model_line is rewritten. The EDP7_KEK_URI env overrides. Kept
    // in sync with SeedRawImpressionsRule.KEK_URI_BY_PROJECT.
    private val EDP7_KEK_URI_BY_PROJECT: Map<String, String> =
      mapOf(
        "halo-cmm-dev" to
          "gcp-kms://projects/halo-cmm-dev-edp/locations/global/keyRings/" +
            "halo-cmm-dev-edp-enc-kr/cryptoKeys/halo-cmm-dev-edp-enc-key-"
        // TODO(marcopremier): add halo-cmm-head and halo-cmm-qa edp7 storage KEK URIs.
      )

    private val edp7KekUri: String =
      env("EDP7_KEK_URI").ifEmpty { EDP7_KEK_URI_BY_PROJECT[PROJECT_ID].orEmpty() }
  }
}
