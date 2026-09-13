// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.integration.k8s

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
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.aws.kms.AwsKmsClientFactory
import org.wfanet.measurement.common.crypto.tink.AwsWebIdentityCredentials
import org.wfanet.measurement.integration.common.ImpressionTestDataConfigs
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.EntityKeyedLabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.EntityKeysWithLabeledEvents
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration
import org.wfanet.measurement.loadtest.edpaggregator.testing.ImpressionsWriter

/**
 * JUnit [TestRule] that writes the QA 2026 synthetic dataset as pre-labeled impressions, stamped
 * with the 2026 model line.
 *
 * This is **additive**: it writes under its own model line and its own date window, so the 2021
 * fixture's event groups, specs and blobs are untouched. Impressions are stored under
 * `<output_base_path>/model-line/<modelLineId>/<date>/`, so a distinct model line is a distinct
 * directory tree.
 *
 * Unlike [WriteReusedLabeledImpressionsRule], VIDs are taken **straight from the data specs** and
 * are not passed through the deployed VID model. That rule relabels via [NonMemoizedVidLabeler] so
 * its out-of-band days agree with the one date the pipeline produces; the 2026 dataset has no
 * pipelined date, so there is nothing to agree with. Relabeling here would be actively harmful: it
 * would map the ~10.6M spec VIDs into the deployed hash-only model's much smaller pool, silently
 * collapsing reach and destroying the Venn topology the dataset exists to provide.
 *
 * Because no raw impressions are written for these dates, the deployed VID labeling pipeline is
 * never triggered for them: raw-impression `done` markers drive the labeling dispatcher, while the
 * markers [CreateDoneBlobs] drops beside labeled impressions drive `DataAvailabilitySync`.
 *
 * The rule is a **no-op** unless a 2026 model line is configured, so dev and head runs are
 * unaffected until their `QA2026_MODEL_LINE` is set. Each EDP's write additionally no-ops when its
 * KMS settings are unresolved, mirroring [WriteReusedLabeledImpressionsRule].
 *
 * @property configProvider yields the QA 2026 [ImpressionTestDataConfig]
 * @property populationSpecProvider yields the QA 2026 synthetic population
 * @property bucket impressions bucket
 * @property modelLineProvider yields the 2026 model line resource name to stamp; null or empty
 *   disables the rule
 */
class WriteQa2026ImpressionsRule(
  private val configProvider: () -> ImpressionTestDataConfig,
  private val populationSpecProvider: () -> PopulationSpec,
  private val bucket: String,
  private val modelLineProvider: () -> String?,
) : TestRule {

  // Resolved only once the rule actually runs, so environments without a QA 2026 model line never
  // parse the 2026 specs and a malformed one cannot break the 2021 fixture's run.
  private val config: ImpressionTestDataConfig by lazy { configProvider() }
  private val populationSpec: PopulationSpec by lazy { populationSpecProvider() }

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        val modelLine = modelLineProvider()
        if (modelLine.isNullOrEmpty()) {
          logger.info("No QA 2026 model line configured; skipping QA 2026 impression write.")
        } else {
          runBlocking { write(modelLine) }
        }
        base.evaluate()
      }
    }
  }

  private suspend fun write(modelLine: String) {
    // ImpressionsWriter / EncryptedStorage generate a per-blob StreamingAEAD DEK wrapped by the KMS
    // AEAD KEK, so both Tink configs must be registered. register() is idempotent.
    AeadConfig.register()
    StreamingAeadConfig.register()
    // Fail rather than silently skip: the caller has already restricted the config to the EDPs it
    // says are provisioned, so an EDP with no KMS handling means the dataset would be written
    // short with no signal, and every downstream reach assertion would be quietly wrong.
    val unhandled =
      config.eventGroupsList
        .map { it.edpName }
        .filterNot { it in GCP_KMS_KEK_URI_BY_EDP || it in AWS_KMS_EDPS }
        .toSortedSet()
    check(unhandled.isEmpty()) {
      "No KMS configuration for QA 2026 EDP(s) $unhandled. Add them to GCP_KMS_KEK_URI_BY_EDP or " +
        "AWS_KMS_EDPS, or drop them from the QA2026_EDPS env var."
    }

    writeGcpKmsEdps(modelLine)
    writeAwsKmsEdps(modelLine)
  }

  /** Writes every event group whose EDP encrypts with a Google Cloud KMS KEK. */
  private suspend fun writeGcpKmsEdps(modelLine: String) {
    val kmsClient: KmsClient by lazy { GcpKmsClient().withDefaultCredentials() }
    for (eventGroup in config.eventGroupsList) {
      val kekUri = GCP_KMS_KEK_URI_BY_EDP[eventGroup.edpName] ?: continue
      if (kekUri.isEmpty()) {
        logger.warning(
          "${eventGroup.edpName} storage KEK URI unresolved; skipping its QA 2026 write."
        )
        continue
      }
      writeEventGroup(eventGroup, modelLine, kmsClient, kekUri)
    }
  }

  /** Writes every event group whose EDP encrypts with an AWS KMS KEK. */
  private suspend fun writeAwsKmsEdps(modelLine: String) {
    val eventGroups = config.eventGroupsList.filter { it.edpName in AWS_KMS_EDPS }
    if (eventGroups.isEmpty()) {
      return
    }
    if (
      EDPA_META_KEK_URI.isEmpty() ||
        EDPA_META_AWS_ROLE_ARN.isEmpty() ||
        EDPA_META_AWS_REGION.isEmpty() ||
        EDPA_META_AWS_WEB_IDENTITY_TOKEN_FILE.isEmpty()
    ) {
      logger.warning("edpa_meta AWS KMS settings unresolved; skipping its QA 2026 write.")
      return
    }
    val kmsClient: KmsClient =
      AwsKmsClientFactory()
        .getKmsClient(
          AwsWebIdentityCredentials(
            roleArn = EDPA_META_AWS_ROLE_ARN,
            webIdentityTokenFilePath = EDPA_META_AWS_WEB_IDENTITY_TOKEN_FILE,
            roleSessionName = AWS_ROLE_SESSION_NAME,
            region = EDPA_META_AWS_REGION,
          )
        )
    for (eventGroup in eventGroups) {
      writeEventGroup(eventGroup, modelLine, kmsClient, EDPA_META_KEK_URI)
    }
  }

  /**
   * Writes one impressions blob (`impressions${output_key_suffix}`) plus metadata sidecar per date
   * for a single event group, under the per-EDP, per-model-line layout the results fulfiller and
   * `DataAvailabilitySync` crawl.
   */
  private suspend fun writeEventGroup(
    eventGroup: ImpressionTestDataConfig.SyntheticEventGroup,
    modelLine: String,
    kmsClient: KmsClient,
    kekUri: String,
  ) {
    val impressionWriter =
      ImpressionsWriter(
        eventGroupReferenceId = eventGroup.eventGroupReferenceId,
        eventGroupPath = "",
        kekUri = kekUri,
        kmsClient = kmsClient,
        impressionsBucket = bucket,
        impressionsMetadataBucket = bucket,
        storagePath = null,
        schema = GCS_SCHEME,
        outputKey = eventGroup.outputKey,
      )
    impressionWriter.writeLabeledImpressionData(
      generateShards(eventGroup),
      modelLine,
      modelLineOutputBasePath = eventGroup.outputBasePath,
    )
    logger.info(
      "Wrote QA 2026 impressions for event group '${eventGroup.eventGroupReferenceId}' under " +
        "${eventGroup.outputBasePath}/model-line/<id>/<date>/."
    )
  }

  /**
   * Generates the per-date entity-keyed shards for an event group, stamping each with its own
   * entity key. Every QA 2026 event group carries at least one entity key: `EventGroupSync` filters
   * its existence check by entity type, so a group without one is invisible to that check and is
   * created again on every sync.
   */
  private fun generateShards(
    eventGroup: ImpressionTestDataConfig.SyntheticEventGroup
  ): Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> {
    require(eventGroup.entityKeySpecsList.isNotEmpty()) {
      "QA 2026 event group '${eventGroup.eventGroupReferenceId}' has no entity key spec"
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
      .asSequence()
      .sortedBy { it.key }
      .map { (date, groups) -> EntityKeyedLabeledEventDateShard(date, groups.asSequence()) }
  }

  private fun resolveSpec(path: String): SyntheticEventGroupSpec =
    ImpressionTestDataConfigs.resolveSyntheticEventGroupSpec(path)

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val GCS_SCHEME = "gs://"
    private const val EDP7_NAME = "edp7"
    private const val EDPA_META_NAME = "edpa_meta"
    private const val AWS_ROLE_SESSION_NAME = "qa2026-correctness-test"

    private fun env(name: String): String = System.getenv(name).orEmpty()

    private val PROJECT_ID: String = env("GOOGLE_CLOUD_PROJECT")

    /**
     * Resource name of the QA 2026 [org.wfanet.measurement.api.v2alpha.ModelLine], or empty when
     * this environment has not been provisioned with one. Empty disables all QA 2026 seeding.
     */
    val MODEL_LINE: String = env("QA2026_MODEL_LINE")

    /** EDPs whose impressions are encrypted with a Google Cloud KMS KEK, by KEK URI. */
    private val GCP_KMS_KEK_URI_BY_EDP: Map<String, String> =
      mapOf(
        EDP7_NAME to env("EDP7_KEK_URI").ifEmpty { Edp7StorageKek.BY_PROJECT[PROJECT_ID].orEmpty() }
      )

    /** EDPs whose impressions are encrypted with an AWS KMS KEK. */
    private val AWS_KMS_EDPS: Set<String> = setOf(EDPA_META_NAME)

    private val EDPA_META_KEK_URI: String = env("EDPA_META_KEK_URI")
    private val EDPA_META_AWS_ROLE_ARN: String = env("EDPA_META_AWS_ROLE_ARN")
    private val EDPA_META_AWS_REGION: String = env("EDPA_META_AWS_REGION")
    private val EDPA_META_AWS_WEB_IDENTITY_TOKEN_FILE: String = env("AWS_WEB_IDENTITY_TOKEN_FILE")
  }
}
