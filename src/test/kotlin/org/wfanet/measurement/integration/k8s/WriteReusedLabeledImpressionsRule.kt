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
import org.wfanet.measurement.aws.kms.AwsKmsClientFactory
import org.wfanet.measurement.common.crypto.tink.AwsWebIdentityCredentials
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
 * Both EDPs' impressions are regenerated and re-encrypted (impressions blob + metadata sidecar via
 * [ImpressionsWriter]), stamped with the memoized line, matching the flat-folder layout the
 * results-fulfiller expects (e.g. `edp/edp7/<date>/impressions[-<output_key>]` plus a matching
 * `metadata[-<output_key>].binpb`). Each event group carries its real entity key, which is how the
 * results-fulfiller matches impressions to event groups. They differ only in KMS:
 * * **edp7** encrypts with a **Google Cloud KMS** KEK the CI runner wraps with its
 *   application-default credentials.
 * * **edpa_meta** encrypts with an **AWS KMS** KEK, assumed via web-identity federation
 *   ([AwsKmsClientFactory] + [AwsWebIdentityCredentials]). The deployed results-fulfiller decrypts
 *   the same bytes with the same AWS KEK, so AWS KMS is exercised end to end.
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
 * The rule is a **no-op** unless a memoized model line was provisioned; each EDP's regeneration
 * additionally no-ops when its KMS settings are unresolved (edp7's Google Cloud KEK URI, or
 * edpa_meta's AWS role/region/web-identity-token/KEK), so unrelated runs are unaffected.
 *
 * @property config the parsed [ImpressionTestDataConfig]; supplies each event group's `output_key`,
 *   `output_base_path`, `edp_name`, and (entity-key) data specs.
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
    // Both EDPs' impressions are regenerated and re-encrypted stamped with the memoized line: edp7
    // with its Google Cloud KMS KEK, edpa_meta with its AWS KMS KEK (assumed via web-identity
    // federation). Generating rather than re-stamping lets each event group carry its real entity
    // key, which is how the results-fulfiller matches impressions to event groups.
    regenerateEdp7Impressions(memoizedModelLine)
    regenerateEdpaMetaImpressions(memoizedModelLine)
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
      writeEventGroup(eventGroup, datesToWrite, memoizedModelLine, kmsClient, edp7KekUri)
    }
  }

  /**
   * Writes one impressions blob (`impressions${output_key_suffix}`) + metadata sidecar per date for
   * a single [event group][ImpressionTestDataConfig.SyntheticEventGroup], under the canonical
   * per-EDP, per-model-line layout `<output_base_path>/model-line/<modelLineId>/<date>/` that the
   * deployed VidLabeler produces, so the pre-labeled data lands in the same folder the
   * DataAvailabilitySync crawls. Restricted to [datesToWrite] and stamped with [memoizedModelLine].
   */
  private suspend fun writeEventGroup(
    eventGroup: ImpressionTestDataConfig.SyntheticEventGroup,
    datesToWrite: Set<LocalDate>,
    memoizedModelLine: String,
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
    val shards: Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> =
      generateShards(eventGroup).filter { it.localDate in datesToWrite }
    impressionWriter.writeLabeledImpressionData(
      shards,
      memoizedModelLine,
      modelLineOutputBasePath = eventGroup.outputBasePath,
    )
    logger.info(
      "Wrote memoized impressions for event group '${eventGroup.eventGroupReferenceId}' " +
        "(${datesToWrite.size} date(s)) under " +
        "${eventGroup.outputBasePath}/model-line/<id>/<date>/."
    )
  }

  /**
   * Regenerates edpa_meta's impressions (impressions blob + metadata sidecar) via
   * [ImpressionsWriter] for **every** date in the window (edpa_meta is never pipelined, so the test
   * produces all of its dates out-of-band), stamped with [memoizedModelLine] and each event group's
   * real entity key.
   *
   * Encrypts with edpa_meta's **AWS KMS** KEK, assumed via web-identity federation
   * ([AwsKmsClientFactory] + [AwsWebIdentityCredentials]), so the deployed results-fulfiller
   * decrypts the same bytes with the same AWS KEK. No-op when the edpa_meta AWS settings are
   * unresolved so unrelated runs are unaffected.
   */
  private suspend fun regenerateEdpaMetaImpressions(memoizedModelLine: String) {
    if (
      EDPA_META_KEK_URI.isEmpty() ||
        EDPA_META_AWS_ROLE_ARN.isEmpty() ||
        EDPA_META_AWS_REGION.isEmpty() ||
        EDPA_META_AWS_WEB_IDENTITY_TOKEN_FILE.isEmpty()
    ) {
      logger.warning(
        "edpa_meta AWS KMS settings unresolved; skipping edpa_meta memoized-impression write."
      )
      return
    }
    val kmsClient: KmsClient =
      AwsKmsClientFactory()
        .getKmsClient(
          AwsWebIdentityCredentials(
            roleArn = EDPA_META_AWS_ROLE_ARN,
            webIdentityTokenFilePath = EDPA_META_AWS_WEB_IDENTITY_TOKEN_FILE,
            roleSessionName = EDPA_META_AWS_ROLE_SESSION_NAME,
            region = EDPA_META_AWS_REGION,
          )
        )
    for (eventGroup in config.eventGroupsList) {
      if (eventGroup.edpName != EDPA_META_NAME) continue
      writeEventGroup(eventGroup, ALL_DATES, memoizedModelLine, kmsClient, EDPA_META_KEK_URI)
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

    // MUST stay in sync with CreateDoneBlobs.START_DATE/END_DATE and SeedRawImpressionsRule.
    private val START_DATE: LocalDate = LocalDate.parse("2021-03-15")
    private val END_DATE: LocalDate = LocalDate.parse("2021-03-21")

    private val ALL_DATES: Set<LocalDate> =
      generateSequence(START_DATE) { it.plusDays(1) }.takeWhile { !it.isAfter(END_DATE) }.toSet()

    // edp7's pipelined date — MUST stay in sync with SeedRawImpressionsRule.PIPELINED_DATES and
    // CreateDoneBlobs.EDP7_PIPELINED_DATES. For this date the pipeline produces edp7's
    // memoized-line impressions, so the test must NOT pre-write them.
    private val EDP7_PIPELINED_DATES: Set<LocalDate> = setOf(LocalDate.parse("2021-03-21"))

    private fun env(name: String): String = System.getenv(name).orEmpty()

    private val PROJECT_ID: String = env("GOOGLE_CLOUD_PROJECT")

    // edp7's Google Cloud KMS storage KEK per project (KMS resource names, not secrets), used to
    // re-encrypt edp7's regenerated impressions. The CI runner can wrap this with its Google Cloud
    // credentials. The EDP7_KEK_URI env overrides. Kept in sync with
    // SeedRawImpressionsRule.KEK_URI_BY_PROJECT.
    // edp7's KMS key is in the dev EDP project for all three test envs (edp7
    // kms_config.service_account = primus-sa@halo-cmm-dev-edp everywhere).
    private val EDP7_KEK_URI_ALL_ENVS: String =
      "gcp-kms://projects/halo-cmm-dev-edp/locations/global/keyRings/" +
        "halo-cmm-dev-edp-enc-kr/cryptoKeys/halo-cmm-dev-edp-enc-key-"
    private val EDP7_KEK_URI_BY_PROJECT: Map<String, String> =
      mapOf(
        "halo-cmm-dev" to EDP7_KEK_URI_ALL_ENVS,
        "halo-cmm-head" to EDP7_KEK_URI_ALL_ENVS,
        "halo-cmm-qa" to EDP7_KEK_URI_ALL_ENVS,
      )

    private val edp7KekUri: String =
      env("EDP7_KEK_URI").ifEmpty { EDP7_KEK_URI_BY_PROJECT[PROJECT_ID].orEmpty() }

    // edpa_meta's AWS KMS settings for regenerating its impressions in-test. These mirror
    // GenerateSyntheticData's AWS flags (--kek-uri aws-kms://arn:..., --aws-role-arn, --aws-region,
    // --aws-web-identity-token-file) and must be provided to the CI runner (the same AWS role/key
    // the
    // data staging uses). AWS_WEB_IDENTITY_TOKEN_FILE is the standard AWS SDK env var.
    private const val EDPA_META_AWS_ROLE_SESSION_NAME = "edpa-meta-correctness-test"
    private val EDPA_META_KEK_URI: String = env("EDPA_META_KEK_URI")
    private val EDPA_META_AWS_ROLE_ARN: String = env("EDPA_META_AWS_ROLE_ARN")
    private val EDPA_META_AWS_REGION: String = env("EDPA_META_AWS_REGION")
    private val EDPA_META_AWS_WEB_IDENTITY_TOKEN_FILE: String = env("AWS_WEB_IDENTITY_TOKEN_FILE")
  }
}
