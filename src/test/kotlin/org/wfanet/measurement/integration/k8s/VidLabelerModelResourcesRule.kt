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

import io.grpc.ManagedChannel
import java.nio.file.Path
import java.nio.file.Paths
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.listModelLinesRequest
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfig
import org.wfanet.measurement.edpaggregator.v1alpha.AgeBucket
import org.wfanet.measurement.edpaggregator.v1alpha.AgeRange
import org.wfanet.measurement.edpaggregator.v1alpha.BucketLookup
import org.wfanet.measurement.edpaggregator.v1alpha.EnumLookup
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.ScalarColumn
import org.wfanet.measurement.loadtest.edpaggregator.testing.RawImpressionColumns

/**
 * JUnit [TestRule] that resolves the Kingdom model lines the VID Labeling pipeline uses for the
 * pipelined EDP, and exposes them to the test.
 *
 * The model lines (and their releases/rollouts/shards) plus the deployed dispatcher/monitor
 * `VidLabelingConfigs` are provisioned out-of-band; this rule only looks the lines up by their
 * stable `display_name` and never creates any resource. If a line is missing it fails fast.
 *
 * Three model lines are resolved under the existing suite:
 * * one memoized ([memoizedModelLine]), and
 * * two non-memoized ([nonMemoizedModelLine] is the first of these).
 *
 * The non-memoized line's VID assignment is a pure function of the impression (static model +
 * hash), so the test can reproduce it offline with [labelerInputFieldMapping], unlike the memoized
 * line.
 *
 * Authentication is pure mutual-TLS using the model provider identity (`mp1_*` certs). The Kingdom
 * maps the client cert's authority-key-identifier to the principal, so no API key or principal
 * override is used.
 *
 * Env-provided inputs (set by the cloud-test workflow); if [MODEL_SUITE] is unset the rule is a
 * no-op so unrelated runs are unaffected:
 * * `MODEL_SUITE` — the ModelSuite the lines live under.
 * * `NON_MEMOIZED_MODEL_BLOB_URI` — compiled Riegeli hash-only model blob the pipeline labels with.
 * * `EVENT_TEMPLATE_DESCRIPTOR_BLOB_URI` / `EVENT_TEMPLATE_TYPE` — event schema for the label
 *   output.
 */
class VidLabelerModelResourcesRule(
  private val kingdomPublicApiTarget: String,
  private val kingdomPublicApiCertHost: String?,
) : TestRule {

  /**
   * Resource name of the memoized cloudtest model line, populated by [resolveModelLines]. The cloud
   * test points its measurement at this line (which the pipeline labels) rather than the shared
   * `MODEL_LINE_NAME` var (also used by SyntheticGeneratorCorrectnessTest). Null if provisioning
   * was skipped.
   */
  var memoizedModelLine: String? = null
    private set

  /**
   * Resource name of the first non-memoized (hash-only) cloudtest model line, populated by
   * [resolveModelLines]. The cloud test points its measurements at this line: its VID assignment is
   * a pure function of the impression (static model + hash), so it is deterministically
   * reproducible offline, unlike the memoized line. Null if provisioning was skipped.
   */
  var nonMemoizedModelLine: String? = null
    private set

  /** GCS URI of the non-memoized (hash-only) compiled model blob the pipeline labels with. */
  val nonMemoizedModelBlobUri: String
    get() = NON_MEMOIZED_MODEL_BLOB_URI

  /**
   * The `labeler_input_field_mapping` the deployed pipeline uses to build a `LabelerInput` from the
   * raw-impression columns. Exposed so the test can run the same labeler offline over the
   * out-of-band days and reproduce the pipeline's VID assignment.
   */
  val labelerInputFieldMapping: List<LabelerInputFieldMapping> by lazy {
    buildModelLineConfig().labelerInputFieldMappingList
  }

  private data class LineSpec(val displayName: String, val memoized: Boolean)

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        if (MODEL_SUITE.isEmpty()) {
          logger.warning("MODEL_SUITE unset; skipping VID Labeling model-line resolution.")
        } else {
          runBlocking { resolveModelLines() }
        }
        base.evaluate()
      }
    }
  }

  private suspend fun resolveModelLines() {
    val modelProviderChannel: ManagedChannel = buildChannel("mp1_tls.pem", "mp1_tls.key")
    try {
      val modelLinesStub = ModelLinesCoroutineStub(modelProviderChannel)

      val lineSpecs =
        listOf(
          LineSpec(MEMOIZED_LINE_DISPLAY_NAME, memoized = true),
          LineSpec(NON_MEMOIZED_LINE_1_DISPLAY_NAME, memoized = false),
          LineSpec(NON_MEMOIZED_LINE_2_DISPLAY_NAME, memoized = false),
        )

      for (spec in lineSpecs) {
        val line = getModelLine(modelLinesStub, spec.displayName)
        if (spec.memoized) memoizedModelLine = line.name
        else if (nonMemoizedModelLine == null) nonMemoizedModelLine = line.name
        logger.info("Resolved model line ${line.name} (memoized=${spec.memoized}).")
      }
    } finally {
      modelProviderChannel.shutdown()
    }
  }

  /**
   * Returns the [ModelLine] under [MODEL_SUITE] whose `display_name` matches [displayName], paging
   * as needed. Throws [IllegalStateException] if no line matches: the model lines are provisioned
   * out-of-band, so a missing line is a setup error rather than something to create here.
   */
  private suspend fun getModelLine(stub: ModelLinesCoroutineStub, displayName: String): ModelLine {
    var pageToken = ""
    do {
      val response =
        stub.listModelLines(
          listModelLinesRequest {
            parent = MODEL_SUITE
            pageSize = LIST_PAGE_SIZE
            this.pageToken = pageToken
          }
        )
      response.modelLinesList
        .firstOrNull { it.displayName == displayName }
        ?.let {
          return it
        }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())

    throw IllegalStateException(
      "Cloudtest model line '$displayName' not found under $MODEL_SUITE; it must be provisioned " +
        "out-of-band first."
    )
  }

  /**
   * Builds the operator [ModelLineConfig][VidLabelingConfig.ModelLineConfig] that tells the
   * pipeline how to read the raw-impression Parquet columns [RawImpressionsWriter] emits (see
   * [RawImpressionColumns]). The dispatcher copies these mappings onto the phase params, so the
   * columns named here MUST match the writer exactly.
   */
  private fun buildModelLineConfig(): VidLabelingConfig.ModelLineConfig =
    VidLabelingConfig.ModelLineConfig.newBuilder()
      .apply {
        // LabelerInput projection (what the model sees). event_id.id is required and also feeds
        // RawImpressionSource's shard fingerprint.
        addLabelerInputFieldMapping(
          LabelerInputFieldMapping.newBuilder()
            .setFieldPath("event_id.id")
            .setScalar(ScalarColumn.newBuilder().setColumn(RawImpressionColumns.EVENT_ID))
            .build()
        )
        addLabelerInputFieldMapping(
          LabelerInputFieldMapping.newBuilder()
            .setFieldPath("timestamp_usec")
            .setScalar(ScalarColumn.newBuilder().setColumn(RawImpressionColumns.EVENT_TIME_USEC))
            .build()
        )
        // Identity: the person id drives the model's ID-space routing and VID assignment.
        addLabelerInputFieldMapping(
          LabelerInputFieldMapping.newBuilder()
            .setFieldPath("profile_info.proprietary_id_space_1_user_info.user_id")
            .setScalar(ScalarColumn.newBuilder().setColumn(RawImpressionColumns.PERSON_ID))
            .build()
        )
        // Demographics the reference model routes on: gender (TestEvent name -> VirtualPeople enum)
        // and age (TestEvent bucket -> {min,max} matching the model's 16-34 / 35-54 / 55-99
        // buckets).
        addLabelerInputFieldMapping(
          LabelerInputFieldMapping.newBuilder()
            .setFieldPath("profile_info.proprietary_id_space_1_user_info.demo.demo_bucket.gender")
            .setEnumLookup(
              EnumLookup.newBuilder()
                .setColumn(RawImpressionColumns.PERSON_GENDER)
                .putLookupTable("MALE", "GENDER_MALE")
                .putLookupTable("FEMALE", "GENDER_FEMALE")
            )
            .build()
        )
        addLabelerInputFieldMapping(
          LabelerInputFieldMapping.newBuilder()
            .setFieldPath("profile_info.proprietary_id_space_1_user_info.demo.demo_bucket.age")
            .setAgeRange(
              AgeRange.newBuilder()
                .setBucketLookup(
                  BucketLookup.newBuilder()
                    .setColumn(RawImpressionColumns.PERSON_AGE_GROUP)
                    .putBucketTable("YEARS_18_TO_34", ageBucket(16, 34))
                    .putBucketTable("YEARS_35_TO_54", ageBucket(35, 54))
                    .putBucketTable("YEARS_55_PLUS", ageBucket(55, 99))
                )
            )
            .build()
        )
        // Output event template (TestEvent) so downstream measurement can slice by demographics.
        putEventTemplateFieldMapping("person.gender", RawImpressionColumns.PERSON_GENDER)
        putEventTemplateFieldMapping("person.age_group", RawImpressionColumns.PERSON_AGE_GROUP)
        // Entity keys read per row from the raw-impression columns SeedRawImpressionsRule emits
        // (see SeedRawImpressionsRule.entityKeyColumnsFor). Every file carries the person id
        // (person_id, also the model identity column); entity-key event groups additionally carry
        // the creative id (creative_id), which the fulfiller matches those groups by. Both are
        // OPTIONAL because the legacy group's files have no creative_id column; EntityKeyMapper
        // still requires >=1 key per impression, which every file supplies via person_id. The
        // person key is ignored for legacy groups (matched by event_group_reference_id) and is a
        // harmless non-matching extra on entity-key groups.
        putOptionalEntityKeyFieldMapping(
          RawImpressionColumns.ENTITY_TYPE_PERSON,
          RawImpressionColumns.PERSON_ID,
        )
        putOptionalEntityKeyFieldMapping(
          RawImpressionColumns.ENTITY_TYPE_CREATIVE_ID,
          RawImpressionColumns.CREATIVE_ID,
        )
        eventTemplateDescriptorBlobUri = EVENT_TEMPLATE_DESCRIPTOR_BLOB_URI
        eventTemplateType = EVENT_TEMPLATE_TYPE
      }
      .build()

  private fun ageBucket(minAge: Int, maxAge: Int): AgeBucket =
    AgeBucket.newBuilder().setMinAge(minAge).setMaxAge(maxAge).build()

  private fun buildChannel(certFileName: String, privateKeyFileName: String): ManagedChannel {
    val secretFiles: Path =
      checkNotNull(getRuntimePath(WORKSPACE_PATH.resolve(SECRET_FILES_PATH))) {
        "secretfiles runtime path not found"
      }
    val signingCerts =
      SigningCerts.fromPemFiles(
        secretFiles.resolve(certFileName).toFile(),
        secretFiles.resolve(privateKeyFileName).toFile(),
        secretFiles.resolve(KINGDOM_TRUSTED_CERTS_FILE).toFile(),
      )
    return buildMutualTlsChannel(kingdomPublicApiTarget, signingCerts, kingdomPublicApiCertHost)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")
    private val SECRET_FILES_PATH: Path = Paths.get("src", "main", "k8s", "testing", "secretfiles")
    private const val KINGDOM_TRUSTED_CERTS_FILE = "kingdom_root.pem"

    private const val LIST_PAGE_SIZE = 100

    // Stable display names anchor the get-only lookup in the shared suite.
    private const val MEMOIZED_LINE_DISPLAY_NAME = "cloudtest-memoized"
    private const val NON_MEMOIZED_LINE_1_DISPLAY_NAME = "cloudtest-nonmemoized-1"
    private const val NON_MEMOIZED_LINE_2_DISPLAY_NAME = "cloudtest-nonmemoized-2"

    private fun env(name: String): String = System.getenv(name).orEmpty()

    private val MODEL_SUITE: String = env("MODEL_SUITE")
    private val NON_MEMOIZED_MODEL_BLOB_URI: String = env("NON_MEMOIZED_MODEL_BLOB_URI")
    private val EVENT_TEMPLATE_DESCRIPTOR_BLOB_URI: String =
      env("EVENT_TEMPLATE_DESCRIPTOR_BLOB_URI")
    private val EVENT_TEMPLATE_TYPE: String = env("EVENT_TEMPLATE_TYPE")
  }
}
