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
import com.google.cloud.storage.StorageOptions
import com.google.protobuf.TextFormat
import io.grpc.ManagedChannel
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelShardKt
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt.ModelShardsCoroutineStub
import org.wfanet.measurement.api.v2alpha.createModelLineRequest
import org.wfanet.measurement.api.v2alpha.createModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.createModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.createModelShardRequest
import org.wfanet.measurement.api.v2alpha.listModelLinesRequest
import org.wfanet.measurement.api.v2alpha.listModelReleasesRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.listModelShardsRequest
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelShard
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfig
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigs
import org.wfanet.measurement.edpaggregator.v1alpha.AgeBucket
import org.wfanet.measurement.edpaggregator.v1alpha.AgeRange
import org.wfanet.measurement.edpaggregator.v1alpha.BucketLookup
import org.wfanet.measurement.edpaggregator.v1alpha.EnumLookup
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.ScalarColumn
import org.wfanet.measurement.loadtest.edpaggregator.testing.RawImpressionColumns

/**
 * JUnit [TestRule] that idempotently provisions the Kingdom model resources the VID Labeling pipeline
 * needs for the pipelined EDP, and points the deployed dispatcher/monitor config at them.
 *
 * All operations are get-or-create so the rule is safe to run on every invocation and reuses whatever
 * a previous run created (Kingdom model resources are not deletable). Model resources have no
 * `request_id`, so idempotency is anchored on the model line's stable `display_name`:
 * ```
 * model line (by display_name) -> its ModelRollout -> that rollout's ModelRelease -> ModelShard(edp, release)
 * ```
 *
 * Three model lines are provisioned under the existing suite:
 * * one memoized (its ModelShard has `memoized_vid_assignment_enabled = true`, pointing at the
 *   memoized compiled model blob), and
 * * two non-memoized (shards `false`, pointing at the hash-only model blob).
 *
 * Memoization is decided by the [ModelShard], not by config, so the dispatcher resolves it from the
 * Kingdom by (data provider, release).
 *
 * Authentication is pure mutual-TLS: the model provider identity (`mp1_*` certs) creates the model
 * lines/releases/rollouts and the ModelShards (the Kingdom authorizes CreateModelShard only for
 * the ModelProvider that owns the ModelRelease, not the DataProvider). The
 * Kingdom maps the client cert's authority-key-identifier to the principal, so no API key or
 * principal override is used.
 *
 * Env-provided inputs (set by the cloud-test workflow); if [MODEL_SUITE] is unset the rule is a no-op
 * so unrelated runs are unaffected:
 * * `MODEL_SUITE` — the ModelSuite resource name to create the lines under.
 * * `PIPELINED_DATA_PROVIDER` — the EDP that runs the pipeline (owns the shards).
 * * `MEMOIZED_MODEL_BLOB_URI` / `NON_MEMOIZED_MODEL_BLOB_URI` — compiled Riegeli model blobs.
 * * `EVENT_TEMPLATE_DESCRIPTOR_BLOB_URI` / `EVENT_TEMPLATE_TYPE` — event schema for the label output.
 * * `EDPA_CONFIG_STORAGE_BUCKET` — bucket holding the dispatcher/monitor config textprotos.
 */
class VidLabelerModelResourcesRule(
  private val kingdomPublicApiTarget: String,
  private val kingdomPublicApiCertHost: String?,
) : TestRule {

  /**
   * Resource name of the memoized cloudtest model line, populated by [provision]. The cloud
   * test points its measurement at this line (which the pipeline labels) rather than the shared
   * `MODEL_LINE_NAME` var (also used by SyntheticGeneratorCorrectnessTest). Null if provisioning
   * was skipped.
   */
  var memoizedModelLine: String? = null
    private set

  /**
   * Resource name of the first non-memoized (hash-only) cloudtest model line, populated by
   * [provision]. The cloud test points its measurements at this line: its VID assignment is a pure
   * function of the impression (static model + hash), so it is deterministically reproducible
   * offline, unlike the memoized line. Null if provisioning was skipped.
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

  private data class LineSpec(
    val displayName: String,
    val memoized: Boolean,
    val modelBlobUri: String,
  )

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        if (MODEL_SUITE.isEmpty()) {
          logger.warning("MODEL_SUITE unset; skipping VID Labeling model-resource provisioning.")
        } else {
          runBlocking { provision() }
        }
        base.evaluate()
      }
    }
  }

  private suspend fun provision() {
    val modelProviderChannel: ManagedChannel = buildChannel("mp1_tls.pem", "mp1_tls.key")
    try {
      val modelLinesStub = ModelLinesCoroutineStub(modelProviderChannel)
      val modelReleasesStub = ModelReleasesCoroutineStub(modelProviderChannel)
      val modelRolloutsStub = ModelRolloutsCoroutineStub(modelProviderChannel)
      val modelShardsStub = ModelShardsCoroutineStub(modelProviderChannel)

      val population: String = resolveExistingPopulation(modelReleasesStub)
      val activeStartTime: Instant = ACTIVE_START_DATE.atStartOfDay(ZoneOffset.UTC).toInstant()

      val lineSpecs =
        listOf(
          LineSpec(MEMOIZED_LINE_DISPLAY_NAME, memoized = true, MEMOIZED_MODEL_BLOB_URI),
          LineSpec(NON_MEMOIZED_LINE_1_DISPLAY_NAME, memoized = false, NON_MEMOIZED_MODEL_BLOB_URI),
          LineSpec(NON_MEMOIZED_LINE_2_DISPLAY_NAME, memoized = false, NON_MEMOIZED_MODEL_BLOB_URI),
        )

      val modelLineNames = mutableListOf<String>()
      for (spec in lineSpecs) {
        val line = getOrCreateModelLine(modelLinesStub, spec.displayName, activeStartTime)
        val releaseName =
          getOrCreateReleaseForLine(modelRolloutsStub, modelReleasesStub, line, population)
        getOrCreateModelShard(modelShardsStub, releaseName, spec.modelBlobUri, spec.memoized)
        modelLineNames.add(line.name)
        if (spec.memoized) memoizedModelLine = line.name
        else if (nonMemoizedModelLine == null) nonMemoizedModelLine = line.name
        logger.info("Provisioned model line ${line.name} (memoized=${spec.memoized}).")
      }

      updateVidLabelingConfigs(modelLineNames)
    } finally {
      modelProviderChannel.shutdown()
    }
  }

  /** Reuses an existing [population][ModelRelease.getPopulation] from the suite for new releases. */
  private suspend fun resolveExistingPopulation(stub: ModelReleasesCoroutineStub): String {
    val release =
      stub
        .listModelReleases(
          listModelReleasesRequest {
            parent = MODEL_SUITE
            pageSize = 1
          }
        )
        .modelReleasesList
        .firstOrNull()
    return checkNotNull(release?.population) {
      "No existing ModelRelease under $MODEL_SUITE to reuse a Population from; create one first."
    }
  }

  private suspend fun getOrCreateModelLine(
    stub: ModelLinesCoroutineStub,
    displayName: String,
    activeStartTime: Instant,
  ): ModelLine {
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
      response.modelLinesList.firstOrNull { it.displayName == displayName }?.let {
        return it
      }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())

    return stub.createModelLine(
      createModelLineRequest {
        parent = MODEL_SUITE
        modelLine =
          modelLine {
            this.displayName = displayName
            this.activeStartTime = activeStartTime.toProtoTime()
            type = ModelLine.Type.PROD
          }
      }
    )
  }

  /**
   * Returns the ModelRelease resource name backing [line], creating a release + rollout if the line
   * has none yet. The line's existing rollout is the idempotency anchor for the release.
   */
  private suspend fun getOrCreateReleaseForLine(
    rolloutsStub: ModelRolloutsCoroutineStub,
    releasesStub: ModelReleasesCoroutineStub,
    line: ModelLine,
    population: String,
  ): String {
    val existingRollout =
      rolloutsStub
        .listModelRollouts(
          listModelRolloutsRequest {
            parent = line.name
            pageSize = 1
          }
        )
        .modelRolloutsList
        .firstOrNull()
    if (existingRollout != null) {
      return existingRollout.modelRelease
    }

    val release =
      releasesStub.createModelRelease(
        createModelReleaseRequest {
          parent = MODEL_SUITE
          modelRelease = modelRelease { this.population = population }
        }
      )
    rolloutsStub.createModelRollout(
      createModelRolloutRequest {
        parent = line.name
        modelRollout =
          modelRollout {
            modelRelease = release.name
            instantRolloutDate = ACTIVE_START_DATE.toProtoDate()
          }
      }
    )
    return release.name
  }

  private suspend fun getOrCreateModelShard(
    stub: ModelShardsCoroutineStub,
    releaseName: String,
    modelBlobUri: String,
    memoized: Boolean,
  ) {
    var pageToken = ""
    do {
      val response =
        stub.listModelShards(
          listModelShardsRequest {
            parent = PIPELINED_DATA_PROVIDER
            pageSize = LIST_PAGE_SIZE
            this.pageToken = pageToken
          }
        )
      if (response.modelShardsList.any { it.modelRelease == releaseName }) {
        return
      }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())

    stub.createModelShard(
      createModelShardRequest {
        parent = PIPELINED_DATA_PROVIDER
        modelShard =
          modelShard {
            modelRelease = releaseName
            modelBlob = ModelShardKt.modelBlob { modelBlobPath = modelBlobUri }
            memoizedVidAssignmentEnabled = memoized
          }
      }
    )
  }

  /**
   * Merges a [ModelLineConfig][VidLabelingConfig.ModelLineConfig] for each provisioned line into the
   * deployed dispatcher and monitor `VidLabelingConfigs` textprotos (keyed by model line resource
   * name), preserving every other field. Best-effort: logs and continues on any failure so the
   * model-resource provisioning above is not lost.
   */
  private fun updateVidLabelingConfigs(modelLineNames: List<String>) {
    for (blobKey in listOf(DISPATCHER_CONFIG_BLOB_KEY, MONITOR_CONFIG_BLOB_KEY)) {
      try {
        updateConfigBlob(blobKey, modelLineNames)
      } catch (e: Exception) {
        logger.warning("Failed to update $blobKey in $CONFIG_BUCKET: ${e.message}")
      }
    }
  }

  private fun updateConfigBlob(blobKey: String, modelLineNames: List<String>) {
    val storage = StorageOptions.getDefaultInstance().service
    val blobId = BlobId.of(CONFIG_BUCKET, blobKey)
    val bytes = storage.readAllBytes(blobId)
    val configs =
      VidLabelingConfigs.newBuilder()
        .apply { TextFormat.merge(String(bytes, Charsets.UTF_8), this) }
        .build()

    val builder = configs.toBuilder()
    var updated = false
    for (index in 0 until builder.configsCount) {
      val config = builder.getConfigs(index)
      if (config.dataProvider != PIPELINED_DATA_PROVIDER) continue
      val configBuilder = config.toBuilder()
      for (modelLineName in modelLineNames) {
        configBuilder.putModelLineConfigs(modelLineName, buildModelLineConfig())
      }
      builder.setConfigs(index, configBuilder)
      updated = true
    }
    if (!updated) {
      logger.warning(
        "No VidLabelingConfig for $PIPELINED_DATA_PROVIDER in $blobKey; leaving it unchanged."
      )
      return
    }
    storage.create(
      BlobInfo.newBuilder(blobId).build(),
      TextFormat.printer().printToString(builder.build()).toByteArray(Charsets.UTF_8),
    )
    logger.info("Updated $blobKey with ${modelLineNames.size} model_line_configs.")
  }

  /**
   * Builds the operator [ModelLineConfig][VidLabelingConfig.ModelLineConfig] that tells the pipeline
   * how to read the raw-impression Parquet columns [RawImpressionsWriter] emits (see
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
        // and age (TestEvent bucket -> {min,max} matching the model's 16-34 / 35-54 / 55-99 buckets).
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
    private val SECRET_FILES_PATH: Path =
      Paths.get("src", "main", "k8s", "testing", "secretfiles")
    private const val KINGDOM_TRUSTED_CERTS_FILE = "kingdom_root.pem"

    private const val LIST_PAGE_SIZE = 100

    // Stable display names anchor idempotent get-or-create in the shared suite.
    private const val MEMOIZED_LINE_DISPLAY_NAME = "cloudtest-memoized"
    private const val NON_MEMOIZED_LINE_1_DISPLAY_NAME = "cloudtest-nonmemoized-1"
    private const val NON_MEMOIZED_LINE_2_DISPLAY_NAME = "cloudtest-nonmemoized-2"

    // Matches the direct-path DataAvailabilitySync date range used by the correctness test.
    private val ACTIVE_START_DATE: LocalDate = LocalDate.parse("2021-03-15")

    // Deployed config blob object keys (see terraform edp-aggregator module).
    private const val DISPATCHER_CONFIG_BLOB_KEY = "vid-labeling-dispatcher-config.textproto"
    private const val MONITOR_CONFIG_BLOB_KEY = "vid-labeling-monitor-config.textproto"

    private fun env(name: String): String = System.getenv(name).orEmpty()

    private val MODEL_SUITE: String = env("MODEL_SUITE")
    private val PIPELINED_DATA_PROVIDER: String = env("PIPELINED_DATA_PROVIDER")
    private val MEMOIZED_MODEL_BLOB_URI: String = env("MEMOIZED_MODEL_BLOB_URI")
    private val NON_MEMOIZED_MODEL_BLOB_URI: String = env("NON_MEMOIZED_MODEL_BLOB_URI")
    private val EVENT_TEMPLATE_DESCRIPTOR_BLOB_URI: String = env("EVENT_TEMPLATE_DESCRIPTOR_BLOB_URI")
    private val EVENT_TEMPLATE_TYPE: String = env("EVENT_TEMPLATE_TYPE")
    private val CONFIG_BUCKET: String = env("EDPA_CONFIG_STORAGE_BUCKET")
  }
}
