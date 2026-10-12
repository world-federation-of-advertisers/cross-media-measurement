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

import com.google.cloud.storage.StorageOptions
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.TypeRegistry
import com.google.protobuf.timestamp
import com.google.type.interval
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Clock
import java.time.LocalDate
import java.time.ZoneId
import java.util.logging.Logger
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.ClassRule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.measurement.integration.k8s.testing.EdpaReportingIntegrationTestConfig
import org.measurement.integration.k8s.testing.HighOverlapDatasetManifest
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.measurement.integration.k8s.testing.highOverlapDatasetManifest
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.integration.common.EventGroupConfig
import org.wfanet.measurement.integration.common.HighOverlapDataset
import org.wfanet.measurement.integration.common.ImpressionTestDataConfigs
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Seeds the high overlap synthetic dataset for one ModelLine, then checks what it wrote.
 *
 * This is seeding, not coverage: the rules below do the writing and the single test case reads the
 * manifest back. It is tagged `manual` and `external` so it never runs in an ordinary build and its
 * result is never cached — invoke it by label, from `update-cmms` with `run-seeding`.
 *
 * Run it when the dataset's specs change or an environment gains an EDP, not on every test run:
 * what it writes persists and the data it produces is deterministic.
 * `EdpAggregatorReportingIntegrationTest` reads the manifest this leaves behind and refuses to
 * report over a dataset that no longer matches its specs.
 */
class HighOverlapSeedingTest {

  @Test
  fun `manifest describes the seeded dataset`() {
    if (MODEL_LINE.isEmpty()) {
      logger.info("No high overlap model line configured; nothing was seeded.")
      return
    }
    val manifest = runBlocking { readManifest() }

    assertThat(manifest.specFingerprint)
      .isEqualTo(HighOverlapDataset.fingerprint(IMPRESSION_TEST_DATA_CONFIG, POPULATION_SPEC))
    assertThat(manifest.seededEdpsList).containsExactlyElementsIn(EDP_NAMES)
    assertThat(manifest.modelLine).isEqualTo(MODEL_LINE)
  }

  /** Uploads the high overlap EventGroups and waits for `EventGroupSync` to pick them up. */
  private class UploadEventGroups : TestRule {

    private val bucket = TEST_CONFIG.storageBucket
    private val googleProjectId: String = googleProjectId()
    private val storageClient = StorageOptions.getDefaultInstance().service

    /**
     * Per-EDP blobs for the high overlap EventGroups.
     *
     * [objectKey] is a distinct object under the EDP's `event-groups/` prefix, which is what the
     * DataWatcher matches on; `EventGroupSync` reads whichever blob triggered it. [objectMapKey] is
     * not derived from it: the sync always writes its output to the `event_group_map_blob_uri`
     * fixed per EDP in `EventGroupSyncConfig`, so it is shared with every other test seeding this
     * EDP.
     */
    private data class EdpStorage(
      val objectMapKey: String,
      val objectKey: String,
      val blobUri: String,
      val eventGroupReferenceIds: Set<String>,
    )

    private val edpStorageList: List<EdpStorage> =
      HighOverlapDataset.eventGroupRefIdsByEdp(PROVISIONED_CONFIG).map { (edpName, referenceIds) ->
        EdpStorage(
          objectMapKey = "$edpName/event-groups-map/$edpName-event-group.binpb",
          objectKey = "$edpName/event-groups/$edpName-high-overlap-event-group.binpb",
          blobUri = "gs://$bucket/$edpName/event-groups/$edpName-high-overlap-event-group.binpb",
          eventGroupReferenceIds = referenceIds,
        )
      }

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          if (MODEL_LINE.isEmpty()) {
            logger.info("No high overlap model line configured; skipping EventGroup upload.")
          } else {
            runBlocking {
              edpStorageList.forEach { storageClient.delete(bucket, it.objectMapKey) }
              val allEventGroups =
                buildEventGroups(ImpressionTestDataConfigs.toEventGroupMap(PROVISIONED_CONFIG))
              for (edpStorage in edpStorageList) {
                val groups =
                  allEventGroups.filter {
                    it.eventGroupReferenceId in edpStorage.eventGroupReferenceIds
                  }
                uploadEventGroups(edpStorage, groups)
                waitForEventGroupSyncToComplete(edpStorage)
              }
              logger.info("High overlap EventGroup sync completed.")
            }
          }
          base.evaluate()
        }
      }
    }

    private suspend fun waitForEventGroupSyncToComplete(storage: EdpStorage) {
      withTimeout(EVENT_GROUP_SYNC_TIMEOUT) {
        while (storageClient.get(bucket, storage.objectMapKey) == null) {
          logger.info("Waiting on high overlap Event Group Sync to complete...")
          delay(EVENT_GROUP_SYNC_POLLING_INTERVAL)
        }
      }
    }

    private suspend fun uploadEventGroups(storage: EdpStorage, eventGroups: List<EventGroup>) {
      val eventGroupsBlobUri = SelectedStorageClient.parseBlobUri(storage.blobUri)
      MesosRecordIoStorageClient(
          SelectedStorageClient(
            blobUri = eventGroupsBlobUri,
            rootDirectory = null,
            projectId = googleProjectId,
          )
        )
        .writeBlob(storage.objectKey, eventGroups.asFlow().map { it.toByteString() })
    }

    private fun buildEventGroups(eventGroupMap: Map<String, EventGroupConfig>): List<EventGroup> {
      return eventGroupMap.flatMap { (referenceId, config) ->
        when (config) {
          // Every high overlap event group carries an entity key: EventGroupSync filters its
          // Kingdom listing by entity type, so one without a key would be re-created on every sync.
          is EventGroupConfig.LegacySpec ->
            error("high overlap event group $referenceId has no entity key")
          // One EventGroup per entity key, spanning every date spec. The Kingdom enforces
          // uniqueness on both reference ID and entity key, so a row per date spec would
          // collide.
          is EventGroupConfig.MultiEntityKey ->
            config.entityKeySpecs.map { entityKeySpec ->
              val dateRanges = entityKeySpec.spec.dateSpecsList.map { it.dateRange }
              val startTime =
                dateRanges
                  .minOf { LocalDate.of(it.start.year, it.start.month, it.start.day) }
                  .atStartOfDay(ZONE_ID)
                  .toInstant()
              // Both bounds are exclusive: the date spec's and the interval's.
              val endTime =
                dateRanges.maxOf { it.endExclusive.toLocalDate() }.atStartOfDay(ZONE_ID).toInstant()
              eventGroup {
                eventGroupReferenceId =
                  "${entityKeySpec.entityKey.entityType}-${entityKeySpec.entityKey.entityId}"
                measurementConsumer = TEST_CONFIG.measurementConsumer
                dataAvailabilityInterval = interval {
                  this.startTime = timestamp { seconds = startTime.epochSecond }
                  this.endTime = timestamp { seconds = endTime.epochSecond }
                }
                eventGroupMetadata = eventGroupMetadata {
                  adMetadata = adMetadata {
                    campaignMetadata = campaignMetadata {
                      brand = "some-brand"
                      campaign = "some-campaign"
                    }
                  }
                  entityKeySpec.entityMetadata?.let { entityMetadata = it }
                }
                entityKey = entityKey {
                  entityType = entityKeySpec.entityKey.entityType
                  entityId = entityKeySpec.entityKey.entityId
                }
                // Every segment alternates media per stripe.
                mediaTypes += listOf(MediaType.VIDEO, MediaType.DISPLAY)
              }
            }
        }
      }
    }

    companion object {
      // Inferred as kotlin.time.Duration, which withTimeout and delay take.
      private val EVENT_GROUP_SYNC_TIMEOUT = 5.minutes
      private val EVENT_GROUP_SYNC_POLLING_INTERVAL = 5.seconds
    }
  }

  /**
   * Writes the `done` markers that trigger `DataAvailabilitySync` for the high overlap impressions.
   */
  private class CreateDoneBlobs : TestRule {

    private val bucket = TEST_CONFIG.storageBucket
    private val googleProjectId: String = googleProjectId()

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          if (MODEL_LINE.isEmpty()) {
            logger.info("No high overlap model line configured; skipping DONE blobs.")
          } else {
            runBlocking {
              val paths =
                HighOverlapDataset.datesByImpressionPath(PROVISIONED_CONFIG).flatMap {
                  (impressionPath, dates) ->
                  dates.map { date ->
                    "gs://$bucket/$impressionPath/model-line/$MODEL_LINE_ID/$date/done"
                  }
                }
              logger.info("Creating ${paths.size} high overlap DONE blob(s)...")
              writeDoneBlobs(paths)
            }
          }
          base.evaluate()
        }
      }
    }

    private suspend fun writeDoneBlobs(paths: List<String>) {
      paths.forEach { path ->
        val doneBlobUri = SelectedStorageClient.parseBlobUri(path)
        val selectedStorageClient =
          SelectedStorageClient(
            blobUri = doneBlobUri,
            rootDirectory = null,
            projectId = googleProjectId,
          )
        selectedStorageClient.getBlob(doneBlobUri.key)?.delete()
        selectedStorageClient.writeBlob(doneBlobUri.key, emptyFlow())
      }
    }
  }

  /**
   * Records what was seeded, last.
   *
   * Only a run that reached this point describes data a reader can rely on, so a run that fails
   * partway leaves the previous manifest in place and the reporting test reports the older
   * fingerprint rather than trusting half a dataset.
   */
  private class WriteManifest : TestRule {

    private val googleProjectId: String = googleProjectId()

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          if (MODEL_LINE.isEmpty()) {
            logger.info("No high overlap model line configured; skipping the manifest.")
          } else {
            runBlocking { write() }
          }
          base.evaluate()
        }
      }
    }

    private suspend fun write() {
      val manifest = highOverlapDatasetManifest {
        specFingerprint =
          HighOverlapDataset.fingerprint(IMPRESSION_TEST_DATA_CONFIG, POPULATION_SPEC)
        seededEdps += EDP_NAMES.sorted()
        modelLine = MODEL_LINE
        generateTime = timestamp { seconds = Clock.systemUTC().instant().epochSecond }
      }
      val key = HighOverlapDataset.manifestBlobKey(MODEL_LINE_ID)
      val blobUri = SelectedStorageClient.parseBlobUri("gs://${TEST_CONFIG.storageBucket}/$key")
      val client =
        SelectedStorageClient(blobUri = blobUri, rootDirectory = null, projectId = googleProjectId)
      client.getBlob(key)?.delete()
      client.writeBlob(key, listOf(manifest.toByteString()).asFlow())
      logger.info("Wrote dataset manifest ${manifest.specFingerprint} for $MODEL_LINE")
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val ZONE_ID = ZoneId.of("UTC")

    // Declared before TEST_CONFIG: reading it resolves a runtime path, so these must already be
    // initialized by the time any eager property forces it.
    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")
    private val CONFIG_PATH: Path =
      Paths.get("src", "test", "kotlin", "org", "wfanet", "measurement", "integration", "k8s")
    private const val TEST_CONFIG_NAME = "edpa_reporting_integration_test_config.textproto"

    /** Shared with `EdpAggregatorReportingIntegrationTest`: one environment, one description. */
    val TEST_CONFIG: EdpaReportingIntegrationTestConfig by lazy {
      val configFile = runtimePath(CONFIG_PATH.resolve(TEST_CONFIG_NAME)).toFile()
      parseTextProto(configFile, EdpaReportingIntegrationTestConfig.getDefaultInstance())
    }

    /**
     * Resource name of the high overlap ModelLine, or empty where the dataset is not provisioned.
     */
    val MODEL_LINE: String = TEST_CONFIG.modelLine

    private val MODEL_LINE_ID: String by lazy {
      requireNotNull(ModelLineKey.fromName(MODEL_LINE)) {
          "model_line must be a full ModelLine resource name: $MODEL_LINE"
        }
        .modelLineId
    }

    private val POPULATION_SPEC_TYPE_REGISTRY: TypeRegistry =
      TypeRegistry.newBuilder().add(Common.getDescriptor()).build()

    private val IMPRESSION_TEST_DATA_CONFIG: ImpressionTestDataConfig by lazy {
      parseTextProto(
        ImpressionTestDataConfigs.resolveSpecPath(
          "high_overlap_impression_test_data_config.textproto"
        ),
        ImpressionTestDataConfig.getDefaultInstance(),
      )
    }

    private val POPULATION_SPEC: PopulationSpec by lazy {
      parseTextProto(
        ImpressionTestDataConfigs.resolveSpecPath(
          IMPRESSION_TEST_DATA_CONFIG.populationSpecResourcePath
        ),
        PopulationSpec.getDefaultInstance(),
        POPULATION_SPEC_TYPE_REGISTRY,
      )
    }

    /** EDPs this seeding covers: every one the high overlap dataset defines. */
    private val EDP_NAMES: Set<String> =
      setOf("edp7", "edpa_meta", "edpa_video_pub", "edpa_retail_pub")

    /** Config restricted to [EDP_NAMES], empty when the dataset is not configured. */
    private val PROVISIONED_CONFIG: ImpressionTestDataConfig by lazy {
      if (MODEL_LINE.isEmpty()) {
        ImpressionTestDataConfig.getDefaultInstance()
      } else {
        IMPRESSION_TEST_DATA_CONFIG.toBuilder()
          .clearEventGroups()
          .addAllEventGroups(
            IMPRESSION_TEST_DATA_CONFIG.eventGroupsList.filter { it.edpName in EDP_NAMES }
          )
          .build()
      }
    }

    private fun googleProjectId(): String =
      System.getenv("GOOGLE_CLOUD_PROJECT") ?: error("GOOGLE_CLOUD_PROJECT must be set")

    /** Resolves a workspace-relative path in the test's runfiles. */
    private fun runtimePath(workspaceRelativePath: Path): Path =
      checkNotNull(getRuntimePath(WORKSPACE_PATH.resolve(workspaceRelativePath))) {
        "Runtime path not found for $workspaceRelativePath"
      }

    private suspend fun readManifest(): HighOverlapDatasetManifest {
      val key = HighOverlapDataset.manifestBlobKey(MODEL_LINE_ID)
      val blobUri = SelectedStorageClient.parseBlobUri("gs://${TEST_CONFIG.storageBucket}/$key")
      val blob =
        SelectedStorageClient(
            blobUri = blobUri,
            rootDirectory = null,
            projectId = googleProjectId(),
          )
          .getBlob(key) ?: error("Seeding wrote no manifest at $key")
      return HighOverlapDatasetManifest.parseFrom(blob.read().flatten())
    }

    private val modelResources =
      HighOverlapModelResourcesRule(
        populationSpecProvider = { POPULATION_SPEC },
        populationDataProvider = TEST_CONFIG.populationDataProvider,
        modelLineName = MODEL_LINE,
        earliestEventDateProvider = {
          HighOverlapDataset.datesByImpressionPath(PROVISIONED_CONFIG).values.flatten().min()
        },
        kingdomPublicApiTarget = TEST_CONFIG.kingdomPublicApiTarget,
        kingdomPublicApiCertHost = TEST_CONFIG.kingdomPublicApiCertHost.ifEmpty { null },
      )
    private val uploadEventGroups = UploadEventGroups()
    private val writeImpressions =
      WriteHighOverlapImpressionsRule(
        configProvider = { PROVISIONED_CONFIG },
        populationSpecProvider = { POPULATION_SPEC },
        bucket = TEST_CONFIG.storageBucket,
        modelLineProvider = { MODEL_LINE.ifEmpty { null } },
        edp7KekUri = TEST_CONFIG.edp7KekUri,
        edpaMetaKekUri = TEST_CONFIG.edpaMetaKekUri,
        edpaMetaAwsRoleArn = TEST_CONFIG.edpaMetaAwsRoleArn,
        edpaMetaAwsRegion = TEST_CONFIG.edpaMetaAwsRegion,
      )
    private val createDoneBlobs = CreateDoneBlobs()
    private val writeManifest = WriteManifest()

    @ClassRule
    @JvmField
    val chainedRule =
      chainRulesSequentially(
        modelResources,
        uploadEventGroups,
        writeImpressions,
        createDoneBlobs,
        writeManifest,
      )
  }
}
