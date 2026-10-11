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

package org.wfanet.measurement.integration.common

import com.google.cloud.storage.StorageOptions
import com.google.protobuf.timestamp
import com.google.type.interval
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
import kotlinx.coroutines.withTimeout
import org.measurement.integration.k8s.testing.HighOverlapDatasetManifest
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.measurement.integration.k8s.testing.highOverlapDatasetManifest
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Writes the whole high overlap dataset for one ModelLine: model resources, EventGroups,
 * impressions, `done` markers, and a manifest recording what was written.
 *
 * Seeding is idempotent and the dataset is deterministic, so this is a job rather than test setup —
 * a test run reports over whatever an earlier seeding left behind. [HighOverlapDatasetManifest] is
 * what lets a test tell "left behind by this revision of the specs" from "left behind by an older
 * one".
 *
 * @param config the EDPs to seed, already restricted to those the caller provisions
 */
class HighOverlapSeeder(
  private val config: ImpressionTestDataConfig,
  private val populationSpec: PopulationSpec,
  private val bucket: String,
  private val modelLine: String,
  private val measurementConsumer: String,
  private val googleProjectId: String,
  private val modelResources: HighOverlapModelResources,
  private val impressionWriter: HighOverlapImpressionWriter,
  private val clock: Clock = Clock.systemUTC(),
) {
  private val storageClient = StorageOptions.getDefaultInstance().service

  /**
   * Per-EDP blobs for the high overlap EventGroups.
   *
   * [objectKey] is a distinct object under the EDP's `event-groups/` prefix, which is what the
   * DataWatcher matches on; `EventGroupSync` reads whichever blob triggered it. [objectMapKey] is
   * not derived from it: the sync always writes its output to the `event_group_map_blob_uri` fixed
   * per EDP in `EventGroupSyncConfig`, so it is shared with every other test seeding this EDP.
   */
  private data class EdpStorage(
    val objectMapKey: String,
    val objectKey: String,
    val blobUri: String,
    val eventGroupReferenceIds: Set<String>,
  )

  /** Seeds the dataset and returns the manifest it wrote. */
  suspend fun seed(): HighOverlapDatasetManifest {
    require(modelLine.isNotEmpty()) { "model_line is required to seed the high overlap dataset" }
    modelResources.provision()
    uploadEventGroups()
    impressionWriter.write(modelLine)
    createDoneBlobs()
    return writeManifest()
  }

  private suspend fun uploadEventGroups() {
    val edpStorageList: List<EdpStorage> =
      HighOverlapDataset.eventGroupRefIdsByEdp(config).map { (edpName, referenceIds) ->
        EdpStorage(
          objectMapKey = "$edpName/event-groups-map/$edpName-event-group.binpb",
          objectKey = "$edpName/event-groups/$edpName-high-overlap-event-group.binpb",
          blobUri = "gs://$bucket/$edpName/event-groups/$edpName-high-overlap-event-group.binpb",
          eventGroupReferenceIds = referenceIds,
        )
      }
    edpStorageList.forEach { storageClient.delete(bucket, it.objectMapKey) }
    val allEventGroups = buildEventGroups(ImpressionTestDataConfigs.toEventGroupMap(config))
    for (edpStorage in edpStorageList) {
      val groups =
        allEventGroups.filter { it.eventGroupReferenceId in edpStorage.eventGroupReferenceIds }
      writeEventGroups(edpStorage, groups)
      awaitEventGroupSync(edpStorage)
    }
    logger.info("High overlap EventGroup sync completed.")
  }

  private suspend fun awaitEventGroupSync(storage: EdpStorage) {
    withTimeout(EVENT_GROUP_SYNC_TIMEOUT) {
      while (storageClient.get(bucket, storage.objectMapKey) == null) {
        logger.info("Waiting on high overlap Event Group Sync to complete...")
        delay(EVENT_GROUP_SYNC_POLLING_INTERVAL)
      }
    }
  }

  private suspend fun writeEventGroups(storage: EdpStorage, eventGroups: List<EventGroup>) {
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
    return eventGroupMap.flatMap { (referenceId, eventGroupConfig) ->
      when (eventGroupConfig) {
        // Every high overlap event group carries an entity key: EventGroupSync filters its Kingdom
        // listing by entity type, so one without a key would be re-created on every sync.
        is EventGroupConfig.LegacySpec ->
          error("high overlap event group $referenceId has no entity key")
        // One EventGroup per entity key, spanning every date spec. The Kingdom enforces uniqueness
        // on both reference ID and entity key, so a row per date spec would collide.
        is EventGroupConfig.MultiEntityKey ->
          eventGroupConfig.entityKeySpecs.map { entityKeySpec ->
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
              this.measurementConsumer = this@HighOverlapSeeder.measurementConsumer
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

  private suspend fun createDoneBlobs() {
    val paths =
      HighOverlapDataset.datesByImpressionPath(config).flatMap { (impressionPath, dates) ->
        dates.map { date -> "gs://$bucket/$impressionPath/model-line/$modelLineId/$date/done" }
      }
    logger.info("Creating ${paths.size} high overlap DONE blob(s)...")
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

  /**
   * Writes the manifest last.
   *
   * Only a seeding that reached this point describes data a test can rely on, so a run that fails
   * partway leaves the previous manifest in place and the next test reports the older fingerprint
   * rather than trusting half a dataset.
   */
  private suspend fun writeManifest(): HighOverlapDatasetManifest {
    val manifest = highOverlapDatasetManifest {
      specFingerprint = HighOverlapDataset.fingerprint(config, populationSpec)
      seededEdps += config.eventGroupsList.map { it.edpName }.distinct().sorted()
      this.modelLine = this@HighOverlapSeeder.modelLine
      generateTime = timestamp { seconds = clock.instant().epochSecond }
    }
    val key = HighOverlapDataset.manifestBlobKey(modelLineId)
    val blobUri = SelectedStorageClient.parseBlobUri("gs://$bucket/$key")
    val client =
      SelectedStorageClient(blobUri = blobUri, rootDirectory = null, projectId = googleProjectId)
    client.getBlob(key)?.delete()
    client.writeBlob(key, listOf(manifest.toByteString()).asFlow())
    logger.info("Wrote dataset manifest ${manifest.specFingerprint} for $modelLine")
    return manifest
  }

  private val modelLineId: String
    get() =
      requireNotNull(ModelLineKey.fromName(modelLine)) {
          "model_line must be a full ModelLine resource name: $modelLine"
        }
        .modelLineId

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val ZONE_ID = ZoneId.of("UTC")

    // Inferred as kotlin.time.Duration, which withTimeout and delay take.
    private val EVENT_GROUP_SYNC_TIMEOUT = 5.minutes
    private val EVENT_GROUP_SYNC_POLLING_INTERVAL = 5.seconds
  }
}
