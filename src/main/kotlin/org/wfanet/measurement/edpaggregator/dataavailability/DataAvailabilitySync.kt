/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.protobuf.ByteString
import com.google.protobuf.util.JsonFormat
import com.google.type.interval
import io.grpc.StatusException
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.UUID
import java.util.logging.Logger
import kotlin.text.Charsets.UTF_8
import kotlin.time.TimeSource
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.DataProviderKt.dataAvailabilityMapEntry
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.replaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.EntityKey
import org.wfanet.measurement.edpaggregator.v1alpha.EntityKeyGroup
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt.filter as listFilter
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchUpdateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.entityKey
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.updateImpressionMetadataRequest
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs
import org.wfanet.measurement.storage.BlobMetadataStorageClient
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

/**
 * Synchronizes impression data availability between Cloud Storage and the Kingdom.
 *
 * This class coordinates the workflow that occurs after impression data has been fully uploaded to
 * Cloud Storage and signaled by the presence of a "done" blob in the relevant folder. It handles:
 * - Crawling the folder where the "done" blob resides to find and parse impression metadata files
 *   (`.binpb` or `.json`).
 * - Persisting impression metadata records via the
 *   [ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub].
 * - Computing model line availability intervals using the impression metadata service.
 * - Updating the kingdom data provider's availability intervals through
 *   [DataProvidersGrpcKt.DataProvidersCoroutineStub].
 *
 * The synchronization process is throttled via the provided [Throttler] to prevent overwhelming
 * downstream services with bursts of requests.
 *
 * Typical workflow:
 * 1. A "done" blob path is passed to [sync].
 * 2. Metadata blobs in the same folder are discovered and parsed into [ImpressionMetadata].
 * 3. Valid impression metadata are persisted (created or updated).
 * 4. Model line availability intervals are computed from the persisted metadata.
 * 5. The data provider's availability intervals are updated accordingly.
 *
 * @property edpImpressionPath a string name assigned to the EDP. All impressions for this edp will
 *   be within a subfolder with that name.
 * @property storageClient Client for accessing Cloud Storage blobs, used for crawling and reading
 *   metadata files.
 * @property dataProvidersStub gRPC stub for interacting with the Kingdom Data Providers service.
 * @property impressionMetadataServiceStub gRPC stub for creating impression metadata and computing
 *   model line availability.
 * @property dataProviderName The resource name of the data provider, used as a parent identifier in
 *   gRPC requests.
 * @property throttler A throttling utility to regulate request flow to external services.
 * @property impressionMetadataBatchSize Maximum number of impression metadata records per batch
 *   request.
 * @property modelLineMap Mapping from a source model line to additional model lines that should
 *   receive the same availability interval updates.
 * @property errorIfGapsExist If true, skip replacing data availability intervals when date gaps are
 *   detected and log a warning. If false (default), log a warning but proceed with replacing data
 *   availability intervals normally. Gap dates are always logged regardless of this setting.
 * @property metrics Metrics recorder for telemetry.
 */
class DataAvailabilitySync(
  private val edpImpressionPath: String,
  private val storageClient: BlobMetadataStorageClient,
  private val dataProvidersStub: DataProvidersGrpcKt.DataProvidersCoroutineStub,
  private val impressionMetadataServiceStub:
    ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub,
  private val dataProviderName: String,
  private val throttler: Throttler,
  private val impressionMetadataBatchSize: Int,
  private val modelLineMap: Map<String, List<String>>,
  private val errorIfGapsExist: Boolean,
  private val metrics: DataAvailabilitySyncMetrics = DataAvailabilitySyncMetrics(),
) {
  private val validImpressionPathRegex: Regex = Regex("^$edpImpressionPath/[^/]+(/.*)?$")

  /** Holds an [ImpressionMetadata] along with its associated impressions blob key. */
  private data class ImpressionMetadataWithBlobKey(
    val impressionMetadata: ImpressionMetadata,
    val impressionsBlobKey: String,
  )

  init {
    require(!edpImpressionPath.startsWith("/")) { "edpImpressionPath cannot start with a slash" }
    require(!edpImpressionPath.endsWith("/")) { "edpImpressionPath cannot end with a slash" }
    require(impressionMetadataBatchSize > 0) {
      "impressionMetadataBatchSize must be greater than zero"
    }
  }

  /**
   * Synchronizes impression availability data after a completion signal.
   *
   * This function is triggered when a "done" blob is detected in Cloud Storage, indicating that all
   * impression data for a given day have been fully uploaded. The "done" blob resides in the same
   * folder as the metadata files, and its path is used to determine which folder to crawl when
   * collecting and processing metadata for that day.
   *
   * @param doneBlobPath the full Cloud Storage object path of the "done" blob.
   */
  suspend fun sync(doneBlobPath: String) {
    // Start timing for sync duration
    val syncStartTime = TimeSource.Monotonic.markNow()

    try {
      // 1. Crawl for metadata files
      val doneBlobUri: BlobUri = SelectedStorageClient.parseBlobUri(doneBlobPath)
      val folderPrefix = doneBlobUri.key.substringBeforeLast("/", "")

      if (edpImpressionPath.isNotBlank()) {
        require(validImpressionPathRegex.matches(folderPrefix)) {
          "Folder prefix $folderPrefix does not match expected pattern $validImpressionPathRegex"
        }
      }

      val doneBlobFolderPath = doneBlobUri.key.substringBeforeLast("/")
      val impressionMetadataBlobs: Flow<StorageClient.Blob> =
        storageClient.listBlobs(doneBlobFolderPath)

      // 1. Retrieve blob details from storage and build a map and validate them
      val impressionMetadataMap: Map<ModelLineKey, List<ImpressionMetadataWithBlobKey>> =
        createModelLineToImpressionMetadataMap(impressionMetadataBlobs, doneBlobUri)

      if (impressionMetadataMap.isEmpty()) {
        logger.info("There were no valid impressions metadata.")
        // Record sync duration even if no records
        recordSyncDuration(syncStartTime, SYNC_STATUS_SUCCESS)
        return
      }

      // Count total records
      val totalRecords = impressionMetadataMap.values.sumOf { it.size }

      // 2. Persist ImpressionMetadata (create new, update changed)
      impressionMetadataMap.values.forEach { metadataWithBlobKeys ->
        saveImpressionMetadata(metadataWithBlobKeys)
      }

      // 2b. Stamp the `done` blob with the synced-by marker. The marker means "the
      // ImpressionMetadata store is updated for this date" — Spanner records are persisted
      // and metadata blobs are stamped. It does NOT imply that Kingdom data availability has
      // been updated: gap detection at step 5 may skip the Kingdom publish entirely when
      // errorIfGapsExist=true, and Kingdom publish has its own failure signal
      // (`cmmsRpcErrorsCounter`) when it is attempted.
      //
      // DataAvailabilityMonitor uses the marker to tell late-arrival from never-arrived: a done
      // blob older than its threshold without this marker means Sync did not (yet) update the
      // ImpressionMetadata store for the date.
      storageClient.updateBlobMetadata(
        blobKey = doneBlobUri.key,
        metadata =
          mapOf(DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE),
      )

      // 3. Retrieve model line bound from ImpressionMetadataStorage for all model lines
      val modelLineBounds: ComputeModelLineBoundsResponse =
        impressionMetadataServiceStub.computeModelLineBounds(
          computeModelLineBoundsRequest { parent = dataProviderName }
        )

      // Build availability entries from the response
      val availabilityEntries =
        modelLineBounds.modelLineBoundsList.flatMap { bound ->
          val availabilityInterval = interval {
            startTime = bound.value.startTime
            endTime = bound.value.endTime
          }
          val mappedModelLines = modelLineMap[bound.key]
          if (mappedModelLines != null) {
            logger.info(
              "Model line mapping found: ${bound.key} -> ${mappedModelLines.joinToString(", ")}"
            )
          } else {
            logger.info("No model line mapping found for: ${bound.key}")
          }
          val modelLines = mappedModelLines ?: listOf(bound.key)
          modelLines.map { modelLine ->
            dataAvailabilityMapEntry {
              key = modelLine
              value = availabilityInterval
            }
          }
        }

      // Check, per model line present in this batch, for date gaps or in-range unfinalized
      // dates before updating availability intervals. Done locally via
      // DataAvailabilityBlobs.enumerateDateInfo — Sync doesn't need DataAvailabilityMonitor's
      // staleness/threshold/metrics machinery, only the storage-level date classification.
      //
      // A model line is blocked from publishing when it has a true gap (a missing date between
      // two finalized dates) OR when a date inside its finalized range
      // [earliestFinalized, latestFinalized] still has no "done" blob. Unfinalized dates that
      // trail after latestFinalized or lead before earliestFinalized are ignored — they extend
      // the window rather than punching a hole in it.
      val blockedDetails = mutableListOf<String>()
      for (modelLineKey in impressionMetadataMap.keys) {
        val modelLinePrefix =
          if (edpImpressionPath.isEmpty()) "model-line/${modelLineKey.modelLineId}/"
          else "$edpImpressionPath/model-line/${modelLineKey.modelLineId}/"
        val enumerated = DataAvailabilityBlobs.enumerateDateInfo(storageClient, modelLinePrefix)
        val finalized = enumerated.datesWithDoneBlob.keys
        val unfinalized = enumerated.datesWithoutDoneBlob
        val gaps = DataAvailabilityBlobs.findGaps(finalized + unfinalized)
        val earliest = finalized.minOrNull()
        val latest = finalized.maxOrNull()
        val inRangeUnfinalized =
          earliest != null && latest != null && unfinalized.any { it in earliest..latest }
        if (gaps.isNotEmpty() || inRangeUnfinalized) {
          blockedDetails.add(
            "Model line ${modelLineKey.toName()} gap dates: $gaps, " +
              "unfinalized dates: $unfinalized"
          )
        }

        // Emit per-batch date-status metrics for the dates Sync just classified, mirroring
        // what DataAvailabilityMonitor emits when invoked on the same paths. The healthy count
        // here is "finalized minus in-range unfinalized blockers" — Sync's per-batch view, not
        // Monitor's full late-arrival/unprocessed-done check.
        val healthyCount = if (gaps.isEmpty() && !inRangeUnfinalized) finalized.size else 0
        emitDateStatusMetric(modelLineKey, DataAvailabilityMonitorMetrics.STATUS_GAP, gaps.size)
        emitDateStatusMetric(
          modelLineKey,
          DataAvailabilityMonitorMetrics.STATUS_WITHOUT_DONE_BLOB,
          unfinalized.size,
        )
        emitDateStatusMetric(
          modelLineKey,
          DataAvailabilityMonitorMetrics.STATUS_HEALTHY,
          healthyCount,
        )
      }
      if (blockedDetails.isNotEmpty()) {
        logger.warning(
          "Date gaps or in-range unfinalized dates detected in $edpImpressionPath. " +
            blockedDetails.joinToString("; ")
        )
        if (errorIfGapsExist) {
          logger.warning(
            "Skipping replaceDataAvailabilityIntervals due to date gaps or in-range unfinalized " +
              "dates in $edpImpressionPath."
          )
          recordSyncDuration(syncStartTime, SYNC_STATUS_SKIPPED_GAPS)
          return
        }
      }
      if (availabilityEntries.isNotEmpty()) {
        throttler.onReady {
          try {
            dataProvidersStub.replaceDataAvailabilityIntervals(
              replaceDataAvailabilityIntervalsRequest {
                name = dataProviderName
                dataAvailabilityIntervals += availabilityEntries
              }
            )
          } catch (e: StatusException) {
            // Record CMMS RPC error
            metrics.cmmsRpcErrorsCounter.add(
              1,
              Attributes.of(
                DATA_PROVIDER_KEY_ATTR,
                dataProviderName,
                RPC_METHOD_ATTR,
                RPC_METHOD_REPLACE_DATA_AVAILABILITY_INTERVALS,
                STATUS_CODE_ATTR,
                e.status.code.name,
              ),
            )
            throw Exception("Error replacing DataAvailability intervals", e)
          }
        }
      }

      // Record successful sync
      recordSyncDuration(syncStartTime, SYNC_STATUS_SUCCESS)

      // Record records synced
      metrics.recordsSyncedCounter.add(
        totalRecords.toLong(),
        Attributes.of(
          DATA_PROVIDER_KEY_ATTR,
          dataProviderName,
          SYNC_STATUS_ATTR,
          SYNC_STATUS_SUCCESS,
        ),
      )
    } catch (e: Exception) {
      // Record sync duration even on failure
      recordSyncDuration(syncStartTime, SYNC_STATUS_FAILED)
      throw e
    }
  }

  private fun recordSyncDuration(
    startTime: TimeSource.Monotonic.ValueTimeMark,
    syncStatus: String,
  ) {
    val syncDuration = startTime.elapsedNow().inWholeMilliseconds / 1000.0
    metrics.syncDurationHistogram.record(
      syncDuration,
      Attributes.of(DATA_PROVIDER_KEY_ATTR, dataProviderName, SYNC_STATUS_ATTR, syncStatus),
    )
  }

  private fun emitDateStatusMetric(modelLineKey: ModelLineKey, status: String, count: Int) {
    if (count <= 0) return
    DataAvailabilityMonitorMetrics.dateStatusCounter.add(
      count.toLong(),
      Attributes.of(
        DataAvailabilityMonitorMetrics.MODEL_LINE_ATTR,
        modelLineKey.toName(),
        DataAvailabilityMonitorMetrics.EDP_IMPRESSION_PATH_ATTR,
        edpImpressionPath,
        DataAvailabilityMonitorMetrics.DATE_STATUS_ATTR,
        status,
        DataAvailabilityMonitorMetrics.SOURCE_ATTR,
        DataAvailabilityMonitorMetrics.SOURCE_SYNC,
      ),
    )
  }

  private suspend fun saveImpressionMetadata(
    impressionMetadataList: List<ImpressionMetadataWithBlobKey>
  ) {
    try {
      // Build a map from metadata blob URI to impressions blob key for later lookup
      val impressionsBlobKeyByMetadataUri =
        impressionMetadataList.associate { it.impressionMetadata.blobUri to it.impressionsBlobKey }

      val blobUris = impressionMetadataList.map { it.impressionMetadata.blobUri }

      // List existing entries by blob_uri
      val existingByBlobUri = listImpressionMetadataByBlobUris(blobUris)

      // Partition into creates and updates
      val toCreate = mutableListOf<ImpressionMetadataWithBlobKey>()
      val toUpdate = mutableListOf<ImpressionMetadataWithBlobKey>()

      for (item in impressionMetadataList) {
        val existing = existingByBlobUri[item.impressionMetadata.blobUri]
        if (existing == null) {
          toCreate.add(item)
        } else if (hasContentChanged(item.impressionMetadata, existing)) {
          toUpdate.add(
            ImpressionMetadataWithBlobKey(
              impressionMetadata = item.impressionMetadata.copy { name = existing.name },
              impressionsBlobKey = item.impressionsBlobKey,
            )
          )
        }
      }

      // BatchCreate new entries, chunked at the RPC boundary
      val createResponses =
        toCreate.chunked(impressionMetadataBatchSize).flatMap { createChunk ->
          throttler
            .onReady {
              impressionMetadataServiceStub.batchCreateImpressionMetadata(
                batchCreateImpressionMetadataRequest {
                  parent = dataProviderName
                  createChunk.forEach { item ->
                    requests += createImpressionMetadataRequest {
                      parent = dataProviderName
                      impressionMetadata = item.impressionMetadata
                      requestId = contentAwareRequestId(item.impressionMetadata)
                    }
                  }
                }
              )
            }
            .impressionMetadataList
        }

      // BatchUpdate changed entries, chunked at the RPC boundary
      val updateResponses =
        toUpdate.chunked(impressionMetadataBatchSize).flatMap { updateChunk ->
          throttler
            .onReady {
              impressionMetadataServiceStub.batchUpdateImpressionMetadata(
                batchUpdateImpressionMetadataRequest {
                  parent = dataProviderName
                  updateChunk.forEach { item ->
                    requests += updateImpressionMetadataRequest {
                      impressionMetadata = item.impressionMetadata
                      requestId = contentAwareRequestId(item.impressionMetadata)
                    }
                  }
                }
              )
            }
            .impressionMetadataList
        }

      // Set GCS object metadata on every scanned metadata blob — not just newly
      // created/updated ones. A re-sync of a date whose metadata content is unchanged would
      // land in neither `createResponses` nor `updateResponses` (contentAwareRequestId
      // dedupes), but the marker stamp must still run so DataAvailabilityMonitor sees the
      // blob as synced. Resource IDs come from the response when available, else from the
      // pre-existing entries listed up-front.
      val resourceIdByBlobUri: Map<String, ImpressionMetadata> =
        (existingByBlobUri +
          createResponses.associateBy { it.blobUri } +
          updateResponses.associateBy { it.blobUri })
      for (item in impressionMetadataList) {
        val blobUri = item.impressionMetadata.blobUri
        val resultMetadata = resourceIdByBlobUri.getValue(blobUri)
        val metadataBlobUri = SelectedStorageClient.parseBlobUri(blobUri)
        val customCreateTime = resultMetadata.interval.startTime.toInstant()

        storageClient.updateBlobMetadata(
          blobKey = metadataBlobUri.key,
          customCreateTime = customCreateTime,
          metadata =
            mapOf(
              WatchedBlobs.IMPRESSION_METADATA_RESOURCE_ID_KEY to resultMetadata.name,
              DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE,
            ),
        )

        // Also update the impressions blob with Custom-Time (no resource ID needed)
        val impressionsBlobKey = impressionsBlobKeyByMetadataUri.getValue(blobUri)
        storageClient.updateBlobMetadata(
          blobKey = impressionsBlobKey,
          customCreateTime = customCreateTime,
        )
      }
    } catch (e: StatusException) {
      throw Exception("Error saving Impressions Metadata", e)
    }
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listImpressionMetadataByBlobUris(
    blobUris: List<String>
  ): Map<String, ImpressionMetadata> {
    return blobUris
      .chunked(impressionMetadataBatchSize)
      .flatMap { blobUriChunk ->
        impressionMetadataServiceStub
          .listResources { pageToken: String ->
            val response =
              throttler.onReady {
                impressionMetadataServiceStub.listImpressionMetadata(
                  listImpressionMetadataRequest {
                    parent = dataProviderName
                    filter = listFilter { this.blobUris += blobUriChunk }
                    if (pageToken.isNotEmpty()) {
                      this.pageToken = pageToken
                    }
                  }
                )
              }
            ResourceList(response.impressionMetadataList, response.nextPageToken)
          }
          .flattenConcat()
          .toList()
      }
      .associateBy { it.blobUri }
  }

  private fun hasContentChanged(
    scanned: ImpressionMetadata,
    existing: ImpressionMetadata,
  ): Boolean {
    return contentAwareRequestId(scanned) != contentAwareRequestId(existing)
  }

  /**
   * Reads impression metadata blobs from a given flow of storage objects and groups them by model
   * line.
   *
   * This function iterates over each blob in [impressionMetadataBlobs], filtering out those that do
   * not have the string "metadata" in the file name.
   *
   * For each matching blob:
   * - Determines the format based on the file extension:
   *     - `.binpb`: parses the content as a binary `BlobDetails` protobuf.
   *     - `.json`: parses the content as JSON using [JsonFormat.parser] with
   *       `ignoringUnknownFields()`.
   *     - Other extensions: throws [IllegalArgumentException].
   * - Validates that the parsed `BlobDetails` has both a start and end time in its interval.
   * - Validates that the parsed `BlobDetails` has at least one of `event_group_reference_id` or
   *   `entity_keys` populated, and that every value within `entity_keys` is populated and not
   *   empty.
   * - Verifies that the referenced encrypted impressions blob exists in storage.
   * - Constructs an [ImpressionMetadata] and groups it by [ModelLineKey].
   *
   * @param impressionMetadataBlobs the flow of [StorageClient.Blob] objects to read and parse.
   * @param doneBlobUri the URI of the "done" blob, used to derive the storage scheme and bucket.
   * @return a map where each key is a [ModelLineKey] and each value is the list of
   *   [ImpressionMetadataWithBlobKey] objects associated with that model line.
   * @throws com.google.protobuf.InvalidProtocolBufferException if a binary `.binpb` blob cannot be
   *   parsed as `BlobDetails`.
   * @throws IllegalArgumentException if a blob has an unsupported file extension.
   */
  private suspend fun createModelLineToImpressionMetadataMap(
    impressionMetadataBlobs: Flow<StorageClient.Blob>,
    doneBlobUri: BlobUri,
  ): Map<ModelLineKey, List<ImpressionMetadataWithBlobKey>> {
    val impressionMetadataMap =
      mutableMapOf<ModelLineKey, MutableList<ImpressionMetadataWithBlobKey>>()
    impressionMetadataBlobs.filter(DataAvailabilityBlobs::isMetadataBlob).collect {
      impressionMetadataBlob ->
      val fileName = impressionMetadataBlob.blobKey.substringAfterLast("/").lowercase()
      val bytes: ByteString = impressionMetadataBlob.read().flatten()

      // Build the blob details object
      val blobDetails =
        if (fileName.endsWith(PROTO_FILE_SUFFIX)) {
          BlobDetails.parseFrom(bytes)
        } else if (fileName.endsWith(JSON_FILE_SUFFIX)) {
          val builder = BlobDetails.newBuilder()
          JsonFormat.parser().ignoringUnknownFields().merge(bytes.toString(UTF_8), builder)
          builder.build()
        } else {
          throw IllegalArgumentException("Unsupported file extension for metadata: $fileName")
        }

      // Validate intervals
      require(blobDetails.interval.hasStartTime() && blobDetails.interval.hasEndTime()) {
        "Found interval without start or end time for blob detail with blob_uri = ${blobDetails.blobUri}"
      }
      // At least one of event_group_reference_id or entity_keys must identify the
      // EventGroup that produced this blob. Both empty means the metadata cannot be
      // associated with any EventGroup downstream.
      require(
        blobDetails.eventGroupReferenceId.isNotEmpty() || blobDetails.entityKeysList.isNotEmpty()
      ) {
        "BlobDetails must have either event_group_reference_id or entity_keys populated " +
          "for blob_uri = ${blobDetails.blobUri}"
      }
      // Every EntityKeyGroup must be well-formed: a non-empty entity_type and at least
      // one non-empty entity_id. Malformed groups would create unusable EntityKey entries
      // on the resulting ImpressionMetadata.
      blobDetails.entityKeysList.forEachIndexed { groupIndex, group ->
        require(group.entityType.isNotEmpty()) {
          "BlobDetails entity_keys[$groupIndex].entity_type is empty " +
            "for blob_uri = ${blobDetails.blobUri}"
        }
        require(group.entityIdsList.isNotEmpty()) {
          "BlobDetails entity_keys[$groupIndex].entity_ids is empty " +
            "for blob_uri = ${blobDetails.blobUri}"
        }
        group.entityIdsList.forEachIndexed { idIndex, entityId ->
          require(entityId.isNotEmpty()) {
            "BlobDetails entity_keys[$groupIndex].entity_ids[$idIndex] is empty " +
              "for blob_uri = ${blobDetails.blobUri}"
          }
        }
      }
      val impressionBlobUri: BlobUri = SelectedStorageClient.parseBlobUri(blobDetails.blobUri)
      val metadataBlobUri =
        when (doneBlobUri.scheme) {
          "gs" -> "${doneBlobUri.scheme}://${doneBlobUri.bucket}/${impressionMetadataBlob.blobKey}"
          "file" ->
            "${doneBlobUri.scheme}:///${doneBlobUri.bucket}/${impressionMetadataBlob.blobKey}"
          else -> throw IllegalArgumentException("Unsupported scheme: ${doneBlobUri.scheme}")
        }
      logger.info("Checking impression blob presence: ${impressionBlobUri.key}")
      val impressionBlob = storageClient.getBlob(impressionBlobUri.key)
      if (impressionBlob == null) {
        logger.info(
          "Encrypted impressions blob non found for metadata: ${impressionMetadataBlob.blobKey}."
        )
      } else {
        logger.info("MetadataBlobUri is: $metadataBlobUri")
        val impressionMetadata = impressionMetadata {
          this.blobUri = metadataBlobUri
          blobTypeUrl = BLOB_TYPE_URL
          eventGroupReferenceId = blobDetails.eventGroupReferenceId
          modelLine = blobDetails.modelLine
          interval = blobDetails.interval
          entityKeys += blobDetails.entityKeysList.flatMap { it.toEntityKeys() }
        }
        val modelLineKey =
          requireNotNull(ModelLineKey.fromName(blobDetails.modelLine)) {
            "Invalid model line resource name: ${blobDetails.modelLine}"
          }
        impressionMetadataMap
          .getOrPut(modelLineKey) { mutableListOf() }
          .add(
            ImpressionMetadataWithBlobKey(
              impressionMetadata = impressionMetadata,
              impressionsBlobKey = impressionBlobUri.key,
            )
          )
      }
    }
    return impressionMetadataMap
  }

  /**
   * Generates a deterministic UUID string from the content-bearing fields of an
   * [ImpressionMetadata].
   *
   * The hash includes blob_uri, model_line, interval, event_group_reference_id, and entity_keys
   * (sorted for determinism). This ensures the same request_id is produced when the content is
   * identical, enabling idempotent skipping. When any field changes (e.g., entity_keys are added),
   * a new request_id is generated, signaling the server to apply the update.
   */
  private fun contentAwareRequestId(metadata: ImpressionMetadata): String {
    val canonicalParts = buildString {
      append(metadata.blobUri)
      append(FIELD_SEPARATOR)
      append(metadata.modelLine)
      append(FIELD_SEPARATOR)
      append(metadata.interval.startTime.seconds)
      append(':')
      append(metadata.interval.startTime.nanos)
      append('-')
      append(metadata.interval.endTime.seconds)
      append(':')
      append(metadata.interval.endTime.nanos)
      append(FIELD_SEPARATOR)
      append(metadata.eventGroupReferenceId)
      append(FIELD_SEPARATOR)
      val sortedKeys =
        metadata.entityKeysList.map { "${it.entityType}$FIELD_SEPARATOR${it.entityId}" }.sorted()
      append(sortedKeys.joinToString(FIELD_SEPARATOR))
    }
    val hash = MessageDigest.getInstance("SHA-256").digest(canonicalParts.toByteArray())
    val bytes = hash.copyOf(16)
    bytes[6] = (bytes[6].toInt() and 0x0F or 0x40).toByte()
    bytes[8] = (bytes[8].toInt() and 0x3F or 0x80).toByte()
    val buffer = ByteBuffer.wrap(bytes)
    return UUID(buffer.long, buffer.long).toString()
  }

  fun BlobUri.asUriString(): String =
    if (scheme == "file") {
      "file:///$bucket/$key"
    } else {
      "$scheme://$bucket/$key"
    }

  /**
   * Flattens an [EntityKeyGroup] (one entity_type with many entity_ids) into a list of [EntityKey]
   * entries (one entity_type/entity_id pair per id).
   */
  private fun EntityKeyGroup.toEntityKeys(): List<EntityKey> {
    val source = this
    return source.entityIdsList.map { id ->
      entityKey {
        entityType = source.entityType
        entityId = id
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val DATA_PROVIDER_KEY_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.data_availability_sync.data_provider_key")
    private val SYNC_STATUS_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.data_availability_sync.sync_status")
    private val RPC_METHOD_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.data_availability_sync.rpc_method")
    private val STATUS_CODE_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.data_availability_sync.status_code")
    private const val SYNC_STATUS_SUCCESS = "success"
    private const val SYNC_STATUS_FAILED = "failed"
    private const val SYNC_STATUS_SKIPPED_GAPS = "skipped_gaps"
    private const val RPC_METHOD_REPLACE_DATA_AVAILABILITY_INTERVALS =
      "ReplaceDataAvailabilityIntervals"
    // Protobuf string fields cannot contain null bytes, so this eliminates any collision risk.
    private const val FIELD_SEPARATOR = "\u0000"
    private const val PROTO_FILE_SUFFIX = ".binpb"
    private const val JSON_FILE_SUFFIX = ".json"
    private const val BLOB_TYPE_URL =
      "type.googleapis.com/wfa.measurement.securecomputation.impressions.BlobDetails"
  }
}
