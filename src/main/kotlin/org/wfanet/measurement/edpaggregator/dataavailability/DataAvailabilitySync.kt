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
import com.google.protobuf.util.Timestamps
import com.google.type.Interval
import com.google.type.interval
import io.grpc.StatusException
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.text.Charsets.UTF_8
import kotlin.time.TimeSource
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.DataProviderKt.dataAvailabilityMapEntry
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.replaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.telemetry.XmmTraceAttributes
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.ModelLineCutover
import org.wfanet.measurement.edpaggregator.ModelLineCutoverValidator
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.EntityKey
import org.wfanet.measurement.edpaggregator.v1alpha.EntityKeyGroup
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt.filter as listFilter
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchUndeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchUpdateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.entityKey
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.updateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs
import org.wfanet.measurement.storage.BlobMetadataStorageClient
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

/** Updates an existing blob only while it remains at [generation]. */
fun interface GenerationMatchedBlobMetadataUpdater {
  suspend fun update(
    blobKey: String,
    generation: Long,
    customCreateTime: Instant?,
    metadata: Map<String, String>,
  )
}

/** Indicates that an external labeled-output revision changed while it was being synchronized. */
class ExternalDataAvailabilitySnapshotChangedException(
  message: String = "External labeled-output snapshot changed during synchronization",
  cause: Throwable? = null,
) : Exception(message, cause)

/**
 * Synchronizes impression data availability between Cloud Storage and the Kingdom.
 *
 * This class coordinates the workflow that occurs after impression data has been fully uploaded to
 * Cloud Storage and signaled by the presence of a "done" blob in the relevant folder. It handles:
 * - Discovering impression metadata sidecars by either listing an externally written folder or
 *   resolving the exact deterministic VID Labeler outputs for registered raw inputs.
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
 * 2. Metadata blobs are discovered using the selected [DiscoveryMode] and parsed into
 *    [ImpressionMetadata].
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
 * @property publishEventGroupActivities Optional function for publishing positive EventGroup
 *   activity derived from finalized impression metadata.
 * @property kingdomPublicationTimeout Deadline for publishing availability to the Kingdom.
 * @property availabilityImpressionPaths All impression roots checked for provider-wide date gaps.
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
  private val modelLineCutovers: List<ModelLineCutover> = emptyList(),
  private val modelLineDataSources: Map<String, ModelLineDataSource> = emptyMap(),
  private val publishEventGroupActivities: (suspend (Collection<ImpressionMetadata>) -> Int)?,
  private val kingdomPublicationTimeout: Duration = DEFAULT_KINGDOM_PUBLICATION_TIMEOUT,
  private val metrics: DataAvailabilitySyncMetrics = DataAvailabilitySyncMetrics(),
  private val availabilityImpressionPaths: List<String> = listOf(edpImpressionPath),
  private val generationMatchedMetadataUpdater: GenerationMatchedBlobMetadataUpdater? = null,
) {
  data class ModelLineDataSource(
    val storageClient: BlobMetadataStorageClient,
    val edpImpressionPath: String,
  )

  enum class Outcome {
    NO_WORK,
    BLOCKED_GAPS,
    PUBLISHED,
  }

  enum class Stage {
    DISCOVERY,
    METADATA_PERSISTENCE,
    GAP_POLICY,
    KINGDOM_PUBLICATION,
  }

  /** Selects how metadata sidecars are discovered for one synchronization. */
  sealed interface DiscoveryMode {
    /** Lists the done object's folder. Used for externally uploaded labeled impressions. */
    object FolderScan : DiscoveryMode

    /**
     * Derives the exact VID Labeler output pair for each registered raw-impression input.
     *
     * A missing output and sidecar means that the input produced no labeled impressions. If only
     * one member of the pair exists, discovery fails so the durable WorkItem can retry.
     */
    data class VidLabelerOutputs(
      val rawImpressionUpload: String,
      val rawImpressionBlobUris: List<String>,
      val modelLine: String,
      val eventDate: LocalDate,
    ) : DiscoveryMode {
      init {
        require(rawImpressionUpload.isNotEmpty()) { "rawImpressionUpload must not be empty" }
        require(rawImpressionBlobUris.isNotEmpty()) { "rawImpressionBlobUris must not be empty" }
      }
    }
  }

  private val validImpressionPathRegex: Regex = Regex("^$edpImpressionPath/[^/]+(/.*)?$")

  /** Holds an [ImpressionMetadata] along with its associated impressions blob key. */
  private data class ImpressionMetadataWithBlobKey(
    val impressionMetadata: ImpressionMetadata,
    val impressionsBlobKey: String,
    val metadataBlob: CapturedBlob? = null,
    val impressionsBlob: CapturedBlob? = null,
  )

  private data class MetadataBlobCandidate(
    val blob: StorageClient.Blob,
    val expectedImpressionBlobUri: String? = null,
    val capturedBlob: CapturedBlob? = null,
    val content: ByteString? = null,
  )

  private data class CapturedBlob(val blob: StorageClient.Blob, val generation: Long)

  private inner class ExternalFolderSnapshot(
    private val folderPrefix: String,
    private val folderBlobs: Map<String, CapturedBlob>,
    private val finalizedAt: Instant,
    private val conditionalStorageClient: ConditionalOperationStorageClient,
  ) {
    private val referencedBlobs = mutableMapOf<String, CapturedBlob>()

    val metadataBlobs: List<CapturedBlob>
      get() = folderBlobs.values.filter { DataAvailabilityBlobs.isMetadataBlob(it.blob) }

    suspend fun getReferencedBlob(blobKey: String): CapturedBlob? {
      referencedBlobs[blobKey]?.let {
        return it
      }
      val captured =
        folderBlobs[blobKey]
          ?: storageClient.getBlob(blobKey)?.let { captureBlob(it) }
          ?: return null
      validateCreationTime(captured)
      referencedBlobs[blobKey] = captured
      return captured
    }

    suspend fun validate() {
      val currentFolderVersions =
        storageClient.listBlobs(folderPrefix).toList().associate { blob ->
          blob.blobKey to captureBlob(blob).generation
        }
      val capturedFolderVersions = folderBlobs.mapValues { it.value.generation }
      if (currentFolderVersions != capturedFolderVersions) {
        throw ExternalDataAvailabilitySnapshotChangedException()
      }
      for ((blobKey, captured) in referencedBlobs) {
        if (blobKey in folderBlobs) continue
        val currentGeneration = conditionalStorageClient.getFreshnessToken(blobKey)?.toLongOrNull()
        if (currentGeneration != captured.generation) {
          throw ExternalDataAvailabilitySnapshotChangedException()
        }
      }
    }

    private fun validateCreationTime(captured: CapturedBlob) {
      if (captured.blob.createTime.isAfter(finalizedAt)) {
        throw ExternalDataAvailabilitySnapshotChangedException(
          "External labeled-output object was created after its completion marker"
        )
      }
    }
  }

  init {
    require(!edpImpressionPath.startsWith("/")) { "edpImpressionPath cannot start with a slash" }
    require(!edpImpressionPath.endsWith("/")) { "edpImpressionPath cannot end with a slash" }
    require(availabilityImpressionPaths.isNotEmpty()) {
      "availabilityImpressionPaths must not be empty"
    }
    require(edpImpressionPath in availabilityImpressionPaths) {
      "availabilityImpressionPaths must include the discovery root"
    }
    require(availabilityImpressionPaths.all { !it.startsWith("/") && !it.endsWith("/") }) {
      "availabilityImpressionPaths must not start or end with a slash"
    }
    require(impressionMetadataBatchSize > 0) {
      "impressionMetadataBatchSize must be greater than zero"
    }
    require(impressionMetadataBatchSize <= MAX_IMPRESSION_METADATA_BATCH_SIZE) {
      "impressionMetadataBatchSize must not exceed $MAX_IMPRESSION_METADATA_BATCH_SIZE"
    }
    require(modelLineMap.values.all { it.isNotEmpty() }) {
      "modelLineMap entries must contain at least one model line"
    }
    ModelLineCutoverValidator.validate(modelLineCutovers, modelLineMap.values.flatten().toSet())
    val cutoverContributors =
      modelLineCutovers.flatMapTo(mutableSetOf()) {
        listOf(it.historicalModelLine, it.replacementModelLine)
      }
    require(modelLineDataSources.keys.containsAll(cutoverContributors)) {
      "modelLineDataSources must contain every cutover contributor"
    }
    require(!kingdomPublicationTimeout.isZero && !kingdomPublicationTimeout.isNegative) {
      "kingdomPublicationTimeout must be positive"
    }
  }

  /**
   * Synchronizes impression availability data after a completion signal.
   *
   * For external uploads, DataWatcher supplies a "done" blob and the containing folder is listed.
   * For VID Labeler outputs, a durable WorkItem supplies the "done" blob and the exact expected
   * output keys are derived from registered raw inputs.
   *
   * @param doneBlobPath the full Cloud Storage object path of the "done" blob.
   * @param dataAvailabilitySyncLease synchronization lease resource name.
   * @param ensureLeaseActive validates the synchronization lease before each mutation.
   * @param doneBlobGeneration the immutable positive generation of that object.
   * @param externalDoneBlobCreateTime creation time of an externally produced completion marker.
   *   When set for [DiscoveryMode.FolderScan], the folder is bound to that finalized revision.
   * @param discoveryMode folder scanning for external writes or exact deterministic output
   *   discovery for a VID Labeler WorkItem.
   * @param onStage called before synchronization enters a new stage.
   */
  suspend fun sync(
    doneBlobPath: String,
    dataAvailabilitySyncLease: String,
    doneBlobGeneration: Long,
    externalDoneBlobCreateTime: Instant? = null,
    discoveryMode: DiscoveryMode = DiscoveryMode.FolderScan,
    onStage: (Stage) -> Unit = {},
    ensureLeaseActive: suspend () -> Unit,
  ): Outcome {
    require(dataAvailabilitySyncLease.isNotEmpty()) {
      "dataAvailabilitySyncLease must not be empty"
    }
    require(doneBlobGeneration > 0L) { "doneBlobGeneration must be positive" }
    // Start timing for sync duration
    val syncStartTime = TimeSource.Monotonic.markNow()

    try {
      ensureLeaseActive()
      onStage(Stage.DISCOVERY)
      // 1. Crawl for metadata files
      val doneBlobUri: BlobUri = SelectedStorageClient.parseBlobUri(doneBlobPath)
      val folderPrefix = doneBlobUri.key.substringBeforeLast("/", "")

      if (edpImpressionPath.isNotBlank()) {
        require(validImpressionPathRegex.matches(folderPrefix)) {
          "Folder prefix $folderPrefix does not match expected pattern $validImpressionPathRegex"
        }
      }

      val doneBlobFolderPath = doneBlobUri.key.substringBeforeLast("/")
      val doneBlobFolderPrefix = "$doneBlobFolderPath/"
      val externalSnapshot =
        if (discoveryMode == DiscoveryMode.FolderScan && externalDoneBlobCreateTime != null) {
          captureExternalFolderSnapshot(
            doneBlobUri,
            doneBlobGeneration,
            externalDoneBlobCreateTime,
            doneBlobFolderPrefix,
          )
        } else {
          null
        }
      val vidLabelerOutputs = discoveryMode as? DiscoveryMode.VidLabelerOutputs
      if (vidLabelerOutputs != null) {
        require(
          doneBlobUri.key ==
            "$edpImpressionPath/" +
              LabeledImpressionsBlobKeys.forDone(
                vidLabelerOutputs.rawImpressionUpload,
                vidLabelerOutputs.modelLine,
                vidLabelerOutputs.eventDate,
              )
        ) {
          "Done-object path does not match the expected upload, model line, event date, and " +
            "filename"
        }
        require(doneBlobGeneration > 0L) {
          "doneBlobGeneration must be positive for VID Labeler output discovery"
        }
      }
      val impressionMetadataBlobs: Flow<MetadataBlobCandidate> =
        when (discoveryMode) {
          DiscoveryMode.FolderScan ->
            externalSnapshot?.metadataBlobs?.asFlow()?.map { captured ->
              MetadataBlobCandidate(
                blob = captured.blob,
                capturedBlob = captured,
                content = readCapturedBlob(captured),
              )
            } ?: storageClient.listBlobs(doneBlobFolderPrefix).map { MetadataBlobCandidate(it) }
          is DiscoveryMode.VidLabelerOutputs ->
            discoverVidLabelerMetadataBlobs(discoveryMode, doneBlobUri)
        }

      // 1. Retrieve blob details from storage and build a map and validate them
      val impressionMetadataMap: Map<ModelLineKey, List<ImpressionMetadataWithBlobKey>> =
        createModelLineToImpressionMetadataMap(
          impressionMetadataBlobs,
          doneBlobUri,
          doneBlobGeneration,
          vidLabelerOutputs?.modelLine,
          vidLabelerOutputs?.eventDate,
          vidLabelerOutputs?.rawImpressionUpload,
          externalSnapshot,
        )

      if (impressionMetadataMap.isEmpty() && modelLineCutovers.isEmpty()) {
        logger.info("There were no valid impressions metadata.")
        if (vidLabelerOutputs != null) {
          // The registered raw inputs were checked and every output/sidecar pair was absent, so
          // labeling intentionally produced no available impressions. Mark the done object as
          // fully processed to prevent the monitor from reporting a successful no-output job as
          // stuck.
          val syncId = UUID.randomUUID().toString()
          ensureLeaseActive()
          updateBlobMetadata(
            blobKey = doneBlobUri.key,
            expectedGeneration = doneBlobGeneration,
            metadata =
              mapOf(
                DataAvailabilityBlobs.SYNC_ID_KEY to syncId,
                DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE,
                DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY to syncId,
              ),
          )
        }
        // Record sync duration even if no records
        recordSyncDuration(syncStartTime, SYNC_STATUS_SUCCESS)
        return Outcome.NO_WORK
      }

      if (impressionMetadataMap.isEmpty()) {
        logger.info(
          "There were no valid impressions metadata; continuing to publish finalized cutover " +
            "dates."
        )
      }

      // Count total records
      val totalRecords = impressionMetadataMap.values.sumOf { it.size }
      val syncId = UUID.randomUUID().toString()

      // 2. Announce this attempt before mutating ImpressionMetadata. This invalidates any
      // publication marker left by an earlier attempt, so a failure during persistence remains
      // visible and retryable.
      onStage(Stage.METADATA_PERSISTENCE)
      externalSnapshot?.validate()
      ensureLeaseActive()
      updateBlobMetadata(
        blobKey = doneBlobUri.key,
        expectedGeneration = doneBlobGeneration,
        metadata = mapOf(DataAvailabilityBlobs.SYNC_ID_KEY to syncId),
      )

      // 3. Persist ImpressionMetadata (create new, update changed)
      impressionMetadataMap.values.forEach { metadataWithBlobKeys ->
        saveImpressionMetadata(metadataWithBlobKeys, dataAvailabilitySyncLease, ensureLeaseActive)
      }
      Span.current()
        .addEvent(
          "edpa.data_availability.impression_metadata_scanned",
          Attributes.builder()
            .put(RECORD_COUNT_ATTR, totalRecords.toLong())
            .put(XmmTraceAttributes.OUTCOME, "succeeded")
            .build(),
        )

      // Record metadata-store completion separately. Disjoint marker writes prevent an older
      // overlapping attempt from overwriting a newer attempt's ID and falsely completing it.
      ensureLeaseActive()
      updateBlobMetadata(
        blobKey = doneBlobUri.key,
        expectedGeneration = doneBlobGeneration,
        metadata =
          mapOf(DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE),
      )

      // 4. Retrieve model line bound from ImpressionMetadataStorage for all model lines
      ensureLeaseActive()
      val modelLineBounds: ComputeModelLineBoundsResponse =
        impressionMetadataServiceStub.computeModelLineBounds(
          computeModelLineBoundsRequest { parent = dataProviderName }
        )

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
      onStage(Stage.GAP_POLICY)
      val cutoverContributorLines =
        modelLineCutovers.flatMapTo(mutableSetOf()) {
          listOf(it.historicalModelLine, it.replacementModelLine)
        }
      val modelLinesToValidate =
        (impressionMetadataMap.keys.map(ModelLineKey::toName) + cutoverContributorLines)
          .map { checkNotNull(ModelLineKey.fromName(it)) }
          .toSet()
      val dateInfoByModelLine = mutableMapOf<String, DataAvailabilityBlobs.EnumeratedDateInfo>()
      for (modelLineKey in modelLinesToValidate) {
        val dataSource = modelLineDataSources[modelLineKey.toName()]
        val enumerated =
          if (dataSource != null) {
            val modelLinePrefix =
              if (dataSource.edpImpressionPath.isEmpty()) {
                "model-line/${modelLineKey.modelLineId}/"
              } else {
                "${dataSource.edpImpressionPath}/model-line/${modelLineKey.modelLineId}/"
              }
            listOf(
              DataAvailabilityBlobs.enumerateDateInfo(dataSource.storageClient, modelLinePrefix)
            )
          } else {
            availabilityImpressionPaths.distinct().map { impressionPath ->
              val modelLinePrefix =
                if (impressionPath.isEmpty()) "model-line/${modelLineKey.modelLineId}/"
                else "$impressionPath/model-line/${modelLineKey.modelLineId}/"
              DataAvailabilityBlobs.enumerateDateInfo(storageClient, modelLinePrefix)
            }
          }
        dateInfoByModelLine[modelLineKey.toName()] =
          DataAvailabilityBlobs.EnumeratedDateInfo(
            datesWithDoneBlob =
              enumerated
                .flatMap { it.datesWithDoneBlob.entries }
                .groupBy({ it.key }, { it.value })
                .mapValues { (_, blobs) -> blobs.flatten() },
            datesWithoutDoneBlob = enumerated.flatMap { it.datesWithoutDoneBlob }.distinct(),
          )
      }
      val blockedDetails = mutableListOf<String>()
      var cutoverGapDetected = false
      for (modelLineKey in modelLinesToValidate) {
        val modelLineName = modelLineKey.toName()
        val enumerated = dateInfoByModelLine.getValue(modelLineName)
        val finalized = enumerated.datesWithDoneBlob.keys
        val unfinalized =
          (enumerated.datesWithoutDoneBlob +
              if (modelLineName in cutoverContributorLines) {
                enumerated.datesWithDoneBlob
                  .filterValues { blobs -> blobs.any { !DataAvailabilityBlobs.isSynced(it) } }
                  .keys
              } else {
                emptySet()
              })
            .distinct()
        val presentDates = (finalized + unfinalized).toSet()
        val requiredRange = requiredDateRange(modelLineName, dateInfoByModelLine)
        val gaps =
          if (requiredRange == null) DataAvailabilityBlobs.findGaps(presentDates)
          else findMissingDates(requiredRange, presentDates)
        val earliest = finalized.minOrNull()
        val latest = finalized.maxOrNull()
        val inRangeUnfinalized =
          if (requiredRange == null) {
            earliest != null && latest != null && unfinalized.any { it in earliest..latest }
          } else {
            unfinalized.any { it in requiredRange }
          }
        val missingContributor =
          modelLineName in cutoverContributorLines &&
            (requiredRange == null || finalized.none { it in requiredRange })
        val blocksThisGap = errorIfGapsExist || modelLineName in cutoverContributorLines
        if (gaps.isNotEmpty() || inRangeUnfinalized || missingContributor) {
          if (modelLineName in cutoverContributorLines) cutoverGapDetected = true
          blockedDetails.add(
            "Model line ${modelLineKey.toName()} gap dates: $gaps, " +
              "unfinalized dates: $unfinalized, " +
              "missing contributor: $missingContributor"
          )
        }

        // Emit per-batch date-status metrics for the dates Sync just classified, mirroring
        // what DataAvailabilityMonitor emits when invoked on the same paths. The healthy count
        // here is "finalized minus in-range unfinalized blockers" — Sync's per-batch view, not
        // Monitor's full late-arrival/unprocessed-done check.
        val healthyCount =
          if (gaps.isEmpty() && !inRangeUnfinalized && !missingContributor) finalized.size else 0
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
        if (gaps.isNotEmpty() || inRangeUnfinalized || missingContributor) {
          VidLabelingTraceLogging.log(
            logger,
            Level.WARNING,
            "edpa.data_availability.gap_decision",
            VidLabelingTraceAttributes.DATA_PROVIDER_NAME_STRING to dataProviderName,
            VidLabelingTraceAttributes.MODEL_LINE_NAME_STRING to modelLineKey.toName(),
            XmmTraceAttributes.LIFECYCLE_STAGE_STRING to "data_availability_publish",
            XmmTraceAttributes.OUTCOME_STRING to if (blocksThisGap) "blocked" else "allowed",
          )
        }
      }
      if (blockedDetails.isNotEmpty()) {
        Span.current()
          .addEvent(
            "edpa.data_availability.gap_decision",
            Attributes.of(
              XmmTraceAttributes.OUTCOME,
              if (errorIfGapsExist || cutoverGapDetected) "blocked" else "allowed",
            ),
          )
        logger.warning(
          "Date gaps or in-range unfinalized dates detected in $edpImpressionPath. " +
            blockedDetails.joinToString("; ")
        )
        if (errorIfGapsExist || cutoverGapDetected) {
          logger.warning(
            "Skipping replaceDataAvailabilityIntervals due to date gaps or in-range unfinalized " +
              "dates in $edpImpressionPath."
          )
          recordSyncDuration(syncStartTime, SYNC_STATUS_SKIPPED_GAPS)
          return Outcome.BLOCKED_GAPS
        }
      }
      val availabilityEntries =
        buildAvailabilityIntervals(modelLineBounds, dateInfoByModelLine).map {
          (modelLine, availabilityInterval) ->
          dataAvailabilityMapEntry {
            key = modelLine
            value = availabilityInterval
          }
        }
      check(availabilityEntries.isNotEmpty()) {
        "No data availability intervals were computed for $dataProviderName"
      }
      onStage(Stage.KINGDOM_PUBLICATION)
      throttler.onReady {
        externalSnapshot?.validate()
        ensureLeaseActive()
        try {
          dataProvidersStub
            .withDeadlineAfter(kingdomPublicationTimeout.toNanos(), TimeUnit.NANOSECONDS)
            .replaceDataAvailabilityIntervals(
              replaceDataAvailabilityIntervalsRequest {
                name = dataProviderName
                dataAvailabilityIntervals += availabilityEntries
              }
            )
        } catch (e: StatusException) {
          for (entry in availabilityEntries) {
            logAvailabilityInterval(entry.key, entry.value, "failed", e)
          }
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
      Span.current()
        .addEvent(
          "edpa.data_availability.kingdom_published",
          Attributes.of(XmmTraceAttributes.OUTCOME, "succeeded"),
        )
      for (entry in availabilityEntries) {
        logAvailabilityInterval(entry.key, entry.value, "published")
      }

      if (publishEventGroupActivities != null) {
        val publishedActivityCount =
          publishEventGroupActivities(
            impressionMetadataMap.values.flatten().map { it.impressionMetadata }
          )
        Span.current()
          .addEvent(
            "edpa.data_availability.event_group_activity_published",
            Attributes.builder().put(ACTIVITY_COUNT_ATTR, publishedActivityCount.toLong()).build(),
          )
      }

      // This marker is the durable completion signal for both phases of synchronization. Only
      // update the publication ID here. If another attempt has written a newer sync ID while this
      // attempt was publishing, the resulting mismatch must remain visible and retryable.
      ensureLeaseActive()
      updateBlobMetadata(
        blobKey = doneBlobUri.key,
        expectedGeneration = doneBlobGeneration,
        metadata = mapOf(DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY to syncId),
      )
      Span.current()
        .addEvent(
          "edpa.data_availability.sync_marker_written",
          Attributes.of(XmmTraceAttributes.OUTCOME, "succeeded"),
        )

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
      return Outcome.PUBLISHED
    } catch (e: Exception) {
      if (e is ExternalDataAvailabilitySnapshotChangedException) {
        metrics.snapshotValidationFailuresCounter.add(
          1,
          Attributes.of(DATA_PROVIDER_KEY_ATTR, dataProviderName),
        )
      }
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

  private suspend fun captureExternalFolderSnapshot(
    doneBlobUri: BlobUri,
    doneBlobGeneration: Long,
    doneBlobCreateTime: Instant,
    folderPrefix: String,
  ): ExternalFolderSnapshot {
    val conditionalStorageClient =
      storageClient as? ConditionalOperationStorageClient
        ?: throw IllegalStateException(
          "External folder synchronization requires version-aware storage"
        )
    checkNotNull(generationMatchedMetadataUpdater) {
      "External folder synchronization requires generation-matched metadata updates"
    }
    val doneBlob =
      storageClient.getBlob(doneBlobUri.key)?.let { captureBlob(it) }
        ?: throw ExternalDataAvailabilitySnapshotChangedException(
          "External labeled-output completion marker is missing"
        )
    if (
      doneBlob.generation != doneBlobGeneration || doneBlob.blob.createTime != doneBlobCreateTime
    ) {
      throw ExternalDataAvailabilitySnapshotChangedException(
        "External labeled-output completion marker changed before discovery"
      )
    }
    val folderBlobs =
      storageClient.listBlobs(folderPrefix).toList().associate { blob ->
        blob.blobKey to captureBlob(blob)
      }
    if (folderBlobs[doneBlobUri.key]?.generation != doneBlobGeneration) {
      throw ExternalDataAvailabilitySnapshotChangedException(
        "External labeled-output completion marker changed during discovery"
      )
    }
    if (
      folderBlobs.values.any { captured ->
        captured.blob.blobKey != doneBlobUri.key &&
          captured.blob.createTime.isAfter(doneBlobCreateTime)
      }
    ) {
      throw ExternalDataAvailabilitySnapshotChangedException(
        "External labeled-output object was created after its completion marker"
      )
    }
    return ExternalFolderSnapshot(
      folderPrefix,
      folderBlobs,
      doneBlobCreateTime,
      conditionalStorageClient,
    )
  }

  private fun captureBlob(blob: StorageClient.Blob): CapturedBlob {
    val generation =
      (blob as? ConditionalOperationStorageClient.Blob)?.freshnessToken?.toLongOrNull()
        ?: throw IllegalStateException("Storage blob does not expose a numeric generation")
    return CapturedBlob(blob, generation)
  }

  private suspend fun readCapturedBlob(captured: CapturedBlob): ByteString {
    return try {
      captured.blob.read().flatten()
    } catch (e: Exception) {
      throw ExternalDataAvailabilitySnapshotChangedException(
        "Captured external labeled-output object version is unavailable",
        e,
      )
    }
  }

  private suspend fun updateBlobMetadata(
    blobKey: String,
    expectedGeneration: Long?,
    customCreateTime: Instant? = null,
    metadata: Map<String, String> = emptyMap(),
  ) {
    if (expectedGeneration == null || generationMatchedMetadataUpdater == null) {
      storageClient.updateBlobMetadata(blobKey, customCreateTime, metadata)
      return
    }
    generationMatchedMetadataUpdater.update(blobKey, expectedGeneration, customCreateTime, metadata)
  }

  private fun buildAvailabilityIntervals(
    modelLineBounds: ComputeModelLineBoundsResponse,
    dateInfoByModelLine: Map<String, DataAvailabilityBlobs.EnumeratedDateInfo>,
  ): Map<String, Interval> {
    val boundsByModelLine =
      modelLineBounds.modelLineBoundsList.associate { bound ->
        bound.key to
          interval {
            startTime = bound.value.startTime
            endTime = bound.value.endTime
          }
      }
    val cutoversByExternalModelLine = modelLineCutovers.associateBy { it.externalModelLine }
    val cutoverContributorModelLines =
      modelLineCutovers.flatMapTo(mutableSetOf()) {
        listOf(it.historicalModelLine, it.replacementModelLine)
      }
    val availabilityByModelLine = linkedMapOf<String, Interval>()

    for ((internalModelLine, availabilityInterval) in boundsByModelLine) {
      if (internalModelLine in cutoverContributorModelLines) continue
      val externalModelLines = modelLineMap[internalModelLine] ?: listOf(internalModelLine)
      for (externalModelLine in externalModelLines) {
        if (externalModelLine !in cutoversByExternalModelLine) {
          availabilityByModelLine[externalModelLine] = availabilityInterval
        }
      }
    }

    for (cutover in modelLineCutovers) {
      val historicalStartDate =
        dateInfoByModelLine[cutover.historicalModelLine]?.datesWithDoneBlob?.keys?.minOrNull()
          ?: continue
      val replacementEndDate =
        dateInfoByModelLine[cutover.replacementModelLine]
          ?.datesWithDoneBlob
          ?.keys
          ?.filter { it >= cutover.cutoverDate }
          ?.maxOrNull() ?: continue
      val historicalStart = historicalStartDate.atStartOfDay(ZoneOffset.UTC).toInstant()
      val replacementEnd = replacementEndDate.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant()
      if (historicalStart < replacementEnd) {
        availabilityByModelLine[cutover.externalModelLine] = interval {
          startTime = historicalStart.toProtoTime()
          endTime = replacementEnd.toProtoTime()
        }
      }
    }
    return availabilityByModelLine
  }

  private fun requiredDateRange(
    modelLine: String,
    dateInfoByModelLine: Map<String, DataAvailabilityBlobs.EnumeratedDateInfo>,
  ): ClosedRange<LocalDate>? {
    val cutover =
      modelLineCutovers.firstOrNull {
        modelLine == it.historicalModelLine || modelLine == it.replacementModelLine
      } ?: return null
    return if (modelLine == cutover.replacementModelLine) {
      val end =
        maxOf(
          cutover.cutoverDate,
          dateInfoByModelLine[modelLine]?.datesWithDoneBlob?.keys?.maxOrNull()
            ?: cutover.cutoverDate,
        )
      cutover.cutoverDate..end
    } else {
      val start =
        minOf(
          cutover.cutoverDate,
          dateInfoByModelLine[modelLine]?.datesWithDoneBlob?.keys?.minOrNull()
            ?: cutover.cutoverDate,
        )
      val replacementDates =
        dateInfoByModelLine[cutover.replacementModelLine]?.datesWithDoneBlob?.keys.orEmpty()
      val replacementEnd = replacementDates.maxOrNull() ?: cutover.cutoverDate
      val end = minOf(replacementEnd, cutover.legacyRequiredThroughDate)
      start..maxOf(start, end)
    }
  }

  private fun findMissingDates(
    requiredRange: ClosedRange<LocalDate>,
    presentDates: Set<LocalDate>,
  ): List<LocalDate> =
    generateSequence(requiredRange.start) { it.plusDays(1) }
      .takeWhile { it <= requiredRange.endInclusive }
      .filter { it !in presentDates }
      .toList()

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
    impressionMetadataList: List<ImpressionMetadataWithBlobKey>,
    dataAvailabilitySyncLease: String,
    ensureLeaseActive: suspend () -> Unit,
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
      val toRestore = mutableListOf<ImpressionMetadata>()

      for (item in impressionMetadataList) {
        val existing = existingByBlobUri[item.impressionMetadata.blobUri]
        if (existing == null) {
          toCreate.add(item)
        } else {
          check(
            (existing.metadataBlobGeneration == 0L ||
              item.impressionMetadata.metadataBlobGeneration > 0L) &&
              (existing.impressionsBlobGeneration == 0L ||
                item.impressionMetadata.impressionsBlobGeneration > 0L)
          ) {
            "Version-pinned metadata requires snapshot-aware synchronization"
          }
          if (existing.state == ImpressionMetadata.State.DELETED) {
            toRestore.add(existing)
          }
          val scanned =
            item.impressionMetadata.copy {
              name = existing.name
              if (item.impressionMetadata.rawImpressionUpload.isEmpty()) {
                rawImpressionUpload = existing.rawImpressionUpload
              }
            }
          if (hasContentChanged(scanned, existing)) {
            toUpdate.add(
              ImpressionMetadataWithBlobKey(
                impressionMetadata = scanned,
                impressionsBlobKey = item.impressionsBlobKey,
                metadataBlob = item.metadataBlob,
                impressionsBlob = item.impressionsBlob,
              )
            )
          }
        }
      }

      // BatchCreate new entries, chunked at the RPC boundary
      val createResponses =
        chunkByWeight(
            toCreate,
            maxItems = impressionMetadataBatchSize,
            maxWeight = MAX_ENTITY_KEYS_PER_BATCH,
          ) {
            it.impressionMetadata.entityKeysCount
          }
          .flatMap { createChunk ->
            ensureLeaseActive()
            runMetadataAction(
              action = "created",
              attempted = createChunk.map { it.impressionMetadata },
            ) {
              throttler
                .onReady {
                  impressionMetadataServiceStub.batchCreateImpressionMetadata(
                    batchCreateImpressionMetadataRequest {
                      parent = dataProviderName
                      this.dataAvailabilitySyncLease = dataAvailabilitySyncLease
                      createChunk.forEach { item ->
                        requests += createImpressionMetadataRequest {
                          parent = dataProviderName
                          this.dataAvailabilitySyncLease = dataAvailabilitySyncLease
                          impressionMetadata = item.impressionMetadata
                          requestId = contentAwareRequestId(item.impressionMetadata)
                        }
                      }
                    }
                  )
                }
                .impressionMetadataList
            }
          }

      // BatchUpdate changed entries, chunked at the RPC boundary
      val updateResponses =
        chunkByWeight(
            toUpdate,
            maxItems = impressionMetadataBatchSize,
            maxWeight = MAX_ENTITY_KEYS_PER_BATCH,
          ) { item ->
            existingByBlobUri.getValue(item.impressionMetadata.blobUri).entityKeysCount +
              item.impressionMetadata.entityKeysCount
          }
          .flatMap { updateChunk ->
            ensureLeaseActive()
            runMetadataAction(
              action = "updated",
              attempted = updateChunk.map { it.impressionMetadata },
            ) {
              throttler
                .onReady {
                  impressionMetadataServiceStub.batchUpdateImpressionMetadata(
                    batchUpdateImpressionMetadataRequest {
                      parent = dataProviderName
                      this.dataAvailabilitySyncLease = dataAvailabilitySyncLease
                      updateChunk.forEach { item ->
                        requests += updateImpressionMetadataRequest {
                          this.dataAvailabilitySyncLease = dataAvailabilitySyncLease
                          impressionMetadata = item.impressionMetadata
                          requestId = contentAwareRequestId(item.impressionMetadata)
                        }
                      }
                    }
                  )
                }
                .impressionMetadataList
            }
          }

      // Restore soft-deleted entries only after their latest content has been persisted. This
      // prevents stale metadata from becoming visible if an update fails.
      val restoreResponses =
        toRestore.chunked(impressionMetadataBatchSize).flatMap { restoreChunk ->
          ensureLeaseActive()
          runMetadataAction(action = "restored", attempted = restoreChunk) {
            throttler
              .onReady {
                impressionMetadataServiceStub.batchUndeleteImpressionMetadata(
                  batchUndeleteImpressionMetadataRequest {
                    parent = dataProviderName
                    this.dataAvailabilitySyncLease = dataAvailabilitySyncLease
                    names += restoreChunk.map { it.name }
                  }
                )
              }
              .impressionMetadataList
          }
        }
      val unchangedCount =
        impressionMetadataList.size -
          (toCreate.map { it.impressionMetadata.blobUri } +
              toUpdate.map { it.impressionMetadata.blobUri } +
              toRestore.map { it.blobUri })
            .toSet()
            .size
      Span.current()
        .addEvent(
          "edpa.data_availability.impression_metadata_actions",
          Attributes.builder()
            .put(CREATED_COUNT_ATTR, createResponses.size.toLong())
            .put(UPDATED_COUNT_ATTR, updateResponses.size.toLong())
            .put(RESTORED_COUNT_ATTR, restoreResponses.size.toLong())
            .put(UNCHANGED_COUNT_ATTR, unchangedCount.toLong())
            .build(),
        )

      // Set GCS object metadata on every scanned metadata blob — not just newly
      // created/updated ones. A re-sync of a date whose metadata content is unchanged would
      // land in neither `createResponses` nor `updateResponses` (contentAwareRequestId
      // dedupes), but the marker stamp must still run so DataAvailabilityMonitor sees the
      // blob as synced. Resource IDs come from the response when available, else from the
      // pre-existing entries listed up-front.
      val resourceIdByBlobUri: Map<String, ImpressionMetadata> =
        (existingByBlobUri +
          createResponses.associateBy { it.blobUri } +
          restoreResponses.associateBy { it.blobUri } +
          updateResponses.associateBy { it.blobUri })
      val changedBlobUris =
        (toCreate.map { it.impressionMetadata.blobUri } +
            toUpdate.map { it.impressionMetadata.blobUri } +
            toRestore.map { it.blobUri })
          .toSet()
      for ((blobUri, existing) in existingByBlobUri) {
        if (blobUri !in changedBlobUris) {
          logImpressionMetadata(existing, "unchanged", "succeeded")
        }
      }
      for (item in impressionMetadataList) {
        ensureLeaseActive()
        val blobUri = item.impressionMetadata.blobUri
        val resultMetadata = resourceIdByBlobUri.getValue(blobUri)
        val metadataBlobUri = SelectedStorageClient.parseBlobUri(blobUri)
        val customCreateTime = resultMetadata.interval.startTime.toInstant()

        updateBlobMetadata(
          blobKey = metadataBlobUri.key,
          expectedGeneration = item.metadataBlob?.generation,
          customCreateTime = customCreateTime,
          metadata =
            mapOf(
              WatchedBlobs.IMPRESSION_METADATA_RESOURCE_ID_KEY to resultMetadata.name,
              DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE,
            ),
        )

        // Also update the impressions blob with Custom-Time (no resource ID needed)
        ensureLeaseActive()
        val impressionsBlobKey = impressionsBlobKeyByMetadataUri.getValue(blobUri)
        updateBlobMetadata(
          blobKey = impressionsBlobKey,
          expectedGeneration = item.impressionsBlob?.generation,
          customCreateTime = customCreateTime,
        )
      }
    } catch (e: StatusException) {
      throw Exception("Error saving Impressions Metadata", e)
    }
  }

  private suspend fun runMetadataAction(
    action: String,
    attempted: List<ImpressionMetadata>,
    block: suspend () -> List<ImpressionMetadata>,
  ): List<ImpressionMetadata> {
    return try {
      block().also { persisted ->
        for (impressionMetadata in persisted) {
          logImpressionMetadata(impressionMetadata, action, "succeeded")
        }
      }
    } catch (e: StatusException) {
      for (impressionMetadata in attempted) {
        logImpressionMetadata(impressionMetadata, action, "failed", e)
      }
      throw Exception("Error applying ImpressionMetadata action $action", e)
    }
  }

  private fun logImpressionMetadata(
    impressionMetadata: ImpressionMetadata,
    action: String,
    outcome: String,
    error: Throwable? = null,
  ) {
    VidLabelingTraceLogging.log(
      logger,
      if (error == null) Level.INFO else Level.WARNING,
      "edpa.data_availability.impression_metadata",
      VidLabelingTraceAttributes.DATA_PROVIDER_NAME_STRING to dataProviderName,
      VidLabelingTraceAttributes.IMPRESSION_METADATA_NAME_STRING to
        impressionMetadata.name.takeIf { it.isNotEmpty() },
      VidLabelingTraceAttributes.MODEL_LINE_NAME_STRING to impressionMetadata.modelLine,
      VidLabelingTraceAttributes.GCS_OBJECT_PATH_HASH_STRING to
        VidLabelingTraceAttributes.gcsObjectPathHash(impressionMetadata.blobUri),
      VidLabelingTraceAttributes.IMPRESSION_METADATA_ACTION_STRING to action,
      XmmTraceAttributes.LIFECYCLE_STAGE_STRING to "data_availability_metadata",
      XmmTraceAttributes.OUTCOME_STRING to outcome,
      XmmTraceAttributes.ERROR_TYPE_STRING to error?.let(XmmTraceAttributes::errorType),
      XmmTraceAttributes.ERROR_CODE_STRING to error?.let(XmmTraceAttributes::errorCode),
    )
  }

  private fun logAvailabilityInterval(
    modelLine: String,
    interval: com.google.type.Interval,
    outcome: String,
    error: Throwable? = null,
  ) {
    VidLabelingTraceLogging.log(
      logger,
      if (error == null) Level.INFO else Level.WARNING,
      "edpa.data_availability.interval_published",
      VidLabelingTraceAttributes.DATA_PROVIDER_NAME_STRING to dataProviderName,
      VidLabelingTraceAttributes.MODEL_LINE_NAME_STRING to modelLine,
      VidLabelingTraceAttributes.AVAILABILITY_INTERVAL_START_STRING to
        Timestamps.toString(interval.startTime),
      VidLabelingTraceAttributes.AVAILABILITY_INTERVAL_END_STRING to
        Timestamps.toString(interval.endTime),
      XmmTraceAttributes.LIFECYCLE_STAGE_STRING to "data_availability_publish",
      XmmTraceAttributes.OUTCOME_STRING to outcome,
      XmmTraceAttributes.ERROR_TYPE_STRING to error?.let(XmmTraceAttributes::errorType),
      XmmTraceAttributes.ERROR_CODE_STRING to error?.let(XmmTraceAttributes::errorCode),
    )
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
                    showDeleted = true
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

  @OptIn(ExperimentalCoroutinesApi::class)
  private fun discoverVidLabelerMetadataBlobs(
    discoveryMode: DiscoveryMode.VidLabelerOutputs,
    doneBlobUri: BlobUri,
  ): Flow<MetadataBlobCandidate> =
    discoveryMode.rawImpressionBlobUris.distinct().asFlow().flatMapMerge(
      MAX_CONCURRENT_OUTPUT_DISCOVERY
    ) { rawImpressionBlobUri ->
      flow {
        val outputBlobKey =
          if (edpImpressionPath.isEmpty()) {
            LabeledImpressionsBlobKeys.forInput(
              rawImpressionBlobUri,
              discoveryMode.modelLine,
              discoveryMode.eventDate,
            )
          } else {
            "$edpImpressionPath/" +
              LabeledImpressionsBlobKeys.forInput(
                rawImpressionBlobUri,
                discoveryMode.modelLine,
                discoveryMode.eventDate,
              )
          }
        val metadataBlobKey = outputBlobKey + VID_LABELER_METADATA_SUFFIX
        val outputBlob = storageClient.getBlob(outputBlobKey)
        val metadataBlob = storageClient.getBlob(metadataBlobKey)
        when {
          outputBlob == null && metadataBlob == null -> Unit
          outputBlob == null ->
            throw IllegalStateException(
              "VID Labeler metadata sidecar exists without its output: $metadataBlobKey"
            )
          metadataBlob == null ->
            throw IllegalStateException(
              "VID Labeler output exists without its metadata sidecar: $outputBlobKey"
            )
          else ->
            emit(MetadataBlobCandidate(metadataBlob, absoluteBlobUri(doneBlobUri, outputBlobKey)))
        }
      }
    }

  private fun absoluteBlobUri(doneBlobUri: BlobUri, blobKey: String): String =
    when (doneBlobUri.scheme) {
      "gs" -> "${doneBlobUri.scheme}://${doneBlobUri.bucket}/$blobKey"
      "file" -> "${doneBlobUri.scheme}:///${doneBlobUri.bucket}/$blobKey"
      else -> throw IllegalArgumentException("Unsupported scheme: ${doneBlobUri.scheme}")
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
   * @param doneBlobGeneration the immutable positive generation of the "done" blob.
   * @return a map where each key is a [ModelLineKey] and each value is the list of
   *   [ImpressionMetadataWithBlobKey] objects associated with that model line.
   * @throws com.google.protobuf.InvalidProtocolBufferException if a binary `.binpb` blob cannot be
   *   parsed as `BlobDetails`.
   * @throws IllegalArgumentException if a blob has an unsupported file extension.
   */
  private suspend fun createModelLineToImpressionMetadataMap(
    impressionMetadataBlobs: Flow<MetadataBlobCandidate>,
    doneBlobUri: BlobUri,
    doneBlobGeneration: Long,
    expectedModelLine: String?,
    expectedEventDate: LocalDate?,
    rawImpressionUpload: String?,
    externalSnapshot: ExternalFolderSnapshot?,
  ): Map<ModelLineKey, List<ImpressionMetadataWithBlobKey>> {
    val impressionMetadataMap =
      mutableMapOf<ModelLineKey, MutableList<ImpressionMetadataWithBlobKey>>()
    impressionMetadataBlobs.collect { candidate ->
      val impressionMetadataBlob = candidate.blob
      if (
        candidate.expectedImpressionBlobUri == null &&
          !DataAvailabilityBlobs.isMetadataBlob(impressionMetadataBlob)
      ) {
        return@collect
      }
      val fileName = impressionMetadataBlob.blobKey.substringAfterLast("/").lowercase()
      val bytes: ByteString = candidate.content ?: impressionMetadataBlob.read().flatten()

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
      require(blobDetails.blobUri.isNotEmpty()) {
        "BlobDetails blob_uri is empty for metadata: ${impressionMetadataBlob.blobKey}"
      }

      if (candidate.expectedImpressionBlobUri != null) {
        require(blobDetails.blobUri == candidate.expectedImpressionBlobUri) {
          "BlobDetails blob_uri does not match the deterministic VID Labeler output"
        }
      }
      // Validate intervals
      require(blobDetails.interval.hasStartTime() && blobDetails.interval.hasEndTime()) {
        "Found interval without start or end time for blob detail with blob_uri = ${blobDetails.blobUri}"
      }
      if (expectedModelLine != null) {
        require(blobDetails.modelLine == expectedModelLine) {
          "BlobDetails model_line does not match the WorkItem"
        }
      }
      if (expectedEventDate != null) {
        val intervalStartDate =
          blobDetails.interval.startTime.toInstant().atZone(ZoneOffset.UTC).toLocalDate()
        require(intervalStartDate == expectedEventDate) {
          "BlobDetails interval does not match the WorkItem event date"
        }
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
      require(
        impressionBlobUri.scheme == doneBlobUri.scheme &&
          impressionBlobUri.bucket == doneBlobUri.bucket &&
          impressionBlobUri.key.isNotEmpty()
      ) {
        "BlobDetails blob_uri must reference an object in the upload storage root"
      }
      val metadataBlobUri =
        when (doneBlobUri.scheme) {
          "gs" -> "${doneBlobUri.scheme}://${doneBlobUri.bucket}/${impressionMetadataBlob.blobKey}"
          "file" ->
            "${doneBlobUri.scheme}:///${doneBlobUri.bucket}/${impressionMetadataBlob.blobKey}"
          else -> throw IllegalArgumentException("Unsupported scheme: ${doneBlobUri.scheme}")
        }
      logger.info("Checking impression blob presence: ${impressionBlobUri.key}")
      val capturedImpressionBlob = externalSnapshot?.getReferencedBlob(impressionBlobUri.key)
      val impressionBlob =
        if (externalSnapshot != null) capturedImpressionBlob?.blob
        else storageClient.getBlob(impressionBlobUri.key)
      if (impressionBlob == null) {
        val eventDate =
          blobDetails.interval.startTime.toInstant().atZone(ZoneOffset.UTC).toLocalDate()
        throw IllegalStateException(
          "Encrypted impressions blob ${impressionBlobUri.key} referenced by " +
            "${impressionMetadataBlob.blobKey} is missing for model line " +
            "${blobDetails.modelLine} on $eventDate"
        )
      } else {
        logger.info("MetadataBlobUri is: $metadataBlobUri")
        val impressionMetadata = impressionMetadata {
          this.blobUri = metadataBlobUri
          blobTypeUrl = BLOB_TYPE_URL
          eventGroupReferenceId = blobDetails.eventGroupReferenceId
          modelLine = blobDetails.modelLine
          if (rawImpressionUpload != null) {
            this.rawImpressionUpload = rawImpressionUpload
          }
          outputDoneBlobGeneration = doneBlobGeneration
          metadataBlobGeneration = candidate.capturedBlob?.generation ?: 0L
          impressionsBlobGeneration = capturedImpressionBlob?.generation ?: 0L
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
              metadataBlob = candidate.capturedBlob,
              impressionsBlob = capturedImpressionBlob,
            )
          )
      }
    }
    return impressionMetadataMap
  }

  /** Returns a deterministic request ID for metadata contents and captured generations. */
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
      append(metadata.rawImpressionUpload)
      append(FIELD_SEPARATOR)
      append(metadata.outputDoneBlobGeneration)
      append(FIELD_SEPARATOR)
      append(metadata.metadataBlobGeneration)
      append(FIELD_SEPARATOR)
      append(metadata.impressionsBlobGeneration)
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

  private fun <T> chunkByWeight(
    items: Iterable<T>,
    maxItems: Int,
    maxWeight: Int,
    weightOf: (T) -> Int,
  ): List<List<T>> {
    require(maxItems > 0) { "maxItems must be positive" }
    require(maxWeight > 0) { "maxWeight must be positive" }

    val batches = mutableListOf<List<T>>()
    var batch = mutableListOf<T>()
    var batchWeight = 0
    fun flush() {
      if (batch.isEmpty()) return
      batches += batch
      batch = mutableListOf()
      batchWeight = 0
    }

    for (item in items) {
      val weight = weightOf(item)
      require(weight >= 0) { "item weight must not be negative" }
      if (weight > maxWeight) {
        flush()
        batches += listOf(item)
        continue
      }
      if (batch.isNotEmpty() && (batch.size >= maxItems || batchWeight + weight > maxWeight)) {
        flush()
      }
      batch += item
      batchWeight += weight
    }
    flush()
    return batches
  }

  companion object {
    private const val MAX_IMPRESSION_METADATA_BATCH_SIZE = 100
    private const val MAX_ENTITY_KEYS_PER_BATCH = 4_000
    private val RECORD_COUNT_ATTR = AttributeKey.longKey("xmm.edpa.impression_metadata.count")
    private val CREATED_COUNT_ATTR = AttributeKey.longKey("xmm.edpa.impression_metadata.created")
    private val UPDATED_COUNT_ATTR = AttributeKey.longKey("xmm.edpa.impression_metadata.updated")
    private val RESTORED_COUNT_ATTR = AttributeKey.longKey("xmm.edpa.impression_metadata.restored")
    private val UNCHANGED_COUNT_ATTR =
      AttributeKey.longKey("xmm.edpa.impression_metadata.unchanged")
    private val DEFAULT_KINGDOM_PUBLICATION_TIMEOUT: Duration = Duration.ofMinutes(5)
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val DATA_PROVIDER_KEY_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.data_availability_sync.data_provider_key")
    private val SYNC_STATUS_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.data_availability_sync.sync_status")
    private val RPC_METHOD_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.data_availability_sync.rpc_method")
    private val STATUS_CODE_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.data_availability_sync.status_code")
    private val ACTIVITY_COUNT_ATTR: AttributeKey<Long> =
      AttributeKey.longKey("xmm.edpa.event_group_activity.count")
    private const val SYNC_STATUS_SUCCESS = "success"
    private const val SYNC_STATUS_FAILED = "failed"
    private const val SYNC_STATUS_SKIPPED_GAPS = "skipped_gaps"
    private const val RPC_METHOD_REPLACE_DATA_AVAILABILITY_INTERVALS =
      "ReplaceDataAvailabilityIntervals"
    // Protobuf string fields cannot contain null bytes, so this eliminates any collision risk.
    private const val FIELD_SEPARATOR = "\u0000"
    private const val PROTO_FILE_SUFFIX = ".binpb"
    private const val JSON_FILE_SUFFIX = ".json"
    private const val VID_LABELER_METADATA_SUFFIX = ".metadata.binpb"
    private const val MAX_CONCURRENT_OUTPUT_DISCOVERY = 32
    private const val BLOB_TYPE_URL =
      "type.googleapis.com/wfa.measurement.securecomputation.impressions.BlobDetails"
  }
}
