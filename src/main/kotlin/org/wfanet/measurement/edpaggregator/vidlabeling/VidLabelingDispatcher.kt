/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.vidlabeling

import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import com.google.type.Date
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import java.time.Clock
import java.time.LocalDate
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.time.TimeSource
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.listModelLinesRequest
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.BlobUris
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadFileKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt.filter as rankIndexFilter
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesRequestKt.filter as rawUploadFileFilter
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt.filter as rawUploadFilter
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadModelLineRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadRegistrationCompleteRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

/**
 * Registers VID labeling uploads in the EDP Aggregator metadata store and starts the pipeline.
 *
 * Processes "done" blob events by crawling directories for raw impression files, resolving active
 * model lines via the VID Repository API (ListModelLines -> ListModelRollouts -> ListModelShards),
 * and registering per-model-line state for downstream processing. After registration it calls the
 * shared [VidLabelingDispatchSequencer] to aggressively start work for the upload (the "fast path")
 * instead of waiting for the next `VidLabelingMonitor` tick.
 *
 * @param storageClient client for crawling raw impressions directory.
 * @param rawImpressionUploadStub gRPC stub for the `RawImpressionUploadService`.
 * @param rawImpressionUploadFilesStub gRPC stub for the `RawImpressionUploadFileService`.
 * @param rawImpressionUploadModelLineStub gRPC stub for the `RawImpressionUploadModelLineService`.
 * @param rankIndexBlobStub gRPC stub used to verify memoized snapshot history during recovery.
 * @param modelLinesStub gRPC stub for the VID Repository ModelLines API.
 * @param dispatchSequencer shared sequencer that resolves model shards and starts pipeline work;
 *   shared with `VidLabelingMonitor` so dispatch logic lives in one place.
 * @param dataProviderName resource name of the `DataProvider`.
 * @param modelSuiteName resource name of the model suite for ListModelLines.
 * @param overrideModelLines if non-empty, use these model lines instead of querying the API.
 *   Overrides bypass active window checks to support backfilling past data.
 * @param recoverySourceUpload evicted source upload that authorizes a metadata-originated override,
 *   or null for normal dispatch and trusted direct backfill requests.
 * @param modelLineConfigs field mapping configuration keyed by model line resource name.
 * @param readEventDate reads a raw-impression file's UTC event date from its plaintext Parquet
 *   footer (no decryption needed).
 * @param readBlobGeneration reads the storage generation for a raw-impression file.
 * @param clock clock for determining active model line windows.
 * @param metrics OpenTelemetry metrics recorder.
 */
class VidLabelingDispatcher(
  private val storageClient: StorageClient,
  private val rawImpressionUploadStub: RawImpressionUploadServiceCoroutineStub,
  private val rawImpressionUploadFilesStub: RawImpressionUploadFileServiceCoroutineStub,
  private val rawImpressionUploadModelLineStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val rankIndexBlobStub: RankIndexBlobServiceCoroutineStub,
  private val modelLinesStub: ModelLinesCoroutineStub,
  private val dispatchSequencer: VidLabelingDispatchSequencer,
  private val dataProviderName: String,
  private val modelSuiteName: String,
  private val overrideModelLines: List<String>,
  private val recoverySourceUpload: String?,
  private val modelLineConfigs: Map<String, VidLabelerParams.ModelLineConfig>,
  private val readEventDate: suspend (blobKey: String) -> LocalDate,
  private val readBlobGeneration: suspend (blobKey: String) -> Long,
  private val clock: Clock = Clock.systemUTC(),
  private val metrics: VidLabelingDispatcherMetrics = VidLabelingDispatcherMetrics(),
) {

  private data class RawBlobVersion(
    val blob: StorageClient.Blob,
    val blobUri: String,
    val generation: Long,
  )

  /** Caps concurrent Parquet-footer reads so footer fan-out stays well under GCS per-bucket QPS. */
  private val readSemaphore = Semaphore(FOOTER_READ_PARALLELISM)

  /**
   * Uploads VID labeling work for raw impression files in the directory containing the done blob.
   *
   * @param doneBlobPath the full storage URI of the "done" blob that triggered this upload.
   * @param doneBlobGeneration GCS object generation number of the done blob. Used to produce
   *   idempotent request IDs that handle both Pub/Sub redelivery (same generation = same ID) and
   *   EDP re-uploads to the same path (new generation = new ID).
   * @throws IllegalArgumentException if [doneBlobPath] uses an unsupported URI scheme or
   *   [doneBlobGeneration] is null.
   */
  suspend fun upload(doneBlobPath: String, doneBlobGeneration: Long) {
    val startTime: TimeSource.Monotonic.ValueTimeMark = TimeSource.Monotonic.markNow()

    try {
      val doneBlobUri: BlobUri = SelectedStorageClient.parseBlobUri(doneBlobPath)
      val folderPrefix: String =
        doneBlobUri.key.substringBeforeLast("/", missingDelimiterValue = "")
      val listingPrefix = if (folderPrefix.isEmpty()) "" else "$folderPrefix/"

      if (!isCurrentDoneBlobGeneration(doneBlobUri, doneBlobGeneration)) {
        logger.info("Ignoring stale done-object generation $doneBlobGeneration for $doneBlobPath")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      if (recoverySourceUpload != null) {
        validateRecovery(doneBlobPath, doneBlobGeneration, recoverySourceUpload)
      }

      val blobs: List<StorageClient.Blob> =
        storageClient.listBlobs(listingPrefix).filter { !isDoneMarker(it.blobKey) }.toList()

      if (blobs.isEmpty()) {
        logger.info("No raw impression files found in $folderPrefix")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      val revisions = listUploadsByDoneBlob(doneBlobPath)
      val latestRevision = revisions.maxByOrNull { it.doneBlobGeneration }
      if (latestRevision != null && latestRevision.doneBlobGeneration > doneBlobGeneration) {
        logger.info("Ignoring stale done-object generation $doneBlobGeneration for $doneBlobPath")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }
      val exactRevision = revisions.firstOrNull { it.doneBlobGeneration == doneBlobGeneration }
      val previousRevision =
        if (exactRevision != null && exactRevision.replacesRawImpressionUpload.isNotEmpty()) {
          revisions.firstOrNull { it.name == exactRevision.replacesRawImpressionUpload }
        } else {
          latestRevision?.takeIf { it.doneBlobGeneration < doneBlobGeneration }
        }
      val currentBlobVersions = resolveBlobVersions(blobs, doneBlobUri)
      val candidateBlobs =
        if (previousRevision?.state == RawImpressionUpload.State.FAILED) {
          currentBlobVersions
        } else {
          selectUnregisteredBlobVersions(currentBlobVersions, exactRevision?.name)
        }

      val registrationBaseline = exactRevision ?: previousRevision
      if (
        candidateBlobs.isEmpty() &&
          (registrationBaseline == null || isRegistrationComplete(registrationBaseline))
      ) {
        logger.info("No new raw impression object versions found in $folderPrefix")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      // A newer done object can be written while this invocation is listing and diffing the
      // directory. Do not register a stale view. The metadata service additionally serializes
      // distinct generations transactionally, closing the race between this check and create.
      if (!isCurrentDoneBlobGeneration(doneBlobUri, doneBlobGeneration)) {
        logger.info("Ignoring stale done-object generation $doneBlobGeneration for $doneBlobPath")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      val rawImpressionUpload = createRawImpressionUpload(doneBlobPath, doneBlobGeneration)
      if (rawImpressionUpload == null) {
        logger.info("Ignoring stale done-object generation $doneBlobGeneration for $doneBlobPath")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      // Creating this revision atomically supersedes any incomplete predecessor. Re-read the
      // chain and recompute the delta so a concurrent newer marker either wins cleanly, or this
      // revision includes the complete directory after replacing a partial predecessor.
      val refreshedRevisions = listUploadsByDoneBlob(doneBlobPath)
      val refreshedLatest = refreshedRevisions.maxByOrNull { it.doneBlobGeneration }
      if (
        refreshedLatest != null &&
          (refreshedLatest.doneBlobGeneration > doneBlobGeneration ||
            (refreshedLatest.doneBlobGeneration == doneBlobGeneration &&
              refreshedLatest.state == RawImpressionUpload.State.FAILED))
      ) {
        logger.info(
          "Ignoring superseded done-object generation $doneBlobGeneration for $doneBlobPath"
        )
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }
      val refreshedCurrent =
        refreshedRevisions.firstOrNull { it.doneBlobGeneration == doneBlobGeneration }
          ?: rawImpressionUpload
      val refreshedPrevious =
        refreshedCurrent.replacesRawImpressionUpload
          .takeIf { it.isNotEmpty() }
          ?.let { replacedName -> refreshedRevisions.firstOrNull { it.name == replacedName } }
          ?: previousRevision
      val blobsToRegister =
        if (refreshedPrevious?.state == RawImpressionUpload.State.FAILED) {
          currentBlobVersions
        } else {
          selectUnregisteredBlobVersions(currentBlobVersions, refreshedCurrent.name)
        }

      if (blobsToRegister.isEmpty() && !hasRegisteredFiles(refreshedCurrent.name)) {
        markRegistrationComplete(rawImpressionUpload.name)
        logger.info("No new raw impression object versions found in $folderPrefix")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      metrics.filesProcessedCounter.add(
        blobsToRegister.size.toLong(),
        Attributes.of(DATA_PROVIDER_ATTR, dataProviderName),
      )

      createRawImpressionUploadFiles(rawImpressionUpload.name, blobsToRegister)

      val resolvedModelLineNames = resolveModelLines()

      if (resolvedModelLineNames.isEmpty()) {
        markRegistrationComplete(rawImpressionUpload.name)
        logger.info("No active model lines resolved for $modelSuiteName")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      createRawImpressionUploadModelLines(rawImpressionUpload.name, resolvedModelLineNames)
      markRegistrationComplete(rawImpressionUpload.name)

      logger.info(
        "Registered upload ${rawImpressionUpload.name} with ${blobsToRegister.size} files and " +
          "${resolvedModelLineNames.size} model lines"
      )

      dispatchFastPath(rawImpressionUpload.name)

      recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
    } catch (e: Exception) {
      recordUploadDuration(startTime, UPLOAD_STATUS_FAILED)
      throw e
    }
  }

  /**
   * Aggressively starts pipeline work for this DataProvider now instead of waiting for the next
   * `VidLabelingMonitor` tick.
   *
   * The shared [dispatchSequencer] serializes per `(DataProvider, ModelLine)` — different model
   * lines run concurrently, but a model line already in flight is not started again — and claims
   * each model line via an etag CAS, so this is safe to run concurrently with the monitor. Dispatch
   * is best-effort: a failure here must not fail an already-successful registration, because the
   * monitor remains the backstop.
   *
   * @param justRegisteredUpload resource name of the upload just registered, for logging context.
   */
  private suspend fun dispatchFastPath(justRegisteredUpload: String) {
    try {
      val dispatchResult: VidLabelingDispatchSequencer.DispatchResult =
        dispatchSequencer.dispatchNext()
      if (dispatchResult.dispatchedUpload != null) {
        metrics.uploadsDispatchedCounter.add(1, Attributes.of(DATA_PROVIDER_ATTR, dataProviderName))
        logger.info("Fast-path dispatched ${dispatchResult.dispatchedUpload}")
      }
    } catch (e: Exception) {
      logger.log(
        Level.WARNING,
        "Fast-path dispatch failed after registering $justRegisteredUpload; " +
          "VidLabelingMonitor will retry",
        e,
      )
    }
  }

  /**
   * Resolves the active model lines whose model shard is available in the VID Repository.
   *
   * If [overrideModelLines] is non-empty, uses those directly without active window filtering. This
   * supports backfilling past data where the model line may no longer be in the active window.
   * Model shard availability is checked via [dispatchSequencer] so the resolution logic is shared
   * with the dispatch path.
   *
   * @return resource names of model lines that should be registered for this upload.
   */
  private suspend fun resolveModelLines(): List<String> {
    val activeModelLineNames: List<String> =
      if (overrideModelLines.isNotEmpty()) {
        // Override model lines bypass active window checks to support backfilling past data.
        logger.info("Using ${overrideModelLines.size} override model lines")
        overrideModelLines
      } else {
        resolveActiveModelLinesFromApi()
      }

    if (activeModelLineNames.isEmpty()) return emptyList()

    val resolved: List<String> = buildList {
      for (modelLineName in activeModelLineNames) {
        if (dispatchSequencer.resolveShardInfo(modelLineName) != null) {
          add(modelLineName)
        } else {
          logger.warning("Could not resolve model shard for $modelLineName, skipping")
        }
      }
    }

    logger.info("Resolved ${resolved.size} model lines with available shards")
    return resolved
  }

  /**
   * Lists active PROD model lines from the VID Repository API.
   *
   * @return list of active model line resource names that have entries in [modelLineConfigs].
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun resolveActiveModelLinesFromApi(): List<String> {
    val now: Timestamp = Timestamps.fromMillis(clock.millis())

    val activeModelLines: List<String> =
      modelLinesStub
        .listResources { pageToken: String ->
          val response =
            try {
              modelLinesStub.listModelLines(
                listModelLinesRequest {
                  parent = modelSuiteName
                  if (pageToken.isNotEmpty()) {
                    this.pageToken = pageToken
                  }
                }
              )
            } catch (e: StatusException) {
              throw Exception("Error listing model lines for $modelSuiteName", e)
            }
          ResourceList(response.modelLinesList, response.nextPageToken)
        }
        .flattenConcat()
        .filter { modelLine -> isActiveProdModelLineWithConfig(modelLine, now) }
        .map { it.name }
        .toList()

    logger.info("Found ${activeModelLines.size} active PROD model lines from API")
    return activeModelLines
  }

  /**
   * Returns whether [modelLine] is an active PROD model line that has a [modelLineConfigs] entry.
   *
   * @param modelLine the model line to check.
   * @param now the current time used for active window evaluation.
   */
  private fun isActiveProdModelLineWithConfig(modelLine: ModelLine, now: Timestamp): Boolean {
    if (modelLine.type != ModelLine.Type.PROD) return false
    if (!isWithinActiveWindow(modelLine, now)) return false
    // TODO(world-federation-of-advertisers/cross-media-measurement#3956): Remove the static
    // modelLineConfigs dependency. Field mappings should come from ModelShard or be
    // convention-based so adding a new model line in the VID Repository doesn't require a
    // Cloud Function config redeploy.
    if (modelLine.name !in modelLineConfigs) {
      logger.warning("Skipping model line ${modelLine.name}: no config entry")
      return false
    }
    return true
  }

  /**
   * Creates a `RawImpressionUpload` resource to track this upload.
   *
   * Uses the done blob path and GCS generation number to produce an idempotent request ID. Same
   * (path, generation) → same request ID → idempotent on Pub/Sub redelivery. New generation at the
   * same path → new request ID → new upload for EDP re-uploads.
   *
   * On `ALREADY_EXISTS` (redelivery after the AIP-155 idempotency cache has expired, so the server
   * returns the error rather than the cached resource), looks up and returns the existing upload so
   * the caller can continue the idempotent downstream steps. This avoids stranding an upload whose
   * row was created by a prior delivery that died before creating its files or model lines.
   *
   * @param doneBlobPath the full storage URI of the "done" blob.
   * @param generation GCS object generation number.
   * @return the created (or pre-existing) `RawImpressionUpload`.
   */
  private suspend fun createRawImpressionUpload(
    doneBlobPath: String,
    generation: Long,
  ): RawImpressionUpload? {
    val request = createRawImpressionUploadRequest {
      parent = dataProviderName
      rawImpressionUpload = rawImpressionUpload {
        doneBlobUri = doneBlobPath
        doneBlobGeneration = generation
      }
      requestId = RequestIds.forRawImpressionUpload(doneBlobPath, generation)
    }

    return try {
      rawImpressionUploadStub.createRawImpressionUpload(request)
    } catch (e: StatusException) {
      if (e.status.code != Status.Code.ALREADY_EXISTS) throw e
      // TODO(world-federation-of-advertisers/cross-media-measurement#4118): once #4118 adds
      // InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_ALREADY_EXISTS, branch on `e.errorInfo?.reason`
      // before the lookup below. A same-request_id-but-different-done_blob_uri collision (a
      // deterministic-UUID collision in RequestIds.forRawImpressionUpload) also surfaces as
      // ALREADY_EXISTS, yet findUploadByDoneBlobUri returns null for it — log that collision
      // explicitly (logger.severe) and rethrow instead of the opaque IllegalStateException below.
      findUploadByDoneBlob(doneBlobPath, generation)
        ?: if ((findLatestUploadByDoneBlob(doneBlobPath)?.doneBlobGeneration ?: 0L) > generation) {
          null
        } else {
          throw IllegalStateException(
            "createRawImpressionUpload returned ALREADY_EXISTS but no RawImpressionUpload matches " +
              doneBlobPath
          )
        }
    }
  }

  private suspend fun markRegistrationComplete(uploadName: String) {
    rawImpressionUploadStub.markRawImpressionUploadRegistrationComplete(
      markRawImpressionUploadRegistrationCompleteRequest {
        name = uploadName
        requestId = RequestIds.forRawImpressionUploadRegistrationComplete(uploadName)
      }
    )
  }

  private suspend fun hasRegisteredFiles(uploadName: String): Boolean {
    val response =
      rawImpressionUploadFilesStub.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest {
          parent = uploadName
          pageSize = 1
        }
      )
    return response.rawImpressionUploadFilesCount > 0
  }

  private fun isRegistrationComplete(upload: RawImpressionUpload): Boolean =
    upload.registrationComplete || upload.state != RawImpressionUpload.State.CREATED

  /**
   * Finds the existing `RawImpressionUpload` for the exact done-object version. Used to recover
   * from `ALREADY_EXISTS` on create.
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun findUploadByDoneBlob(
    doneBlobPath: String,
    generation: Long,
  ): RawImpressionUpload? =
    rawImpressionUploadStub
      .listResources { pageToken: String ->
        val response =
          try {
            rawImpressionUploadStub.listRawImpressionUploads(
              listRawImpressionUploadsRequest {
                parent = dataProviderName
                filter = rawUploadFilter { doneBlobUri = doneBlobPath }
                if (pageToken.isNotEmpty()) {
                  this.pageToken = pageToken
                }
              }
            )
          } catch (e: StatusException) {
            throw Exception("Error listing RawImpressionUploads for $dataProviderName", e)
          }
        ResourceList(response.rawImpressionUploadsList, response.nextPageToken)
      }
      .flattenConcat()
      .firstOrNull { it.doneBlobGeneration == generation }

  /** Finds the latest registered revision at [doneBlobPath]. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun findLatestUploadByDoneBlob(doneBlobPath: String): RawImpressionUpload? =
    listUploadsByDoneBlob(doneBlobPath).maxByOrNull { it.doneBlobGeneration }

  /** Lists every registered revision for [doneBlobPath]. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listUploadsByDoneBlob(doneBlobPath: String): List<RawImpressionUpload> =
    rawImpressionUploadStub
      .listResources { pageToken: String ->
        val response =
          rawImpressionUploadStub.listRawImpressionUploads(
            listRawImpressionUploadsRequest {
              parent = dataProviderName
              filter = rawUploadFilter { doneBlobUri = doneBlobPath }
              if (pageToken.isNotEmpty()) this.pageToken = pageToken
            }
          )
        ResourceList(response.rawImpressionUploadsList, response.nextPageToken)
      }
      .flattenConcat()
      .toList()

  private suspend fun resolveBlobVersions(
    blobs: List<StorageClient.Blob>,
    doneBlobUri: BlobUri,
  ): List<RawBlobVersion> = buildList {
    for (chunk in blobs.chunked(RAW_IMPRESSION_UPLOAD_FILE_LOOKUP_BATCH_SIZE)) {
      addAll(
        coroutineScope {
          chunk
            .map { blob ->
              async {
                RawBlobVersion(
                  blob = blob,
                  blobUri = BlobUris.buildUri(doneBlobUri, blob.blobKey),
                  generation = readSemaphore.withPermit { readBlobGeneration(blob.blobKey) },
                )
              }
            }
            .awaitAll()
        }
      )
    }
  }

  private suspend fun isCurrentDoneBlobGeneration(
    doneBlobUri: BlobUri,
    expectedGeneration: Long,
  ): Boolean =
    readSemaphore.withPermit { readBlobGeneration(doneBlobUri.key) } == expectedGeneration

  /** Returns object versions that have not been registered by an earlier upload. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun selectUnregisteredBlobVersions(
    current: List<RawBlobVersion>,
    currentUploadName: String?,
  ): List<RawBlobVersion> {
    val registeredVersions = mutableSetOf<Pair<String, Long>>()
    val legacyRegisteredUris = mutableSetOf<String>()
    for (chunk in current.chunked(RAW_IMPRESSION_UPLOAD_FILE_LOOKUP_BATCH_SIZE)) {
      rawImpressionUploadFilesStub
        .listResources { pageToken: String ->
          val response =
            rawImpressionUploadFilesStub.listRawImpressionUploadFiles(
              listRawImpressionUploadFilesRequest {
                parent = "$dataProviderName/rawImpressionUploads/-"
                filter = rawUploadFileFilter { blobUriIn += chunk.map { it.blobUri } }
                showDeleted = true
                if (pageToken.isNotEmpty()) this.pageToken = pageToken
              }
            )
          ResourceList(response.rawImpressionUploadFilesList, response.nextPageToken)
        }
        .flattenConcat()
        .filter { file -> parentUploadName(file) != currentUploadName }
        .collect { file ->
          if (file.blobGeneration == 0L) {
            legacyRegisteredUris += file.blobUri
          } else {
            registeredVersions += file.blobUri to file.blobGeneration
          }
        }
    }
    return current.filter {
      it.blobUri !in legacyRegisteredUris && (it.blobUri to it.generation) !in registeredVersions
    }
  }

  private fun parentUploadName(file: RawImpressionUploadFile): String =
    requireNotNull(RawImpressionUploadFileKey.fromName(file.name)) {
        "Malformed RawImpressionUploadFile resource name: ${file.name}"
      }
      .parentKey
      .toName()

  /** Validates an object-metadata recovery request before honoring its model-line override. */
  private suspend fun validateRecovery(
    doneBlobPath: String,
    doneBlobGeneration: Long,
    sourceUploadName: String,
  ) {
    require(overrideModelLines.isNotEmpty()) {
      "A recovery source upload requires at least one override model line"
    }
    val sourceKey =
      requireNotNull(RawImpressionUploadKey.fromName(sourceUploadName)) {
        "Malformed recovery source upload name: $sourceUploadName"
      }
    require(sourceKey.parentKey.toName() == dataProviderName) {
      "$sourceUploadName does not belong to $dataProviderName"
    }
    val source =
      rawImpressionUploadStub.getRawImpressionUpload(
        getRawImpressionUploadRequest { name = sourceUploadName }
      )
    require(source.doneBlobUri == doneBlobPath) {
      "$sourceUploadName belongs to ${source.doneBlobUri}, not $doneBlobPath"
    }
    require(doneBlobGeneration > source.doneBlobGeneration) {
      "Recovery generation $doneBlobGeneration must be newer than source generation " +
        source.doneBlobGeneration
    }
    val latest = findLatestUploadByDoneBlob(doneBlobPath)
    val registeredRecovery = findUploadByDoneBlob(doneBlobPath, doneBlobGeneration)
    val isInitialDelivery = latest?.name == sourceUploadName
    val isRetryOfLatestRecovery =
      registeredRecovery != null &&
        latest?.name == registeredRecovery.name &&
        registeredRecovery.replacesRawImpressionUpload == sourceUploadName
    require(isInitialDelivery || isRetryOfLatestRecovery) {
      "$sourceUploadName has been superseded by ${latest?.name}; recover the latest revision"
    }

    val rowsByCmmsModelLine = listModelLines(sourceUploadName).associateBy { it.cmmsModelLine }
    val recoverableModelLines =
      rowsByCmmsModelLine.values
        .filter { it.state == RawImpressionUploadModelLine.State.FAILED }
        .filter { hasDeletedSnapshotHistory(sourceUploadName, it.cmmsModelLine) }
        .mapTo(mutableSetOf()) { it.cmmsModelLine }
    require(overrideModelLines.toSet() == recoverableModelLines) {
      "Recovery override must contain the complete set of FAILED memoized model lines whose " +
        "snapshots were deleted; requested=$overrideModelLines, recoverable=$recoverableModelLines"
    }
  }

  private suspend fun listModelLines(uploadName: String): List<RawImpressionUploadModelLine> {
    val rows = mutableListOf<RawImpressionUploadModelLine>()
    var pageToken = ""
    do {
      val response =
        rawImpressionUploadModelLineStub.listRawImpressionUploadModelLines(
          listRawImpressionUploadModelLinesRequest {
            parent = uploadName
            this.pageToken = pageToken
          }
        )
      rows += response.rawImpressionUploadModelLinesList
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return rows
  }

  private suspend fun hasDeletedSnapshotHistory(
    uploadName: String,
    cmmsModelLine: String,
  ): Boolean {
    var pageToken = ""
    var found = false
    do {
      val response =
        rankIndexBlobStub.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            parent = uploadName
            showDeleted = true
            filter = rankIndexFilter {
              blobType = RankIndexBlob.BlobType.SNAPSHOT
              this.cmmsModelLine = cmmsModelLine
            }
            this.pageToken = pageToken
          }
        )
      if (response.rankIndexBlobsList.any { !it.hasDeleteTime() }) return false
      found = found || response.rankIndexBlobsCount > 0
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return found
  }

  private fun LocalDate.toProtoDate(): Date = date {
    year = this@toProtoDate.year
    month = this@toProtoDate.monthValue
    day = this@toProtoDate.dayOfMonth
  }

  /**
   * Creates a `RawImpressionUploadFile` for each raw impression blob in the upload.
   *
   * @param uploadName resource name of the parent `RawImpressionUpload`.
   * @param blobs raw-impression file blobs (key + size) in the upload.
   */
  private suspend fun createRawImpressionUploadFiles(
    uploadName: String,
    blobs: List<RawBlobVersion>,
  ) {
    for (chunk in blobs.chunked(RAW_IMPRESSION_UPLOAD_FILE_BATCH_SIZE)) {
      // Resolve each file's event date from its Parquet footer up front, in bounded parallel. Every
      // read is an independent, read-only GCS tail-range fetch (~1 round trip), so resolving them
      // serially would make the fast path O(files) sequential round trips and time the Cloud
      // Function out on large uploads; [readSemaphore] caps in-flight reads under GCS QPS. The
      // BatchCreate writes below stay serial on purpose: they all write interleaved children of the
      // same RawImpressionUpload row, so parallelizing them would only force Spanner to
      // lock-serialize (or abort-retry) the writes.
      val eventDateByBlobKey: Map<String, LocalDate> = coroutineScope {
        chunk
          .associate { blobVersion ->
            blobVersion.blob.blobKey to
              async { readSemaphore.withPermit { readEventDate(blobVersion.blob.blobKey) } }
          }
          .mapValues { (_, deferred) -> deferred.await() }
      }

      val request = batchCreateRawImpressionUploadFilesRequest {
        parent = uploadName
        for (blobVersion in chunk) {
          val blob = blobVersion.blob
          requests += createRawImpressionUploadFileRequest {
            parent = uploadName
            // size_bytes (REQUIRED) is the GCS object size from the directory listing (the Phase-1
            // last-out bin-packer batches files by it). event_date (REQUIRED) is read from the
            // file's plaintext Parquet footer so consumers can reconcile registered files by date.
            rawImpressionUploadFile = rawImpressionUploadFile {
              blobUri = blobVersion.blobUri
              blobGeneration = blobVersion.generation
              sizeBytes = blob.size
              this.eventDate = eventDateByBlobKey.getValue(blob.blobKey).toProtoDate()
            }
            requestId = RequestIds.forRawImpressionUploadFile(uploadName, blobVersion.blobUri)
          }
        }
      }

      try {
        rawImpressionUploadFilesStub.batchCreateRawImpressionUploadFiles(request)
      } catch (e: StatusException) {
        if (e.status.code == Status.Code.ALREADY_EXISTS) {
          // Idempotent redelivery: these files were already created. Ack and continue.
          logger.info("RawImpressionUploadFiles for $uploadName already exist; skipping")
          continue
        }
        throw e
      }
    }
  }

  /**
   * Creates a `RawImpressionUploadModelLine` for each resolved model line.
   *
   * @param uploadName resource name of the parent `RawImpressionUpload`.
   * @param modelLineNames the resolved model line resource names to register.
   */
  private suspend fun createRawImpressionUploadModelLines(
    uploadName: String,
    modelLineNames: List<String>,
  ) {
    for (chunk in modelLineNames.chunked(RAW_IMPRESSION_UPLOAD_MODEL_LINE_BATCH_SIZE)) {
      val request = batchCreateRawImpressionUploadModelLinesRequest {
        parent = uploadName
        for (modelLineName in chunk) {
          requests += createRawImpressionUploadModelLineRequest {
            parent = uploadName
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = modelLineName
            }
            requestId = RequestIds.forRawImpressionUploadModelLine(uploadName, modelLineName)
          }
        }
      }

      try {
        rawImpressionUploadModelLineStub.batchCreateRawImpressionUploadModelLines(request)
      } catch (e: StatusException) {
        if (e.status.code == Status.Code.ALREADY_EXISTS) {
          // Idempotent redelivery: these model lines were already created. Ack and continue.
          logger.info("RawImpressionUploadModelLines for $uploadName already exist; skipping")
          continue
        }
        throw e
      }
    }

    logger.info("Created ${modelLineNames.size} RawImpressionUploadModelLines for $uploadName")
  }

  private fun recordUploadDuration(startTime: TimeSource.Monotonic.ValueTimeMark, status: String) {
    val duration: Double = startTime.elapsedNow().inWholeMilliseconds / 1000.0
    metrics.uploadDurationHistogram.record(
      duration,
      Attributes.of(DATA_PROVIDER_ATTR, dataProviderName, UPLOAD_STATUS_ATTR, status),
    )
  }

  private fun isDoneMarker(blobKey: String): Boolean {
    return blobKey.substringAfterLast("/").equals(DONE_MARKER_FILE_NAME, ignoreCase = true)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val DONE_MARKER_FILE_NAME = "done"

    private const val RAW_IMPRESSION_UPLOAD_FILE_BATCH_SIZE = 100
    private const val RAW_IMPRESSION_UPLOAD_FILE_LOOKUP_BATCH_SIZE = 100
    private const val RAW_IMPRESSION_UPLOAD_MODEL_LINE_BATCH_SIZE = 50

    /** Max concurrent Parquet-footer reads when resolving event dates (well under GCS QPS). */
    private const val FOOTER_READ_PARALLELISM = 100

    private val DATA_PROVIDER_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.data_provider")
    private val UPLOAD_STATUS_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.dispatch_status")
    private const val UPLOAD_STATUS_SUCCESS = "success"
    private const val UPLOAD_STATUS_FAILED = "failed"

    private fun isWithinActiveWindow(modelLine: ModelLine, now: Timestamp): Boolean {
      if (!modelLine.hasActiveStartTime()) return false
      if (Timestamps.compare(now, modelLine.activeStartTime) < 0) return false
      if (modelLine.hasActiveEndTime() && Timestamps.compare(now, modelLine.activeEndTime) >= 0) {
        return false
      }
      return true
    }
  }
}
