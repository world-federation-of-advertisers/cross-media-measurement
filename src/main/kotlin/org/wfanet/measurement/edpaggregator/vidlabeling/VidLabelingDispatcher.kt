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
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import java.time.Clock
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.time.TimeSource
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.listModelLinesRequest
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.BlobUris
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadModelLineRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
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
 * @param modelLinesStub gRPC stub for the VID Repository ModelLines API.
 * @param dispatchSequencer shared sequencer that resolves model shards and starts pipeline work;
 *   shared with `VidLabelingMonitor` so dispatch logic lives in one place.
 * @param dataProviderName resource name of the `DataProvider`.
 * @param modelSuiteName resource name of the model suite for ListModelLines.
 * @param overrideModelLines if non-empty, use these model lines instead of querying the API.
 *   Overrides bypass active window checks to support backfilling past data.
 * @param modelLineConfigs field mapping configuration keyed by model line resource name.
 * @param clock clock for determining active model line windows.
 * @param metrics OpenTelemetry metrics recorder.
 */
class VidLabelingDispatcher(
  private val storageClient: StorageClient,
  private val rawImpressionUploadStub: RawImpressionUploadServiceCoroutineStub,
  private val rawImpressionUploadFilesStub: RawImpressionUploadFileServiceCoroutineStub,
  private val rawImpressionUploadModelLineStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val modelLinesStub: ModelLinesCoroutineStub,
  private val dispatchSequencer: VidLabelingDispatchSequencer,
  private val dataProviderName: String,
  private val modelSuiteName: String,
  private val overrideModelLines: List<String>,
  private val modelLineConfigs: Map<String, VidLabelerParams.ModelLineConfig>,
  private val clock: Clock = Clock.systemUTC(),
  private val metrics: VidLabelingDispatcherMetrics = VidLabelingDispatcherMetrics(),
) {

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
      val folderPrefix: String = doneBlobUri.key.substringBeforeLast("/")

      val blobs: List<StorageClient.Blob> =
        storageClient.listBlobs(folderPrefix).filter { !isDoneMarker(it.blobKey) }.toList()
      val blobKeys: List<String> = blobs.map { it.blobKey }

      if (blobKeys.isEmpty()) {
        logger.info("No raw impression files found in $folderPrefix")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      metrics.filesProcessedCounter.add(
        blobKeys.size.toLong(),
        Attributes.of(DATA_PROVIDER_ATTR, dataProviderName),
      )

      val rawImpressionUpload = createRawImpressionUpload(doneBlobPath, doneBlobGeneration)

      createRawImpressionUploadFiles(rawImpressionUpload.name, blobs, doneBlobUri)

      val resolvedModelLineNames = resolveModelLines()

      if (resolvedModelLineNames.isEmpty()) {
        logger.info("No active model lines resolved for $modelSuiteName")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      createRawImpressionUploadModelLines(rawImpressionUpload.name, resolvedModelLineNames)

      logger.info(
        "Registered upload ${rawImpressionUpload.name} with ${blobKeys.size} files and " +
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
  ): RawImpressionUpload {
    val request = createRawImpressionUploadRequest {
      parent = dataProviderName
      rawImpressionUpload = rawImpressionUpload { doneBlobUri = doneBlobPath }
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
      findUploadByDoneBlobUri(doneBlobPath)
        ?: throw IllegalStateException(
          "createRawImpressionUpload returned ALREADY_EXISTS but no RawImpressionUpload matches " +
            doneBlobPath
        )
    }
  }

  /**
   * Finds the existing `RawImpressionUpload` for [doneBlobPath] under this DataProvider, matching
   * on `done_blob_uri`. Used to recover from `ALREADY_EXISTS` on create. Matches client-side over
   * the DataProvider's uploads (bounded per DataProvider); no `done_blob_uri` filter exists on the
   * API.
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun findUploadByDoneBlobUri(doneBlobPath: String): RawImpressionUpload? =
    rawImpressionUploadStub
      .listResources { pageToken: String ->
        val response =
          try {
            rawImpressionUploadStub.listRawImpressionUploads(
              listRawImpressionUploadsRequest {
                parent = dataProviderName
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
      .firstOrNull { it.doneBlobUri == doneBlobPath }

  /**
   * Creates a `RawImpressionUploadFile` for each raw impression blob in the upload.
   *
   * @param uploadName resource name of the parent `RawImpressionUpload`.
   * @param blobs raw-impression file blobs (key + size) in the upload.
   * @param doneBlobUri parsed URI of the "done" blob, used to reconstruct full blob URIs.
   */
  private suspend fun createRawImpressionUploadFiles(
    uploadName: String,
    blobs: List<StorageClient.Blob>,
    doneBlobUri: BlobUri,
  ) {
    for (chunk in blobs.chunked(RAW_IMPRESSION_UPLOAD_FILE_BATCH_SIZE)) {
      val request = batchCreateRawImpressionUploadFilesRequest {
        parent = uploadName
        for (blob in chunk) {
          val fileBlobUri = BlobUris.buildUri(doneBlobUri, blob.blobKey)
          requests += createRawImpressionUploadFileRequest {
            parent = uploadName
            // size_bytes (REQUIRED) is the GCS object size from the directory listing; the
            // Phase-1 last-out bin-packer batches files by it.
            rawImpressionUploadFile = rawImpressionUploadFile {
              blobUri = fileBlobUri
              sizeBytes = blob.size
            }
            requestId = RequestIds.forRawImpressionUploadFile(uploadName, fileBlobUri)
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
    private const val RAW_IMPRESSION_UPLOAD_MODEL_LINE_BATCH_SIZE = 50

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
