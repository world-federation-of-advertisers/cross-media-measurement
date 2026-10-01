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
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.time.TimeSource
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.listModelLinesRequest
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.BlobUris
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.rawimpressions.generationMatchedBlobUri
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadCorrectionCandidateKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.UploadHealingOperationKey
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt.filter as rankIndexFilter
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt.filter as rawUploadFilter
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
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
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate as InternalCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub as CorrectionDetectionStub
import org.wfanet.measurement.internal.edpaggregator.activateQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.createQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUpload as internalRawImpressionUpload
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadCorrectionCandidate as internalCandidate
import org.wfanet.measurement.internal.edpaggregator.registerDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.resolveDetectedRawImpressionUploadCorrectionCandidateRequest
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
 * @param correctionDetectionStub gRPC stub for dispatcher correction mutations.
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
 * @param recoveryOperationId eviction operation that authorized [recoverySourceUpload], or null
 *   outside operator recovery.
 * @param modelLineConfigs field mapping configuration keyed by model line resource name.
 * @param readEventDate reads a raw-impression file's UTC event date from its plaintext Parquet
 *   footer (no decryption needed).
 * @param readBlobMetadata reads the storage generation, size, and creation time for a blob in one
 *   metadata lookup.
 * @param rpcThrottlers process-scoped rate limiters shared with the dispatch sequencer.
 * @param clock clock for determining active model line windows.
 * @param metrics OpenTelemetry metrics recorder.
 */
class VidLabelingDispatcher(
  private val storageClient: StorageClient,
  private val rawImpressionUploadStub: RawImpressionUploadServiceCoroutineStub,
  private val rawImpressionUploadFilesStub: RawImpressionUploadFileServiceCoroutineStub,
  private val correctionDetectionStub: CorrectionDetectionStub,
  private val rawImpressionUploadModelLineStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val rankIndexBlobStub: RankIndexBlobServiceCoroutineStub,
  private val modelLinesStub: ModelLinesCoroutineStub,
  private val dispatchSequencer: VidLabelingDispatchSequencer,
  private val dataProviderName: String,
  private val modelSuiteName: String,
  private val overrideModelLines: List<String>,
  private val recoverySourceUpload: String?,
  private val recoveryOperationId: String?,
  private val modelLineConfigs: Map<String, VidLabelerParams.ModelLineConfig>,
  private val readEventDate: suspend (blobKey: String) -> LocalDate,
  private val readBlobMetadata: suspend (blobKey: String) -> RawImpressionBlobMetadata,
  private val rpcThrottlers: VidLabelingRpcThrottlers,
  private val clock: Clock = Clock.systemUTC(),
  private val correctionCandidateRetention: Duration = Duration.ofDays(90),
  private val metrics: VidLabelingDispatcherMetrics = VidLabelingDispatcherMetrics(),
) {

  private data class RawBlobVersion(
    val blob: StorageClient.Blob,
    val blobUri: String,
    val generation: Long,
    val sizeBytes: Long,
  )

  private data class EdpReplacementAuthorization(
    val evictionOperationId: String?,
    val requiredModelLines: List<String>,
  )

  private enum class RecoveryKind {
    EDP_CORRECTION,
    OPERATOR_RECOVERY,
    STALE,
  }

  /** Caps concurrent Parquet-footer reads so footer fan-out stays well under GCS per-bucket QPS. */
  private val readSemaphore = Semaphore(FOOTER_READ_PARALLELISM)
  private val manifestClassifier = RawImpressionUploadManifestClassifier()

  /**
   * Uploads VID labeling work for raw impression files in the directory containing the done blob.
   *
   * @param doneBlobPath the full storage URI of the "done" blob that triggered this upload.
   * @param doneBlobGeneration GCS object generation number of the done blob. Used to produce
   *   idempotent request IDs that handle both DataWatcher redelivery (same generation = same ID)
   *   and EDP re-uploads to the same path (new generation = new ID).
   * @throws IllegalArgumentException if [doneBlobPath] uses an unsupported URI scheme or
   *   [doneBlobGeneration] is null.
   */
  suspend fun upload(doneBlobPath: String, doneBlobGeneration: Long) {
    val startTime: TimeSource.Monotonic.ValueTimeMark = TimeSource.Monotonic.markNow()

    try {
      require((recoverySourceUpload == null) == (recoveryOperationId == null)) {
        "Recovery requests must include both source upload and eviction operation ID"
      }
      val doneBlobUri: BlobUri = SelectedStorageClient.parseBlobUri(doneBlobPath)
      val folderPrefix: String =
        doneBlobUri.key.substringBeforeLast("/", missingDelimiterValue = "")
      val listingPrefix = if (folderPrefix.isEmpty()) "" else "$folderPrefix/"

      val doneBlobMetadata = readBlobMetadata(doneBlobUri.key)
      if (doneBlobMetadata.generation != doneBlobGeneration) {
        logger.info("Ignoring stale done-object generation $doneBlobGeneration for $doneBlobPath")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      val recoveryKind =
        if (recoverySourceUpload != null) {
          validateRecovery(
            doneBlobPath,
            doneBlobGeneration,
            doneBlobMetadata.createTime,
            recoverySourceUpload,
            checkNotNull(recoveryOperationId),
          )
        } else {
          null
        }
      if (recoveryKind == RecoveryKind.STALE) {
        logger.info("Ignoring stale recovery generation $doneBlobGeneration for $doneBlobPath")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      if (!isCurrentDoneBlobGeneration(doneBlobUri, doneBlobGeneration)) {
        logger.info("Ignoring stale done-object generation $doneBlobGeneration for $doneBlobPath")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      val revisions = listUploadsByDoneBlob(doneBlobPath)
      val latestRevision = findLatestUpload(revisions)
      if (
        latestRevision != null &&
          latestRevision.doneBlobGeneration != doneBlobGeneration &&
          latestRevision.hasDoneBlobCreateTime() &&
          Timestamps.compare(
            latestRevision.doneBlobCreateTime,
            doneBlobMetadata.createTime.toProtoTime(),
          ) >= 0
      ) {
        logger.info("Ignoring stale done-object generation $doneBlobGeneration for $doneBlobPath")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }
      val exactRevision =
        revisions.firstOrNull {
          it.doneBlobGeneration == doneBlobGeneration &&
            recoveryOperationId != null &&
            UploadHealingOperationKey.fromName(it.uploadHealingOperation)
              ?.uploadHealingOperationId == recoveryOperationId
        } ?: revisions.firstOrNull { it.doneBlobGeneration == doneBlobGeneration }
      if (recoveryKind == RecoveryKind.EDP_CORRECTION) {
        val quarantinedRevision =
          requireNotNull(exactRevision) {
            "No quarantined RawImpressionUpload exists for recovery generation $doneBlobGeneration"
          }
        val operationId = checkNotNull(recoveryOperationId)
        if (
          isRegistrationComplete(quarantinedRevision) &&
            UploadHealingOperationKey.fromName(quarantinedRevision.uploadHealingOperation)
              ?.uploadHealingOperationId == operationId
        ) {
          logger.info("RawImpressionUpload ${quarantinedRevision.name} is already registered")
          recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
          return
        }
        activateAndResumeQuarantinedUpload(
          quarantinedRevision,
          checkNotNull(recoverySourceUpload),
          operationId,
        )
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }
      if (
        recoverySourceUpload == null &&
          exactRevision != null &&
          isRegistrationComplete(exactRevision) &&
          exactRevision.state != RawImpressionUpload.State.CORRECTION_REQUIRED
      ) {
        logger.info("RawImpressionUpload ${exactRevision.name} is already registered")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }
      val blobs: List<StorageClient.Blob> =
        storageClient.listBlobs(listingPrefix).filter { !isDoneMarker(it.blobKey) }.toList()
      val previousRevision =
        if (exactRevision != null) {
          if (exactRevision.replacesRawImpressionUpload.isNotEmpty()) {
            revisions.firstOrNull { it.name == exactRevision.replacesRawImpressionUpload }
          } else {
            findLatestUpload(revisions.filter { it.name != exactRevision.name })
          }
        } else {
          latestRevision?.takeIf {
            !it.hasDoneBlobCreateTime() ||
              Timestamps.compare(it.doneBlobCreateTime, doneBlobMetadata.createTime.toProtoTime()) <
                0
          }
        }
      val edpReplacementAuthorization =
        if (
          recoverySourceUpload == null &&
            previousRevision?.state == RawImpressionUpload.State.FAILED
        ) {
          validateEdpReplacementOrder(previousRevision)
        } else {
          null
        }
      val evictionOperationId =
        if (recoverySourceUpload != null) {
          checkNotNull(recoveryOperationId)
        } else {
          edpReplacementAuthorization?.evictionOperationId
        }
      val currentBlobVersions = resolveBlobVersions(blobs, doneBlobUri)
      if (currentBlobVersions.isEmpty() && previousRevision == null) {
        throw IllegalArgumentException("An initial raw-impression upload cannot be empty")
      }
      val manifestComparison =
        if (
          recoverySourceUpload == null &&
            previousRevision?.state != RawImpressionUpload.State.FAILED
        ) {
          classifyManifestRevision(
            doneBlobPath,
            doneBlobGeneration,
            doneBlobMetadata.createTime,
            exactRevision,
            previousRevision,
            revisions,
            currentBlobVersions,
          )
        } else {
          null
        }
      if (
        previousRevision?.state == RawImpressionUpload.State.CORRECTION_REQUIRED &&
          manifestComparison != null &&
          manifestComparison.classification in
            setOf(
              RawImpressionUploadManifestClassifier.Classification.NO_OP,
              RawImpressionUploadManifestClassifier.Classification.APPEND,
            )
      ) {
        resolveCorrectionCandidate(previousRevision)
      }
      if (
        manifestComparison?.classification ==
          RawImpressionUploadManifestClassifier.Classification.NO_OP
      ) {
        logger.info("Acknowledged an unchanged raw-impression manifest for $dataProviderName")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }
      if (
        manifestComparison != null &&
          manifestComparison.classification in NON_ADDITIVE_CLASSIFICATIONS
      ) {
        quarantineCorrection(
          doneBlobPath,
          doneBlobGeneration,
          doneBlobMetadata.createTime,
          exactRevision,
          previousRevision,
          currentBlobVersions,
          checkNotNull(manifestComparison),
        )
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }
      val candidateBlobs =
        when {
          recoverySourceUpload != null -> currentBlobVersions
          previousRevision?.state == RawImpressionUpload.State.FAILED -> currentBlobVersions
          manifestComparison?.classification ==
            RawImpressionUploadManifestClassifier.Classification.NEW -> currentBlobVersions
          manifestComparison?.classification ==
            RawImpressionUploadManifestClassifier.Classification.APPEND -> {
            val addedUris =
              manifestComparison.differences
                .filter { it.prior == null && it.current != null }
                .mapTo(mutableSetOf()) { it.blobUri }
            currentBlobVersions.filter { it.blobUri in addedUris }
          }
          else -> currentBlobVersions
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

      val rawImpressionUpload =
        createRawImpressionUpload(
          doneBlobPath,
          doneBlobGeneration,
          doneBlobMetadata.createTime,
          evictionOperationId,
        )
      if (rawImpressionUpload == null) {
        logger.info("Ignoring stale done-object generation $doneBlobGeneration for $doneBlobPath")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      // Creating this revision atomically supersedes any incomplete predecessor. Re-read the
      // chain and recompute the delta so a concurrent newer marker either wins cleanly, or this
      // revision includes the complete directory after replacing a partial predecessor.
      val refreshedRevisions = listUploadsByDoneBlob(doneBlobPath)
      val refreshedLatest = findLatestUpload(refreshedRevisions)
      if (
        refreshedLatest != null &&
          ((refreshedLatest.doneBlobGeneration != doneBlobGeneration &&
            refreshedLatest.hasDoneBlobCreateTime() &&
            Timestamps.compare(
              refreshedLatest.doneBlobCreateTime,
              doneBlobMetadata.createTime.toProtoTime(),
            ) >= 0) ||
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
        refreshedRevisions.firstOrNull { it.name == rawImpressionUpload.name }
          ?: rawImpressionUpload
      if (refreshedCurrent.registrationComplete) {
        logger.info("RawImpressionUpload ${refreshedCurrent.name} is already registered")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }
      val blobsToRegister = candidateBlobs

      if (blobsToRegister.isEmpty() && !hasRegisteredFiles(refreshedCurrent.name)) {
        markRegistrationComplete(rawImpressionUpload)
        logger.info("No new raw impression object versions found in $folderPrefix")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      metrics.filesProcessedCounter.add(
        blobsToRegister.size.toLong(),
        Attributes.of(DATA_PROVIDER_ATTR, dataProviderName),
      )

      createRawImpressionUploadFiles(rawImpressionUpload.name, blobsToRegister)

      val resolvedModelLineNames =
        resolveModelLines(edpReplacementAuthorization?.requiredModelLines.orEmpty())

      if (resolvedModelLineNames.isEmpty()) {
        markRegistrationComplete(rawImpressionUpload)
        logger.info("No active model lines resolved for $modelSuiteName")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      createRawImpressionUploadModelLines(rawImpressionUpload.name, resolvedModelLineNames)
      markRegistrationComplete(rawImpressionUpload)

      logger.info(
        "Registered upload ${rawImpressionUpload.name} with ${blobsToRegister.size} files and " +
          "${resolvedModelLineNames.size} model lines"
      )

      if (rawImpressionUpload.processingDeferred) {
        logger.info("Deferred ${rawImpressionUpload.name} while correction healing is fenced")
      } else {
        dispatchFastPath(rawImpressionUpload.name)
      }

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
   * If [overrideModelLines] or [edpCorrectionModelLines] is non-empty, uses that persisted set
   * directly without active-window filtering. This supports backfilling or correcting historical
   * data after a model line is no longer active. Model-shard availability is checked via
   * [dispatchSequencer] so the resolution logic is shared with the dispatch path.
   *
   * @return resource names of model lines that should be registered for this upload.
   */
  private suspend fun resolveModelLines(edpCorrectionModelLines: List<String>): List<String> {
    val requiredModelLineNames =
      if (overrideModelLines.isNotEmpty()) overrideModelLines else edpCorrectionModelLines
    val activeModelLineNames: List<String> =
      if (requiredModelLineNames.isNotEmpty()) {
        // Healing model lines bypass active-window checks because they reproduce historical work.
        logger.info("Using ${requiredModelLineNames.size} required healing model lines")
        requiredModelLineNames
      } else {
        resolveActiveModelLinesFromApi()
      }

    if (activeModelLineNames.isEmpty()) return emptyList()

    val resolved: List<String> = buildList {
      for (modelLineName in activeModelLineNames) {
        if (dispatchSequencer.resolveShardInfo(modelLineName) != null) {
          add(modelLineName)
        } else if (requiredModelLineNames.isNotEmpty()) {
          error("Required healing model line $modelLineName has no available model shard")
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
              rpcThrottlers.kingdom.onReady {
                modelLinesStub.listModelLines(
                  listModelLinesRequest {
                    parent = modelSuiteName
                    if (pageToken.isNotEmpty()) {
                      this.pageToken = pageToken
                    }
                  }
                )
              }
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

  private suspend fun classifyManifestRevision(
    doneBlobPath: String,
    doneBlobGeneration: Long,
    doneBlobCreateTime: Instant,
    exactRevision: RawImpressionUpload?,
    previousRevision: RawImpressionUpload?,
    revisions: List<RawImpressionUpload>,
    currentBlobVersions: List<RawBlobVersion>,
  ): RawImpressionUploadManifestClassifier.Result {
    val currentName =
      exactRevision?.name ?: "$dataProviderName/rawImpressionUploads/pending-$doneBlobGeneration"
    val history =
      revisions.filter { it.name != currentName }.map { revision -> revision.toManifestRevision() }
    val current =
      RawImpressionUploadManifestClassifier.Revision(
        rawImpressionUpload = currentName,
        doneBlobUri = doneBlobPath,
        doneBlobGeneration = doneBlobGeneration,
        doneBlobCreateTime = doneBlobCreateTime,
        createTime = doneBlobCreateTime,
        replacesRawImpressionUpload =
          exactRevision?.replacesRawImpressionUpload?.takeIf { it.isNotEmpty() }
            ?: previousRevision?.name.orEmpty(),
        registrationComplete = true,
        files =
          currentBlobVersions.map {
            RawImpressionUploadManifestClassifier.File(it.blobUri, it.generation)
          },
      )
    return manifestClassifier.classify(currentName, history + current)
  }

  private suspend fun RawImpressionUpload.toManifestRevision():
    RawImpressionUploadManifestClassifier.Revision {
    return RawImpressionUploadManifestClassifier.Revision(
      rawImpressionUpload = name,
      doneBlobUri = doneBlobUri,
      doneBlobGeneration = doneBlobGeneration,
      doneBlobCreateTime = if (hasDoneBlobCreateTime()) doneBlobCreateTime.toInstant() else null,
      createTime = createTime.toInstant(),
      replacesRawImpressionUpload = replacesRawImpressionUpload,
      uploadHealingOperation = uploadHealingOperation,
      registrationComplete = registrationComplete,
      failed = state == RawImpressionUpload.State.FAILED,
      quarantined = state == RawImpressionUpload.State.CORRECTION_REQUIRED,
      files = listManifestFiles(name),
    )
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listManifestFiles(
    uploadName: String
  ): List<RawImpressionUploadManifestClassifier.File> =
    rawImpressionUploadFilesStub
      .listResources { pageToken: String ->
        val response =
          rpcThrottlers.metadataRead.onReady {
            rawImpressionUploadFilesStub.listRawImpressionUploadFiles(
              listRawImpressionUploadFilesRequest {
                parent = uploadName
                if (pageToken.isNotEmpty()) this.pageToken = pageToken
              }
            )
          }
        ResourceList(response.rawImpressionUploadFilesList, response.nextPageToken)
      }
      .flattenConcat()
      .map {
        RawImpressionUploadManifestClassifier.File(it.blobUri, it.blobGeneration, it.eventDate)
      }
      .toList()

  private suspend fun activateAndResumeQuarantinedUpload(
    upload: RawImpressionUpload,
    sourceUploadName: String,
    operationId: String,
  ) {
    val uploadKey = requireNotNull(RawImpressionUploadKey.fromName(upload.name))
    val sourceKey = requireNotNull(RawImpressionUploadKey.fromName(sourceUploadName))
    val activated =
      rpcThrottlers.metadataWrite.onReady {
        correctionDetectionStub.activateQuarantinedRawImpressionUpload(
          activateQuarantinedRawImpressionUploadRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
            sourceRawImpressionUploadResourceId = sourceKey.rawImpressionUploadId
            evictionOperationId = operationId
            requestId =
              RequestIds.forActivateQuarantinedRawImpressionUpload(upload.name, operationId)
          }
        )
      }
    if (activated.registrationComplete) return
    val activatedUpload = rawImpressionUpload {
      name = upload.name
      etag = activated.etag
    }
    val modelLineNames = resolveModelLines(overrideModelLines)
    if (modelLineNames.isNotEmpty()) {
      createRawImpressionUploadModelLines(upload.name, modelLineNames)
    }
    markRegistrationComplete(activatedUpload)
    if (modelLineNames.isNotEmpty()) {
      dispatchFastPath(upload.name)
    }
    logger.info(
      "Activated quarantined upload ${upload.name} with ${modelLineNames.size} model lines"
    )
  }

  private suspend fun quarantineCorrection(
    doneBlobPath: String,
    doneBlobGeneration: Long,
    doneBlobCreateTime: Instant,
    exactRevision: RawImpressionUpload?,
    previousRevision: RawImpressionUpload?,
    currentBlobVersions: List<RawBlobVersion>,
    comparison: RawImpressionUploadManifestClassifier.Result,
  ) {
    if (
      !isCurrentDoneBlobGeneration(
        SelectedStorageClient.parseBlobUri(doneBlobPath),
        doneBlobGeneration,
      )
    ) {
      logger.info("Ignoring stale correction generation $doneBlobGeneration for $dataProviderName")
      return
    }
    val candidateId =
      RequestIds.forRawImpressionUploadCorrectionCandidate(doneBlobPath, doneBlobGeneration)
    val dataProviderKey = requireNotNull(DataProviderKey.fromName(dataProviderName))
    val quarantinedUpload =
      if (exactRevision == null) {
        rpcThrottlers.metadataWrite.onReady {
          correctionDetectionStub.createQuarantinedRawImpressionUpload(
            createQuarantinedRawImpressionUploadRequest {
              dataProviderResourceId = dataProviderKey.dataProviderId
              rawImpressionUpload = internalRawImpressionUpload {
                doneBlobUri = doneBlobPath
                this.doneBlobGeneration = doneBlobGeneration
                this.doneBlobCreateTime = doneBlobCreateTime.toProtoTime()
              }
              rawImpressionUploadCorrectionCandidateId = candidateId
              requestId = RequestIds.forRawImpressionUpload(doneBlobPath, doneBlobGeneration)
            }
          )
        }
      } else {
        internalRawImpressionUpload {
          dataProviderResourceId = dataProviderKey.dataProviderId
          rawImpressionUploadResourceId =
            requireNotNull(RawImpressionUploadKey.fromName(exactRevision.name))
              .rawImpressionUploadId
          registrationComplete = exactRevision.registrationComplete
        }
      }
    val uploadName =
      RawImpressionUploadKey(dataProviderKey, quarantinedUpload.rawImpressionUploadResourceId)
        .toName()
    if (!quarantinedUpload.registrationComplete) {
      metrics.filesProcessedCounter.add(
        currentBlobVersions.size.toLong(),
        Attributes.of(DATA_PROVIDER_ATTR, dataProviderName),
      )
      createRawImpressionUploadFiles(uploadName, currentBlobVersions)
    }
    val supersededCandidateId =
      previousRevision
        ?.takeIf { it.state == RawImpressionUpload.State.CORRECTION_REQUIRED }
        ?.let { previous ->
          RequestIds.forRawImpressionUploadCorrectionCandidate(
            previous.doneBlobUri,
            previous.doneBlobGeneration,
          )
        }
        .orEmpty()
    val classification = comparison.classification.toCandidateClassification()
    val classificationLabel = classification.name.removePrefix("CLASSIFICATION_")
    val registration =
      rpcThrottlers.metadataWrite.onReady {
        correctionDetectionStub.registerDetectedRawImpressionUploadCorrectionCandidate(
          registerDetectedRawImpressionUploadCorrectionCandidateRequest {
            dataProviderResourceId = dataProviderKey.dataProviderId
            rawImpressionUploadCorrectionCandidateId = candidateId
            rawImpressionUploadCorrectionCandidate = internalCandidate {
              rawImpressionUploadResourceId = quarantinedUpload.rawImpressionUploadResourceId
              this.classification = classification
              priorManifestDigest = comparison.priorManifestDigest
              currentManifestDigest = comparison.currentManifestDigest
              expireTime = doneBlobCreateTime.plus(correctionCandidateRetention).toProtoTime()
            }
            supersededRawImpressionUploadCorrectionCandidateId = supersededCandidateId
            requestId =
              RequestIds.forRegisterRawImpressionUploadCorrectionCandidate(
                doneBlobPath,
                doneBlobGeneration,
              )
          }
        )
      }
    if (registration.newlyCreated) {
      metrics.correctionCandidatesCounter.add(
        1,
        Attributes.of(
          DATA_PROVIDER_ATTR,
          dataProviderName,
          CORRECTION_CLASSIFICATION_ATTR,
          classificationLabel,
        ),
      )
    }
    val candidateName =
      RawImpressionUploadCorrectionCandidateKey(dataProviderKey, candidateId).toName()
    logger.info(
      "Quarantined correction candidate $candidateName as $classificationLabel with " +
        "${currentBlobVersions.size} files"
    )
  }

  private suspend fun resolveCorrectionCandidate(previousRevision: RawImpressionUpload) {
    val candidateId =
      RequestIds.forRawImpressionUploadCorrectionCandidate(
        previousRevision.doneBlobUri,
        previousRevision.doneBlobGeneration,
      )
    val dataProviderId = requireNotNull(DataProviderKey.fromName(dataProviderName)).dataProviderId
    val result =
      rpcThrottlers.metadataWrite.onReady {
        correctionDetectionStub.resolveDetectedRawImpressionUploadCorrectionCandidate(
          resolveDetectedRawImpressionUploadCorrectionCandidateRequest {
            dataProviderResourceId = dataProviderId
            rawImpressionUploadCorrectionCandidateId = candidateId
            requestId = RequestIds.forResolveRawImpressionUploadCorrectionCandidate(candidateId)
          }
        )
      }
    if (result.newlyResolved) {
      logger.info("Resolved correction candidate for a healthy revision of $dataProviderName")
    }
  }

  private fun RawImpressionUploadManifestClassifier.Classification.toCandidateClassification():
    InternalCandidate.Classification =
    when (this) {
      RawImpressionUploadManifestClassifier.Classification.EDITED ->
        InternalCandidate.Classification.CLASSIFICATION_EDITED
      RawImpressionUploadManifestClassifier.Classification.REMOVED ->
        InternalCandidate.Classification.CLASSIFICATION_REMOVED
      RawImpressionUploadManifestClassifier.Classification.MIXED ->
        InternalCandidate.Classification.CLASSIFICATION_MIXED
      RawImpressionUploadManifestClassifier.Classification.NEW,
      RawImpressionUploadManifestClassifier.Classification.NO_OP,
      RawImpressionUploadManifestClassifier.Classification.APPEND ->
        error("Only non-additive revisions are correction candidates")
    }

  /**
   * Creates a `RawImpressionUpload` resource to track this upload.
   *
   * Uses the done blob path and GCS generation number to produce an idempotent request ID. Same
   * (path, generation) → same request ID → idempotent on DataWatcher redelivery. New generation at
   * the same path → new request ID → new upload for EDP re-uploads.
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
    createTime: Instant,
    evictionOperationId: String?,
  ): RawImpressionUpload? {
    val request = createRawImpressionUploadRequest {
      parent = dataProviderName
      rawImpressionUpload = rawImpressionUpload {
        doneBlobUri = doneBlobPath
        doneBlobGeneration = generation
        doneBlobCreateTime = createTime.toProtoTime()
        if (evictionOperationId != null) {
          uploadHealingOperation =
            UploadHealingOperationKey(
                requireNotNull(DataProviderKey.fromName(dataProviderName)),
                evictionOperationId,
              )
              .toName()
        }
      }
      requestId =
        if (evictionOperationId == null) {
          RequestIds.forRawImpressionUpload(doneBlobPath, generation)
        } else {
          RequestIds.forRawImpressionUploadRecovery(doneBlobPath, generation, evictionOperationId)
        }
    }

    return try {
      rpcThrottlers.metadataWrite.onReady {
        rawImpressionUploadStub.createRawImpressionUpload(request)
      }
    } catch (e: StatusException) {
      if (e.status.code != Status.Code.ALREADY_EXISTS) throw e
      // TODO(world-federation-of-advertisers/cross-media-measurement#4118): once #4118 adds
      // InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_ALREADY_EXISTS, branch on `e.errorInfo?.reason`
      // before the lookup below. A same-request_id-but-different-done_blob_uri collision (a
      // deterministic-UUID collision in RequestIds.forRawImpressionUpload) also surfaces as
      // ALREADY_EXISTS, yet findUploadByDoneBlobUri returns null for it — log that collision
      // explicitly (logger.severe) and rethrow instead of the opaque IllegalStateException below.
      val matchingUpload = findUploadByDoneBlob(doneBlobPath, generation, evictionOperationId)
      if (matchingUpload != null) {
        return matchingUpload
      }
      val latestUpload = findLatestUploadByDoneBlob(doneBlobPath)
      if (
        latestUpload != null &&
          latestUpload.hasDoneBlobCreateTime() &&
          Timestamps.compare(latestUpload.doneBlobCreateTime, createTime.toProtoTime()) >= 0
      ) {
        return null
      }
      throw IllegalStateException(
        "createRawImpressionUpload returned ALREADY_EXISTS but no RawImpressionUpload matches " +
          doneBlobPath
      )
    }
  }

  /**
   * Finds the existing `RawImpressionUpload` for the exact done-object version. Used to recover
   * from `ALREADY_EXISTS` on create.
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun findUploadByDoneBlob(
    doneBlobPath: String,
    generation: Long,
    evictionOperationId: String? = null,
  ): RawImpressionUpload? =
    rawImpressionUploadStub
      .listResources { pageToken: String ->
        val response =
          try {
            rpcThrottlers.metadataRead.onReady {
              rawImpressionUploadStub.listRawImpressionUploads(
                listRawImpressionUploadsRequest {
                  parent = dataProviderName
                  filter = rawUploadFilter { doneBlobUri = doneBlobPath }
                  if (pageToken.isNotEmpty()) {
                    this.pageToken = pageToken
                  }
                }
              )
            }
          } catch (e: StatusException) {
            throw Exception("Error listing RawImpressionUploads for $dataProviderName", e)
          }
        ResourceList(response.rawImpressionUploadsList, response.nextPageToken)
      }
      .flattenConcat()
      .firstOrNull {
        it.doneBlobGeneration == generation &&
          (evictionOperationId == null ||
            UploadHealingOperationKey.fromName(it.uploadHealingOperation)
              ?.uploadHealingOperationId == evictionOperationId)
      }

  /** Finds the latest registered revision at [doneBlobPath]. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun findLatestUploadByDoneBlob(doneBlobPath: String): RawImpressionUpload? =
    findLatestUpload(listUploadsByDoneBlob(doneBlobPath))

  private fun findLatestUpload(uploads: List<RawImpressionUpload>): RawImpressionUpload? {
    val timestamped = uploads.filter { it.hasDoneBlobCreateTime() }
    return if (timestamped.isNotEmpty()) {
      timestamped.maxWithOrNull { left, right ->
        val doneTime = Timestamps.compare(left.doneBlobCreateTime, right.doneBlobCreateTime)
        if (doneTime != 0) {
          doneTime
        } else {
          val createTime = Timestamps.compare(left.createTime, right.createTime)
          if (createTime != 0) createTime else left.name.compareTo(right.name)
        }
      }
    } else {
      uploads.maxWithOrNull { left, right -> Timestamps.compare(left.createTime, right.createTime) }
    }
  }

  /** Lists every registered revision for [doneBlobPath]. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listUploadsByDoneBlob(doneBlobPath: String): List<RawImpressionUpload> =
    rawImpressionUploadStub
      .listResources { pageToken: String ->
        val response =
          try {
            rpcThrottlers.metadataRead.onReady {
              rawImpressionUploadStub.listRawImpressionUploads(
                listRawImpressionUploadsRequest {
                  parent = dataProviderName
                  filter = rawUploadFilter { doneBlobUri = doneBlobPath }
                  if (pageToken.isNotEmpty()) this.pageToken = pageToken
                }
              )
            }
          } catch (e: StatusException) {
            throw Exception("Error listing RawImpressionUploads for $dataProviderName", e)
          }
        ResourceList(response.rawImpressionUploadsList, response.nextPageToken)
      }
      .flattenConcat()
      .toList()

  private suspend fun isCurrentDoneBlobGeneration(
    doneBlobUri: BlobUri,
    expectedGeneration: Long,
  ): Boolean = readBlobMetadata(doneBlobUri.key).generation == expectedGeneration

  private suspend fun markRegistrationComplete(upload: RawImpressionUpload) {
    try {
      rpcThrottlers.metadataWrite.onReady {
        rawImpressionUploadStub.markRawImpressionUploadRegistrationComplete(
          markRawImpressionUploadRegistrationCompleteRequest {
            name = upload.name
            etag = upload.etag
            requestId =
              RequestIds.forRawImpressionUploadRegistrationComplete(upload.name, upload.etag)
          }
        )
      }
    } catch (e: StatusException) {
      throw Exception("Error marking RawImpressionUpload ${upload.name} registration complete", e)
    }
  }

  private suspend fun hasRegisteredFiles(uploadName: String): Boolean {
    val response =
      rpcThrottlers.metadataRead.onReady {
        rawImpressionUploadFilesStub.listRawImpressionUploadFiles(
          listRawImpressionUploadFilesRequest {
            parent = uploadName
            pageSize = 1
          }
        )
      }
    return response.rawImpressionUploadFilesCount > 0
  }

  private fun isRegistrationComplete(upload: RawImpressionUpload): Boolean =
    upload.registrationComplete || upload.state != RawImpressionUpload.State.CREATED

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
                val metadata = readSemaphore.withPermit { readBlobMetadata(blob.blobKey) }
                RawBlobVersion(
                  blob = blob,
                  blobUri = BlobUris.buildUri(doneBlobUri, blob.blobKey),
                  generation = metadata.generation,
                  sizeBytes = metadata.sizeBytes,
                )
              }
            }
            .awaitAll()
        }
      )
    }
  }

  /**
   * Validates an object-metadata recovery request before honoring its model-line override.
   *
   * @return the authorized recovery route, or [RecoveryKind.STALE] for an obsolete delivery.
   */
  private suspend fun validateRecovery(
    doneBlobPath: String,
    doneBlobGeneration: Long,
    doneBlobCreateTime: Instant,
    sourceUploadName: String,
    operationId: String,
  ): RecoveryKind {
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
      rpcThrottlers.metadataRead.onReady {
        rawImpressionUploadStub.getRawImpressionUpload(
          getRawImpressionUploadRequest { name = sourceUploadName }
        )
      }
    require(source.doneBlobUri == doneBlobPath) {
      "$sourceUploadName belongs to ${source.doneBlobUri}, not $doneBlobPath"
    }
    require(doneBlobGeneration != source.doneBlobGeneration) {
      "Recovery generation $doneBlobGeneration must differ from source generation " +
        source.doneBlobGeneration
    }
    val doneBlobCreateTimestamp = doneBlobCreateTime.toProtoTime()
    if (source.hasDoneBlobCreateTime()) {
      require(Timestamps.compare(doneBlobCreateTimestamp, source.doneBlobCreateTime) > 0) {
        "Recovery object creation time must be newer than the source upload"
      }
    }
    val latest = findLatestUploadByDoneBlob(doneBlobPath)
    if (
      latest != null &&
        latest.doneBlobGeneration != doneBlobGeneration &&
        latest.hasDoneBlobCreateTime() &&
        Timestamps.compare(latest.doneBlobCreateTime, doneBlobCreateTimestamp) >= 0
    ) {
      return RecoveryKind.STALE
    }
    val registeredRecovery = findUploadByDoneBlob(doneBlobPath, doneBlobGeneration, operationId)
    val isInitialDelivery = latest?.name == sourceUploadName
    val isApprovedQuarantinedDelivery =
      latest?.state == RawImpressionUpload.State.CORRECTION_REQUIRED &&
        latest.replacesRawImpressionUpload == sourceUploadName &&
        latest.doneBlobGeneration == doneBlobGeneration
    val isRetryOfLatestRecovery =
      registeredRecovery != null &&
        latest?.name == registeredRecovery.name &&
        registeredRecovery.replacesRawImpressionUpload == sourceUploadName
    val isRetryAfterIncompleteRecovery =
      latest != null &&
        latest.replacesRawImpressionUpload == sourceUploadName &&
        !isRegistrationComplete(latest)
    require(
      isInitialDelivery ||
        isApprovedQuarantinedDelivery ||
        isRetryOfLatestRecovery ||
        isRetryAfterIncompleteRecovery
    ) {
      "$sourceUploadName has been superseded by ${latest?.name}; recover the latest revision"
    }

    val rowsByCmmsModelLine = listModelLines(sourceUploadName).associateBy { it.cmmsModelLine }
    val requestedRows = overrideModelLines.mapNotNull { rowsByCmmsModelLine[it] }
    require(
      requestedRows.size == overrideModelLines.size &&
        requestedRows.all { it.state == RawImpressionUploadModelLine.State.FAILED }
    ) {
      "Recovery override must identify FAILED source model-line rows"
    }
    val recoveryActions = requestedRows.mapTo(mutableSetOf()) { it.recoveryAction }
    require(
      recoveryActions.size == 1 &&
        recoveryActions.single() in
          setOf(
            RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION,
            RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY,
          ) &&
        requestedRows.all { it.evictionOperationId == operationId }
    ) {
      "Recovery source rows do not belong to one replay action from eviction operation $operationId"
    }
    for (row in requestedRows) {
      requireRecoveryPredecessorReady(row)
    }
    val recoveryAction = recoveryActions.single()
    if (
      recoveryAction == RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
    ) {
      val correctionModelLines =
        rowsByCmmsModelLine.values
          .filter { it.state == RawImpressionUploadModelLine.State.FAILED }
          .filter {
            it.recoveryAction ==
              RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
          }
          .filter { it.evictionOperationId == operationId }
          .mapTo(mutableSetOf()) { it.cmmsModelLine }
      require(overrideModelLines.toSet() == correctionModelLines) {
        "Correction replay must contain the complete set of FAILED correction model lines; " +
          "requested=$overrideModelLines, recoverable=$correctionModelLines"
      }
      return RecoveryKind.EDP_CORRECTION
    }
    val recoverableModelLines =
      rowsByCmmsModelLine.values
        .filter { it.state == RawImpressionUploadModelLine.State.FAILED }
        .filter {
          it.recoveryAction ==
            RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
        }
        .filter { it.evictionOperationId == operationId }
        .filter { hasDeletedSnapshotHistory(sourceUploadName, it.cmmsModelLine) }
        .mapTo(mutableSetOf()) { it.cmmsModelLine }
    require(overrideModelLines.toSet() == recoverableModelLines) {
      "Recovery override must contain the complete set of FAILED memoized model lines whose " +
        "snapshots were deleted; requested=$overrideModelLines, recoverable=$recoverableModelLines"
    }
    return RecoveryKind.OPERATOR_RECOVERY
  }

  /** Enforces the persisted dependency before accepting an EDP correction upload. */
  private suspend fun validateEdpReplacementOrder(
    previousRevision: RawImpressionUpload
  ): EdpReplacementAuthorization? {
    val evictedRows =
      listModelLines(previousRevision.name).filter {
        it.state == RawImpressionUploadModelLine.State.FAILED &&
          it.failureReason == RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
      }
    if (evictedRows.isEmpty()) return null
    check(
      evictedRows.none {
        it.recoveryAction ==
          RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
      }
    ) {
      "${previousRevision.name} requires the operator recovery command, not an EDP upload"
    }
    check(
      evictedRows.none {
        it.recoveryAction ==
          RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_NO_REPLACEMENT
      }
    ) {
      "${previousRevision.name} was permanently removed and does not accept an EDP replacement"
    }
    for (row in evictedRows.filter { it.recoveryPredecessorRawImpressionUpload.isNotEmpty() }) {
      requireRecoveryPredecessorReady(row)
    }
    val evictionOperationIds =
      evictedRows.map { it.evictionOperationId }.filter { it.isNotEmpty() }.distinct()
    check(evictionOperationIds.size <= 1) {
      "${previousRevision.name} belongs to multiple eviction operations"
    }
    return EdpReplacementAuthorization(
      evictionOperationIds.singleOrNull(),
      evictedRows
        .filter {
          it.recoveryAction ==
            RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
        }
        .map { it.cmmsModelLine },
    )
  }

  /** Requires the latest replacement of this row's predecessor to own a live completed snapshot. */
  private suspend fun requireRecoveryPredecessorReady(row: RawImpressionUploadModelLine) {
    val predecessorName = row.recoveryPredecessorRawImpressionUpload
    if (predecessorName.isEmpty()) return
    val predecessor =
      rpcThrottlers.metadataRead.onReady {
        rawImpressionUploadStub.getRawImpressionUpload(
          getRawImpressionUploadRequest { name = predecessorName }
        )
      }
    val revisions = listUploadsByDoneBlob(predecessor.doneBlobUri)
    val latest =
      checkNotNull(findLatestUpload(revisions)) {
        "No upload revision found for recovery predecessor $predecessorName"
      }
    check(
      latest.name == predecessorName || replacesUpload(latest.name, predecessorName, revisions)
    ) {
      "Latest upload ${latest.name} does not replace recovery predecessor $predecessorName"
    }
    val replacementRow =
      listModelLines(latest.name).firstOrNull { it.cmmsModelLine == row.cmmsModelLine }
    check(replacementRow?.state == RawImpressionUploadModelLine.State.COMPLETED) {
      "Recovery predecessor $predecessorName has not been replaced by a completed upload for " +
        row.cmmsModelLine
    }
    check(hasActiveSnapshot(latest.name, row.cmmsModelLine)) {
      "Recovery predecessor $predecessorName has no live replacement snapshot for " +
        row.cmmsModelLine
    }
  }

  private fun replacesUpload(
    candidateName: String,
    predecessorName: String,
    revisions: List<RawImpressionUpload>,
  ): Boolean {
    val revisionsByName = revisions.associateBy { it.name }
    val visited = mutableSetOf<String>()
    var current = revisionsByName[candidateName]?.replacesRawImpressionUpload.orEmpty()
    while (current.isNotEmpty() && visited.add(current)) {
      if (current == predecessorName) return true
      current = revisionsByName[current]?.replacesRawImpressionUpload.orEmpty()
    }
    return false
  }

  private suspend fun listModelLines(uploadName: String): List<RawImpressionUploadModelLine> {
    val rows = mutableListOf<RawImpressionUploadModelLine>()
    var pageToken = ""
    do {
      val response =
        rpcThrottlers.metadataRead.onReady {
          rawImpressionUploadModelLineStub.listRawImpressionUploadModelLines(
            listRawImpressionUploadModelLinesRequest {
              parent = uploadName
              this.pageToken = pageToken
            }
          )
        }
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
        rpcThrottlers.metadataRead.onReady {
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
        }
      if (response.rankIndexBlobsList.any { !it.hasDeleteTime() }) return false
      found = found || response.rankIndexBlobsCount > 0
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return found
  }

  private suspend fun hasActiveSnapshot(uploadName: String, cmmsModelLine: String): Boolean {
    val response =
      rpcThrottlers.metadataRead.onReady {
        rankIndexBlobStub.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            parent = uploadName
            pageSize = 1
            filter = rankIndexFilter {
              blobType = RankIndexBlob.BlobType.SNAPSHOT
              this.cmmsModelLine = cmmsModelLine
            }
          }
        )
      }
    return response.rankIndexBlobsCount > 0
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
   * @param blobs raw-impression object versions in the upload.
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
              async {
                readSemaphore.withPermit {
                  readEventDate(
                    generationMatchedBlobUri(blobVersion.blobUri, blobVersion.generation)
                  )
                }
              }
          }
          .mapValues { (_, deferred) -> deferred.await() }
      }

      val request = batchCreateRawImpressionUploadFilesRequest {
        parent = uploadName
        for (blobVersion in chunk) {
          val blob = blobVersion.blob
          requests += createRawImpressionUploadFileRequest {
            parent = uploadName
            rawImpressionUploadFile = rawImpressionUploadFile {
              blobUri = blobVersion.blobUri
              blobGeneration = blobVersion.generation
              sizeBytes = blobVersion.sizeBytes
              this.eventDate = eventDateByBlobKey.getValue(blob.blobKey).toProtoDate()
            }
            requestId = RequestIds.forRawImpressionUploadFile(uploadName, blobVersion.blobUri)
          }
        }
      }

      try {
        rpcThrottlers.metadataWrite.onReady {
          rawImpressionUploadFilesStub.batchCreateRawImpressionUploadFiles(request)
        }
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
        rpcThrottlers.metadataWrite.onReady {
          rawImpressionUploadModelLineStub.batchCreateRawImpressionUploadModelLines(request)
        }
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

    private val NON_ADDITIVE_CLASSIFICATIONS =
      setOf(
        RawImpressionUploadManifestClassifier.Classification.EDITED,
        RawImpressionUploadManifestClassifier.Classification.REMOVED,
        RawImpressionUploadManifestClassifier.Classification.MIXED,
      )

    private val DATA_PROVIDER_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.data_provider")
    private val UPLOAD_STATUS_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.dispatch_status")
    private val CORRECTION_CLASSIFICATION_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.correction_classification")
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

data class RawImpressionBlobMetadata(
  val generation: Long,
  val sizeBytes: Long,
  val createTime: Instant,
)
