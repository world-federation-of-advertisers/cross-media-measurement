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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.Parser
import io.grpc.Status
import io.grpc.StatusException
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.time.Instant
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigestExtractor
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionSource
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.getVidLabelingJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineCompletedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markVidLabelingJobFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markVidLabelingJobSucceededRequest
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

/**
 * TEE application for VID labeling that processes WorkItems from a Pub/Sub queue.
 *
 * Receives WorkItems containing [VidLabelerParams], resolves required dependencies (KMS clients,
 * storage config), and delegates to a [VidLabeler] for the actual labeling work.
 *
 * This wiring implements the memoized rank-index Phase-2 path only
 * ([VidLabelerParams.memoized_params] set): the TEE loads the per-subpool rank index from the
 * `RankIndexBlobService`, derives each VID from its memoized rank (the labeler hashes any overflow
 * / unseen fingerprint), writes the encrypted labeled output, marks the `VidLabelingJob`
 * `SUCCEEDED`, and — when this call was the last job out for a model line — transitions the parent
 * `RawImpressionUploadModelLine` to `COMPLETED` and drops a `done` marker blob that triggers
 * downstream DataAvailabilitySync. On failure the job is marked `FAILED` (best-effort) and the
 * error rethrown so the TEE framework nacks.
 *
 * @param subscriptionId Pub/Sub subscription for VID labeling queue.
 * @param queueSubscriber handles Pub/Sub pull.
 * @param parser protobuf [Parser] for [WorkItem] messages.
 * @param workItemsClient gRPC stub for WorkItems service.
 * @param workItemAttemptsClient gRPC stub for WorkItemAttempts service.
 * @param kmsClients per-`DataProvider` [KmsClient]s (each the EDP's own KEK), used for BOTH the
 *   raw-impression PME decrypt and the labeled-output encrypt — exactly one client per EDP.
 * @param getStorageConfig builds a [StorageConfig] from [VidLabelerParams.StorageParams].
 * @param vidLabelingJobsStub stub to mark this WorkItem's `VidLabelingJob` `SUCCEEDED`/`FAILED` and
 *   learn whether it is the last job out for one or more model lines.
 * @param rawImpressionUploadModelLinesStub stub for transitioning the parent
 *   `RawImpressionUploadModelLine` to `COMPLETED` on last-job-out.
 * @param rankIndexBlobsStub stub used by [MemoizedRankIndex.load] to resolve the per-subpool
 *   rank-index blob pointers.
 * @param rawImpressionUploadFilesStub stub used by [RawImpressionSource] to discover this upload's
 *   raw-impression files.
 * @param buildParquetStorageClient builds a [ParquetStorageClient] for the raw-impressions storage,
 *   threaded with the per-EDP [KmsClient] for PME decryption.
 * @param buildVidRankMapStorageClient builds a [StorageClient] for the vid-rank-map storage read by
 *   [RankIndexStore].
 * @param loadAssigner loads the compiled VID model (C++/JNI) for a model blob URI into a
 *   [VidAssigner].
 * @param impressionConverter converts Parquet rows into [ConvertedImpression]s (schema seam).
 * @param eventIdDigestExtractor computes the 12-byte `EventIdDigest` of an event id.
 */
class VidLabelerApp(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  workItemsClient: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub,
  private val kmsClients: Map<String, KmsClient>,
  private val getStorageConfig: (VidLabelerParams.StorageParams) -> StorageConfig,
  private val vidLabelingJobsStub: VidLabelingJobServiceCoroutineStub,
  private val rawImpressionUploadModelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
  private val rawImpressionUploadFilesStub: RawImpressionUploadFileServiceCoroutineStub,
  private val buildParquetStorageClient: (StorageConfig, KmsClient) -> ParquetStorageClient,
  private val buildVidRankMapStorageClient: (StorageConfig) -> StorageClient,
  private val loadAssigner: suspend (modelBlobUri: String) -> VidAssigner,
  private val impressionConverter: ImpressionConverter,
  private val eventIdDigestExtractor: EventIdDigestExtractor = EventIdDigestExtractor(),
) :
  BaseTeeApplication(
    subscriptionId = subscriptionId,
    queueSubscriber = queueSubscriber,
    parser = parser,
    workItemsStub = workItemsClient,
    workItemAttemptsStub = workItemAttemptsClient,
  ) {

  override suspend fun runWork(message: Any) {
    val workItemParams = message.unpack(WorkItemParams::class.java)
    val params = workItemParams.appParams.unpack(VidLabelerParams::class.java)

    val dataProvider = params.dataProvider
    require(dataProvider.isNotEmpty()) { "data_provider must not be empty" }
    require(params.hasRawImpressionsStorageParams()) {
      "raw_impressions_storage_params must be set"
    }
    require(params.hasVidLabeledImpressionsStorageParams()) {
      "vid_labeled_impressions_storage_params must be set"
    }
    // Scope is the memoized rank-index Phase-2 path; the non-memoized path is wired separately.
    require(params.hasMemoizedParams()) { "memoized_params must be set" }
    val mp = params.memoizedParams

    // One client per EDP (the EDP's own KEK), used for BOTH the raw-impression PME decrypt and the
    // labeled-output encrypt — mirrors SubpoolAssigner's single-client model.
    val kmsClient =
      requireNotNull(kmsClients[dataProvider]) { "KMS client not found for $dataProvider" }

    val config =
      requireNotNull(params.modelLineConfigsMap[mp.modelLine]) {
        "model_line_configs must contain an entry for ${mp.modelLine}"
      }

    try {
      // Gate on job state first: on Pub/Sub redelivery of an already-completed job, skip relabeling
      // (the output is idempotent via deterministic blob keys) but still run the idempotent mark +
      // last-job-out recovery so a crash between label() and the mark cannot drop the completion.
      val job =
        vidLabelingJobsStub.getVidLabelingJob(getVidLabelingJobRequest { name = mp.vidLabelingJob })
      if (job.state != VidLabelingJob.State.SUCCEEDED) {
        label(params, mp, config, dataProvider, kmsClient)
      } else {
        logger.info("VidLabelingJob ${mp.vidLabelingJob} already SUCCEEDED; skipping relabel")
      }

      markSucceededAndTransition(params, mp, job.etag)
    } catch (t: Throwable) {
      markFailedBestEffort(mp.vidLabelingJob, t)
      throw t
    }
  }

  /** Labels this WorkItem's raw-impression files for [mp]'s single model line (memoized path). */
  private suspend fun label(
    params: VidLabelerParams,
    mp: VidLabelerParams.MemoizedParams,
    config: VidLabelerParams.ModelLineConfig,
    dataProvider: String,
    kmsClient: KmsClient,
  ) {
    val rankIndexStore =
      RankIndexStore(
        buildVidRankMapStorageClient(getStorageConfig(mp.vidRankMapStorageParams)),
        kmsClient,
      )
    val rankIndex =
      MemoizedRankIndex.load(rankIndexBlobsStub, rankIndexStore, dataProvider, mp.modelLine)

    // The raw event-id column is the raw-impression field mapped to LabelerInput's `event_id.id`.
    val eventIdColumn =
      requireNotNull(config.labelerInputFieldMappingMap[EVENT_ID_FIELD_PATH]) {
        "labeler_input_field_mapping must map '$EVENT_ID_FIELD_PATH' to the raw event-id column"
      }

    // The memoized path labels exactly the files carried on this WorkItem; the whole batch is one
    // shard, so shardIndex=0 / totalShards=1.
    val rawImpressionSource =
      RawImpressionSource(
        parquetStorageClient =
          buildParquetStorageClient(
            getStorageConfig(params.rawImpressionsStorageParams),
            kmsClient,
          ),
        rawImpressionUploadFilesStub = rawImpressionUploadFilesStub,
        rawImpressionUpload = parentUpload(mp.vidLabelingJob),
        eventIdColumn = eventIdColumn,
        shardIndex = 0,
        totalShards = 1,
        eventIdDigestExtractor = eventIdDigestExtractor,
      )

    val activeWindow =
      ActiveWindow.of(
        config.activeStartTime.toInstant(),
        if (config.hasActiveEndTime()) config.activeEndTime.toInstant() else null,
      )
    val modelLineSpec =
      ModelLineSpec(
        modelLine = mp.modelLine,
        modelBlobUri = mp.modelBlobPath,
        activeWindow = activeWindow,
        config = config,
        rankIndex = rankIndex,
      )

    VidLabeler(
        rawImpressionSource = rawImpressionSource,
        modelLineSpecs = listOf(modelLineSpec),
        overrideModelLines = emptyList(),
        vidModelLoader = VidModelLoader(loadAssigner),
        impressionConverter = impressionConverter,
        encryptKmsClient = kmsClient,
        // The labeled output is wrapped with the EDP's KEK, the same one the rank-index blobs were
        // written with; read it from the loaded RankIndexBlobs so there is no separate KEK field.
        encryptKekUri = rankIndex.kekUri,
        outputStorageParams = params.vidLabeledImpressionsStorageParams,
        storageConfig = getStorageConfig(params.vidLabeledImpressionsStorageParams),
      )
      .label()
  }

  /**
   * Marks this WorkItem's `VidLabelingJob` `SUCCEEDED` and, when the service reports this call
   * completed one or more model lines (last-job-out), transitions each completed model line's
   * parent `RawImpressionUploadModelLine` to `COMPLETED` and drops the `done` marker blob.
   * Idempotent on Pub/Sub redelivery: the mark is keyed by a deterministic `request_id`, the
   * transition swallows the benign already-advanced races, and the done blob has a deterministic
   * key.
   */
  private suspend fun markSucceededAndTransition(
    params: VidLabelerParams,
    mp: VidLabelerParams.MemoizedParams,
    etag: String,
  ) {
    val response =
      vidLabelingJobsStub.markVidLabelingJobSucceeded(
        markVidLabelingJobSucceededRequest {
          name = mp.vidLabelingJob
          this.etag = etag
          // AIP-155 retry-idempotency key: a Pub/Sub redelivery reuses the same request_id so the
          // server returns the cached result instead of hitting the etag-mismatch path.
          requestId = deterministicUuid("${mp.vidLabelingJob}|succeeded")
        }
      )

    if (!response.hasLastVidLabelingJobResult()) return

    val upload = parentUpload(mp.vidLabelingJob)
    for (completedModelLine in response.lastVidLabelingJobResult.completedModelLinesList) {
      val parent = getParent(upload, completedModelLine)
      if (parent == null) {
        logger.warning(
          "RawImpressionUploadModelLine not found for $completedModelLine under $upload; " +
            "cannot mark COMPLETED"
        )
        continue
      }
      markParentCompleted(parent)
    }

    // Drop a `done` marker blob under the labeled-impressions prefix. This triggers DataWatcher ->
    // DataAvailabilitySync, which crawls that folder for the per-blob .metadata.binpb sidecars
    // written by VidLabelingSink. Written only on last-job-out so the crawl runs once the model
    // line's output is complete.
    writeDoneBlob(params.vidLabeledImpressionsStorageParams)
  }

  /**
   * Transitions [parent] to `COMPLETED`, passing its etag for AIP-154 optimistic locking. Only
   * attempted when the parent is still `LABELING`; swallows only the benign already-advanced races
   * (FAILED_PRECONDITION / ABORTED) so a redelivered last-job-out is a no-op, and rethrows
   * everything else so a transient failure nacks the message.
   */
  private suspend fun markParentCompleted(parent: RawImpressionUploadModelLine) {
    if (parent.state != RawImpressionUploadModelLine.State.LABELING) return
    try {
      rawImpressionUploadModelLinesStub.markRawImpressionUploadModelLineCompleted(
        markRawImpressionUploadModelLineCompletedRequest {
          name = parent.name
          etag = parent.etag
        }
      )
    } catch (e: StatusException) {
      if (
        e.status.code != Status.Code.FAILED_PRECONDITION && e.status.code != Status.Code.ABORTED
      ) {
        throw e
      }
      logger.info(
        "markRawImpressionUploadModelLineCompleted(${parent.name}) already advanced " +
          "(${e.status.code}); treating as done"
      )
    }
  }

  /** Best-effort transition of this job to FAILED so operators see the failure; never throws. */
  private suspend fun markFailedBestEffort(vidLabelingJob: String, cause: Throwable) {
    try {
      val job =
        vidLabelingJobsStub.getVidLabelingJob(getVidLabelingJobRequest { name = vidLabelingJob })
      if (job.state == VidLabelingJob.State.CREATED) {
        vidLabelingJobsStub.markVidLabelingJobFailed(
          markVidLabelingJobFailedRequest {
            name = vidLabelingJob
            etag = job.etag
            // request_id is OPTIONAL on this RPC; deterministic so a redelivery is idempotent.
            requestId = deterministicUuid("$vidLabelingJob|failed")
            errorMessage = (cause.message ?: cause::class.java.simpleName).take(MAX_ERROR_MESSAGE)
          }
        )
      } else {
        logger.info(
          "VidLabelingJob $vidLabelingJob already in state ${job.state}; not re-marking FAILED"
        )
      }
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Failed to mark VidLabelingJob $vidLabelingJob FAILED", e)
    }
  }

  /** Writes an empty `done` marker blob under the labeled-impressions prefix. */
  private suspend fun writeDoneBlob(outputStorageParams: VidLabelerParams.StorageParams) {
    val doneUri = "${outputStorageParams.impressionsBlobPrefix}/labeled-impressions/done"
    val storageConfig = getStorageConfig(outputStorageParams)
    val blobUri = SelectedStorageClient.parseBlobUri(doneUri)
    SelectedStorageClient(blobUri, storageConfig.rootDirectory, storageConfig.projectId)
      .writeBlob(blobUri.key, ByteString.EMPTY)
    logger.info("Wrote done marker blob to $doneUri")
  }

  /**
   * Returns the parent `RawImpressionUploadModelLine` for ([upload], [modelLine]), located via
   * `ListRawImpressionUploadModelLines` filtered by model line, or `null` if absent.
   */
  private suspend fun getParent(upload: String, modelLine: String): RawImpressionUploadModelLine? {
    var parentRow: RawImpressionUploadModelLine? = null
    rawImpressionUploadModelLinesStub
      .listResources { pageToken: String ->
        val response =
          listRawImpressionUploadModelLines(
            listRawImpressionUploadModelLinesRequest {
              parent = upload
              filter =
                ListRawImpressionUploadModelLinesRequestKt.filter { cmmsModelLine = modelLine }
              this.pageToken = pageToken
            }
          )
        ResourceList(response.rawImpressionUploadModelLinesList, response.nextPageToken)
      }
      .collect { page ->
        page.forEach { line -> if (line.cmmsModelLine == modelLine) parentRow = line }
      }
    return parentRow
  }

  /**
   * Derives the parent `RawImpressionUpload` resource name from a `VidLabelingJob` resource name by
   * stripping the `/vidLabelingJobs/{job}` segment.
   */
  private fun parentUpload(vidLabelingJob: String): String =
    vidLabelingJob.substringBefore("/vidLabelingJobs/")

  /**
   * Derives a deterministic UUID4 from [seed], stable across redeliveries, for use as an AIP-155
   * `request_id`. Computed from an MD5 digest of the seed with the RFC-4122 version (4) and variant
   * bits forced, so it satisfies a field's `format = UUID4`.
   */
  private fun deterministicUuid(seed: String): String {
    val bytes = MessageDigest.getInstance("MD5").digest(seed.toByteArray(Charsets.UTF_8))
    bytes[6] = ((bytes[6].toInt() and 0x0f) or 0x40).toByte() // version 4
    bytes[8] = ((bytes[8].toInt() and 0x3f) or 0x80).toByte() // variant 10xx
    val buffer = ByteBuffer.wrap(bytes)
    return UUID(buffer.long, buffer.long).toString()
  }

  companion object {
    private val logger = Logger.getLogger(VidLabelerApp::class.java.name)

    private const val MAX_ERROR_MESSAGE = 1024

    /** LabelerInput field path whose mapped raw column carries the event id used for the digest. */
    private const val EVENT_ID_FIELD_PATH = "event_id.id"

    private fun com.google.protobuf.Timestamp.toInstant(): Instant =
      Instant.ofEpochSecond(seconds, nanos.toLong())
  }
}
