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
import io.opentelemetry.api.common.Attributes
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.UUID
import java.util.logging.Logger
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigestExtractor
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionSource
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
import org.wfanet.measurement.edpaggregator.v1alpha.markVidLabelingJobSucceededRequest
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

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
 * downstream DataAvailabilitySync.
 *
 * Failure model: [runWork] does NOT mark the job `FAILED` itself. A transient failure propagates
 * out of [runWork] so the TEE framework nacks the message, leaving the job in `LABELING`/`CREATED`
 * for Pub/Sub to redeliver and retry. The terminal `FAILED` state is written by the DLQ listener on
 * retry exhaustion (see the design's failure model), which owns the single authoritative FAILED
 * transition.
 *
 * @param subscriptionId Pub/Sub subscription for VID labeling queue.
 * @param queueSubscriber handles Pub/Sub pull.
 * @param parser protobuf [Parser] for [WorkItem] messages.
 * @param workItemsClient gRPC stub for WorkItems service.
 * @param workItemAttemptsClient gRPC stub for WorkItemAttempts service.
 * @param kmsClients per-`DataProvider` [KmsClient]s (each the EDP's own KEK), used for BOTH the
 *   raw-impression PME decrypt and the labeled-output encrypt — exactly one client per EDP.
 * @param getStorageConfig builds a [StorageConfig] from [VidLabelerParams.StorageParams].
 * @param vidLabelingJobsStub stub to mark this WorkItem's `VidLabelingJob` `SUCCEEDED` and learn
 *   whether it is the last job out for one or more model lines.
 * @param rawImpressionUploadModelLinesStub stub for transitioning the parent
 *   `RawImpressionUploadModelLine` to `COMPLETED` on last-job-out.
 * @param rankIndexBlobsStub stub used by [MemoizedRankIndex.load] to resolve the per-subpool
 *   rank-index blob pointers.
 * @param rawImpressionUploadFilesStub stub used by [RawImpressionSource] to discover this upload's
 *   raw-impression files.
 * @param buildParquetStorageClient builds a [ParquetStorageClient] for the raw-impressions storage,
 *   threaded with the per-EDP [KmsClient] for PME decryption.
 * @param buildVidRankMapStorageClient builds a [ConditionalOperationStorageClient] for the
 *   vid-rank-map storage read by [RankIndexStore].
 * @param loadAssigner loads the compiled VID model (C++/JNI) for a model blob URI into a
 *   [VidAssigner].
 * @param buildImpressionConverter builds the per-(WorkItem, model line) [ImpressionConverter],
 *   given the model line, its [VidLabelerParams.ModelLineConfig], and the per-EventGroup entity
 *   keys; the production factory loads the EventTemplate descriptor from the config blob.
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
  private val buildVidRankMapStorageClient: (StorageConfig) -> ConditionalOperationStorageClient,
  private val loadAssigner: suspend (modelBlobUri: String) -> VidAssigner,
  private val buildImpressionConverter:
    suspend (
      modelLine: String,
      config: VidLabelerParams.ModelLineConfig,
      entityKeysByInputBlobUri: Map<String, VidLabelerParams.InputFileEntityKeys>,
    ) -> ImpressionConverter,
  private val eventIdDigestExtractor: EventIdDigestExtractor = EventIdDigestExtractor(),
  private val metrics: VidLabelerAppMetrics = VidLabelerAppMetrics(),
) :
  BaseTeeApplication(
    subscriptionId = subscriptionId,
    queueSubscriber = queueSubscriber,
    parser = parser,
    workItemsStub = workItemsClient,
    workItemAttemptsStub = workItemAttemptsClient,
  ) {

  /**
   * One [VidModelLoader] for the whole process so its per-model-blob cache — and thus each multi-GB
   * `Labeler` build — survives across WorkItems. [loadAssigner] is process-scoped, so a single
   * loader is safe to share across every [runWork] / [label] call.
   */
  private val vidModelLoader = VidModelLoader(loadAssigner)

  /**
   * Processes one VID-labeling WorkItem.
   *
   * Any exception thrown here propagates (including a coroutine `CancellationException`) so the TEE
   * framework nacks the message: a transient failure is retried by Pub/Sub and the job state stays
   * in `LABELING`/`CREATED`. This worker never marks the job `FAILED` itself — the single
   * authoritative terminal `FAILED` transition is owned by the DLQ listener on retry exhaustion
   * (see the class-level failure model).
   */
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
    // [mp] identifies the VidLabelingJob. The require(...) validations above run BEFORE [mp] is
    // known — a WorkItem with no memoized_params cannot be attributed to a job.
    val mp = params.memoizedParams

    // Per-WorkItem metric dimensions: every counter/histogram for this call is keyed by the
    // DataProvider and the WorkItem's model line.
    val attributes =
      Attributes.of(metrics.DATA_PROVIDER_ATTR, dataProvider, metrics.MODEL_LINE_ATTR, mp.modelLine)
    val startNanos = System.nanoTime()
    try {
      // One client per EDP (the EDP's own KEK), used for BOTH the raw-impression PME decrypt
      // and the labeled-output encrypt — mirrors SubpoolAssigner's single-client model.
      val kmsClient =
        requireNotNull(kmsClients[dataProvider]) { "KMS client not found for $dataProvider" }

      val config =
        requireNotNull(params.modelLineConfigsMap[mp.modelLine]) {
          "model_line_configs must contain an entry for ${mp.modelLine}"
        }

      // Gate on job state first: on Pub/Sub redelivery of an already-completed job, skip relabeling
      // (the output is idempotent via deterministic blob keys) but still run the idempotent mark +
      // last-job-out recovery so a crash between label() and the mark cannot drop the completion.
      val job =
        vidLabelingJobsStub.getVidLabelingJob(getVidLabelingJobRequest { name = mp.vidLabelingJob })
      if (job.state != VidLabelingJob.State.SUCCEEDED) {
        label(params, mp, config, dataProvider, kmsClient, job.rawImpressionUploadFilesList)
      } else {
        logger.info("VidLabelingJob ${mp.vidLabelingJob} already SUCCEEDED; skipping relabel")
      }

      markSucceededAndTransition(params, mp, dataProvider, job.etag, attributes)
      // Count only successful completions; a thrown failure skips this and nacks the message.
      metrics.workItemsProcessedCounter.add(1, attributes)
    } finally {
      // Record wall-clock for every attempt (success or failure) so latency dashboards see retries.
      metrics.workItemDurationHistogram.record(
        (System.nanoTime() - startNanos) / NANOS_PER_SECOND,
        attributes,
      )
    }
  }

  /** Labels this WorkItem's raw-impression files for [mp]'s single model line (memoized path). */
  private suspend fun label(
    params: VidLabelerParams,
    mp: VidLabelerParams.MemoizedParams,
    config: VidLabelerParams.ModelLineConfig,
    dataProvider: String,
    kmsClient: KmsClient,
    inputFiles: List<String>,
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

    // File-list mode: label exactly the RawImpressionUploadFiles carried on this WorkItem's
    // VidLabelingJob (the bin-packed batch), each read whole (no fingerprint-shard filter).
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
        eventIdDigestExtractor = eventIdDigestExtractor,
        inputFiles = inputFiles,
      )

    val activeWindow =
      ActiveWindow.of(
        // active_start_time is required on ModelLineConfig (a model line always has an active
        // start),
        // so ActiveWindow.of's non-null start parameter is satisfied directly; only the end is
        // optional and mapped to a null (open-ended) upper bound.
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

    val impressionConverter =
      buildImpressionConverter(mp.modelLine, config, params.entityKeysByInputBlobUriMap)

    VidLabeler(
        rawImpressionSource = rawImpressionSource,
        modelLineSpecs = listOf(modelLineSpec),
        overrideModelLines = emptyList(),
        vidModelLoader = vidModelLoader,
        impressionConverter = impressionConverter,
        encryptKmsClient = kmsClient,
        // The labeled output is wrapped with the EDP's KEK, the same one the rank-index blobs were
        // written with; read it from the loaded RankIndexBlobs so there is no separate KEK field.
        encryptKekUri = rankIndex.kekUri,
        outputStorageParams = params.vidLabeledImpressionsStorageParams,
        storageConfig = getStorageConfig(params.vidLabeledImpressionsStorageParams),
        dataProvider = dataProvider,
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
    dataProvider: String,
    etag: String,
    attributes: Attributes,
  ) {
    val response =
      try {
        vidLabelingJobsStub.markVidLabelingJobSucceeded(
          markVidLabelingJobSucceededRequest {
            name = mp.vidLabelingJob
            // [etag] was captured from GetVidLabelingJob before label(); together with the
            // deterministic request_id below it covers Pub/Sub *redelivery* under the single-writer
            // assumption (only this worker mutates the job), NOT a concurrent first-delivery
            // mutation by another writer.
            this.etag = etag
            // AIP-155 retry-idempotency key: a Pub/Sub redelivery reuses the same request_id so the
            // server returns the cached result instead of hitting the etag-mismatch path.
            requestId = deterministicUuid("${mp.vidLabelingJob}|succeeded")
          }
        )
      } catch (e: StatusException) {
        metrics.markSucceededFailuresCounter.add(1, attributes)
        throw e
      }

    if (!response.hasLastVidLabelingJobResult()) return

    val upload = parentUpload(mp.vidLabelingJob)
    // One unfiltered List of every model line under the upload (a superset of the lines this call
    // completed) instead of one filtered List per completed model line. Keyed by model line so each
    // completed model line resolves to its parent row (name + etag) for the COMPLETED transition.
    val parentsByModelLine = listAllModelLines(upload).associateBy { it.cmmsModelLine }
    for (completedModelLine in response.lastVidLabelingJobResult.completedModelLinesList) {
      val parent = parentsByModelLine[completedModelLine]
      if (parent == null) {
        logger.warning(
          "RawImpressionUploadModelLine not found for $completedModelLine under $upload; " +
            "cannot mark COMPLETED"
        )
        continue
      }
      markParentCompleted(parent, dataProvider)
    }

    // Drop the single `labeled-impressions/done` marker only once *every* model line of this
    // upload has reached COMPLETED. The marker makes DataAvailabilitySync crawl the whole output
    // folder, which mixes every model line's labeled blobs; gating on full-upload completion keeps
    // it from publishing a model line whose labeling is still in flight. This re-lists *after* the
    // marks above so the last worker to finish observes every concurrent writer's COMPLETED state.
    // Idempotent: a redelivery (or a concurrent last-out on the final model line) just rewrites the
    // same empty blob.
    if (allModelLinesCompleted(upload)) {
      writeDoneBlob(params.vidLabeledImpressionsStorageParams)
      metrics.doneBlobsWrittenCounter.add(
        1,
        Attributes.of(metrics.DATA_PROVIDER_ATTR, dataProvider),
      )
    }
  }

  /**
   * Transitions [parent] to `COMPLETED`, passing its etag for AIP-154 optimistic locking. Only
   * attempted when the parent is still `LABELING`; swallows only the benign already-advanced races
   * (FAILED_PRECONDITION / ABORTED) so a redelivered last-job-out is a no-op, and rethrows
   * everything else so a transient failure nacks the message.
   */
  private suspend fun markParentCompleted(
    parent: RawImpressionUploadModelLine,
    dataProvider: String,
  ) {
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
        metrics.markCompletedFailuresCounter.add(
          1,
          Attributes.of(
            metrics.DATA_PROVIDER_ATTR,
            dataProvider,
            metrics.MODEL_LINE_ATTR,
            parent.cmmsModelLine,
          ),
        )
        throw e
      }
      logger.info(
        "markRawImpressionUploadModelLineCompleted(${parent.name}) already advanced " +
          "(${e.status.code}); treating as done"
      )
    }
  }

  /**
   * Returns true iff every [RawImpressionUploadModelLine] under [upload] is in the terminal
   * `COMPLETED` state. Read *after* this WorkItem marked its own model line(s) `COMPLETED`, so the
   * last model line of the upload to finish observes the whole set complete. Returns false for an
   * upload with no model lines; a single `FAILED` model line keeps it false until an operator
   * resolves it.
   */
  private suspend fun allModelLinesCompleted(upload: String): Boolean {
    val rows = listAllModelLines(upload)
    return rows.isNotEmpty() &&
      rows.all { it.state == RawImpressionUploadModelLine.State.COMPLETED }
  }

  /**
   * Lists every [RawImpressionUploadModelLine] under [upload] (no `cmms_model_line` filter). Used
   * both to resolve the parent rows of the model lines this call completed and, on a fresh
   * post-mark read, to gate the upload-wide `done` blob.
   */
  private suspend fun listAllModelLines(upload: String): List<RawImpressionUploadModelLine> {
    val rows = mutableListOf<RawImpressionUploadModelLine>()
    rawImpressionUploadModelLinesStub
      .listResources { pageToken: String ->
        val response =
          listRawImpressionUploadModelLines(
            listRawImpressionUploadModelLinesRequest {
              parent = upload
              this.pageToken = pageToken
            }
          )
        ResourceList(response.rawImpressionUploadModelLinesList, response.nextPageToken)
      }
      .collect { page -> rows.addAll(page) }
    return rows
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
   * Derives the parent `RawImpressionUpload` resource name from a `VidLabelingJob` resource name by
   * stripping the `/vidLabelingJobs/{job}` segment.
   *
   * TODO(world-federation-of-advertisers/cross-media-measurement#4051): Replace this string
   *   stripping with the resource-name parser once #4051 (`VidLabelingJobKey`) and #4013
   *   (`RawImpressionUploadKey`) merge:
   *   `VidLabelingJobKey.fromName(vidLabelingJob)!!.toRawImpressionUploadKey().toName()`. The
   *   parser fails loudly on a malformed or extended resource name instead of silently producing a
   *   wrong parent URI that the next RPC rejects.
   */
  private fun parentUpload(vidLabelingJob: String): String {
    require(vidLabelingJob.contains("/vidLabelingJobs/")) {
      "Malformed VidLabelingJob resource name: $vidLabelingJob"
    }
    return vidLabelingJob.substringBefore("/vidLabelingJobs/")
  }

  /**
   * Derives a deterministic UUID4 from [seed], stable across redeliveries, for use as an AIP-155
   * `request_id`. Computed from an MD5 digest of the seed with the RFC-4122 version (4) and variant
   * bits forced, so it satisfies a field's `format = UUID4`.
   *
   * MD5 here is a non-cryptographic, deterministic idempotency-key derivation (not used for
   * security); the version/variant bits are forced only to satisfy the field format = UUID4.
   *
   * TODO(world-federation-of-advertisers/cross-media-measurement#4081): Replace this local helper
   *   and its call sites with the shared `RequestIds` derivation created in #4081 once it merges
   *   (e.g. `RequestIds.forMarkVidLabelingJobSucceeded(mp.vidLabelingJob)`), so the AIP-155
   *   request_id rule lives in one place instead of being reinvented here.
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

    /** LabelerInput field path whose mapped raw column carries the event id used for the digest. */
    private const val EVENT_ID_FIELD_PATH = "event_id.id"

    /** Nanoseconds per second, for converting [System.nanoTime] deltas to seconds. */
    private const val NANOS_PER_SECOND = 1_000_000_000.0
  }
}
