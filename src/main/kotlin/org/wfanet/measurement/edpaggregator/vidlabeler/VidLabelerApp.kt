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
import java.time.LocalDate
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigestExtractor
import org.wfanet.measurement.edpaggregator.rawimpressions.LabelerInputMapper
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionFileMetadata
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionSource
import org.wfanet.measurement.edpaggregator.service.VidLabelingJobKey
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getVidLabelingJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineCompletedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markVidLabelingJobSucceededRequest
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
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
 * @param encryptKekUris per-`DataProvider` KEK URI used to wrap the labeled output on the
 *   non-memoized path; the memoized path instead reuses the KEK carried on the rank-index blobs.
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
 *   given the model line and its [VidLabelerParams.ModelLineConfig]; the production factory loads
 *   the EventTemplate descriptor from the config blob. Entity keys are read per file from the
 *   Parquet footer at labeling time, not passed here.
 * @param eventIdDigestExtractor computes the 12-byte `EventIdDigest` of an event id.
 */
class VidLabelerApp(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  workItemsClient: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub,
  private val kmsClients: Map<String, KmsClient>,
  private val encryptKekUris: Map<String, String>,
  private val getStorageConfig: (VidLabelerParams.StorageParams) -> StorageConfig,
  private val vidLabelingJobsStub: VidLabelingJobServiceCoroutineStub,
  private val rawImpressionUploadModelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
  private val rawImpressionUploadFilesStub: RawImpressionUploadFileServiceCoroutineStub,
  private val buildParquetStorageClient: (StorageConfig, KmsClient) -> ParquetStorageClient,
  private val buildVidRankMapStorageClient: (StorageConfig) -> ConditionalOperationStorageClient,
  private val loadAssigner:
    suspend (modelStorageConfig: StorageConfig, modelBlobUri: String) -> VidAssigner,
  private val buildImpressionConverter:
    suspend (modelLine: String, config: VidLabelerParams.ModelLineConfig) -> ImpressionConverter,
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

    // Two Phase-2 configurations share this worker and are mutually exclusive: the memoized
    // rank-index path (memoized_params set) and the non-memoized hash-only path (top-level
    // vid_labeling_job set, bundling all of an upload's non-memoized model lines).
    if (params.hasMemoizedParams()) {
      runMemoized(params, dataProvider)
    } else {
      runNonMemoized(params, dataProvider)
    }
  }

  /** Memoized rank-index Phase-2 path: derive each VID from its memoized rank. */
  private suspend fun runMemoized(params: VidLabelerParams, dataProvider: String) {
    // MemoizedParams now carries only the rank-index storage; the job + model line are top-level
    // (shared with the non-memoized path). The memoized path always has exactly one model line.
    val mp = params.memoizedParams
    val modelLine = params.modelLinesList.single()
    val vidLabelingJob = params.vidLabelingJob

    // Per-WorkItem metric dimensions: every counter/histogram for this call is keyed by the
    // DataProvider and the WorkItem's model line.
    val attributes =
      Attributes.of(metrics.DATA_PROVIDER_ATTR, dataProvider, metrics.MODEL_LINE_ATTR, modelLine)
    val startNanos = System.nanoTime()
    try {
      // One client per EDP (the EDP's own KEK), used for BOTH the raw-impression PME decrypt
      // and the labeled-output encrypt — mirrors SubpoolAssigner's single-client model.
      val kmsClient =
        requireNotNull(kmsClients[dataProvider]) { "KMS client not found for $dataProvider" }

      val config =
        requireNotNull(params.modelLineConfigsMap[modelLine]) {
          "model_line_configs must contain an entry for $modelLine"
        }

      // Gate on job state first: on Pub/Sub redelivery of an already-completed job, skip relabeling
      // (the output is idempotent via deterministic blob keys) but still run the idempotent mark +
      // last-job-out recovery so a crash between label() and the mark cannot drop the completion.
      val job =
        vidLabelingJobsStub.getVidLabelingJob(getVidLabelingJobRequest { name = vidLabelingJob })
      val observedEventDates: Set<LocalDate> =
        if (job.state != VidLabelingJob.State.SUCCEEDED) {
          labelMemoized(
            params,
            mp,
            modelLine,
            config,
            dataProvider,
            kmsClient,
            job.rawImpressionUploadFilesList,
          )
        } else {
          logger.info("VidLabelingJob $vidLabelingJob already SUCCEEDED; skipping relabel")
          emptySet()
        }

      markSucceededAndTransition(
        params,
        vidLabelingJob,
        dataProvider,
        job.etag,
        attributes,
        observedEventDates,
        job.rawImpressionUploadFilesList,
        kmsClient,
      )
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

  /**
   * Non-memoized hash-only Phase-2 path: label the bundled model lines (no rank index) against the
   * job's bin-packed files, loading each model from its blob (the TEE never reads the VID Repo) and
   * wrapping the output with this EDP's [encryptKekUris] entry.
   */
  private suspend fun runNonMemoized(params: VidLabelerParams, dataProvider: String) {
    require(params.vidLabelingJob.isNotEmpty()) {
      "vid_labeling_job must be set on the non-memoized path"
    }
    require(params.modelLinesList.isNotEmpty()) {
      "model_lines must list the bundled non-memoized model lines"
    }
    val attributes = Attributes.of(metrics.DATA_PROVIDER_ATTR, dataProvider)
    val startNanos = System.nanoTime()
    try {
      val kmsClient =
        requireNotNull(kmsClients[dataProvider]) { "KMS client not found for $dataProvider" }
      val encryptKekUri =
        requireNotNull(encryptKekUris[dataProvider]) {
          "encrypt KEK URI not found for $dataProvider"
        }

      val job =
        vidLabelingJobsStub.getVidLabelingJob(
          getVidLabelingJobRequest { name = params.vidLabelingJob }
        )
      val observedEventDates: Set<LocalDate> =
        if (job.state != VidLabelingJob.State.SUCCEEDED) {
          labelNonMemoized(
            params,
            dataProvider,
            kmsClient,
            encryptKekUri,
            job.rawImpressionUploadFilesList,
          )
        } else {
          logger.info("VidLabelingJob ${params.vidLabelingJob} already SUCCEEDED; skipping relabel")
          emptySet()
        }

      markSucceededAndTransition(
        params,
        params.vidLabelingJob,
        dataProvider,
        job.etag,
        attributes,
        observedEventDates,
        job.rawImpressionUploadFilesList,
        kmsClient,
      )
      metrics.workItemsProcessedCounter.add(1, attributes)
    } finally {
      metrics.workItemDurationHistogram.record(
        (System.nanoTime() - startNanos) / NANOS_PER_SECOND,
        attributes,
      )
    }
  }

  /** Labels this WorkItem's raw-impression files for [mp]'s single model line (memoized path). */
  private suspend fun labelMemoized(
    params: VidLabelerParams,
    mp: VidLabelerParams.MemoizedParams,
    modelLine: String,
    config: VidLabelerParams.ModelLineConfig,
    dataProvider: String,
    kmsClient: KmsClient,
    inputFiles: List<String>,
  ): Set<LocalDate> {
    val rankIndexStore =
      RankIndexStore(
        buildVidRankMapStorageClient(getStorageConfig(mp.vidRankMapStorageParams)),
        kmsClient,
      )
    val rankIndex =
      MemoizedRankIndex.load(rankIndexBlobsStub, rankIndexStore, dataProvider, modelLine)

    // The raw event-id column is the raw-impression field mapped to LabelerInput's `event_id.id`.
    val eventIdColumn = resolveEventIdColumn(config)

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
        rawImpressionUpload = parentUpload(params.vidLabelingJob),
        eventIdColumn = eventIdColumn,
        eventIdDigestExtractor = eventIdDigestExtractor,
        inputFiles = inputFiles,
      )

    // Schema-drift guard (#3993): fail fast if a mapped raw column is missing from the file schema
    // (e.g. renamed upstream) instead of silently unsetting the field on every labeled impression.
    rawImpressionSource.validateSchema(
      LabelerInputMapper(config.labelerInputFieldMappingList).referencedColumnKinds()
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
    require(params.hasModelStorageParams()) { "model_storage_params must be set" }
    val modelLineSpec =
      ModelLineSpec(
        modelLine = modelLine,
        modelBlobUri =
          requireNotNull(params.modelBlobPathsMap[modelLine]) {
            "model_blob_paths must contain an entry for $modelLine"
          },
        modelStorageConfig = getStorageConfig(params.modelStorageParams),
        activeWindow = activeWindow,
        config = config,
        rankIndex = rankIndex,
      )

    val impressionConverter = buildImpressionConverter(modelLine, config)

    return VidLabeler(
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
   * Labels the bundled non-memoized model lines ([VidLabelerParams.model_lines]) against
   * [inputFiles] with the hash-only path (no rank index). One [VidLabeler] run covers every bundled
   * line; the shared [impressionConverter] and raw event-id column are taken from the first line's
   * config, since an EDP's model lines share its raw event schema and EventTemplate.
   */
  private suspend fun labelNonMemoized(
    params: VidLabelerParams,
    dataProvider: String,
    kmsClient: KmsClient,
    encryptKekUri: String,
    inputFiles: List<String>,
  ): Set<LocalDate> {
    val modelLines = params.modelLinesList
    val configsByModelLine =
      modelLines.associateWith { modelLine ->
        requireNotNull(params.modelLineConfigsMap[modelLine]) {
          "model_line_configs must contain an entry for $modelLine"
        }
      }

    // The raw event-id column is a property of the EDP's raw data, shared across its model lines;
    // read it from the first bundled line's config.
    val firstModelLine = modelLines.first()
    val firstConfig = configsByModelLine.getValue(firstModelLine)
    val eventIdColumn = resolveEventIdColumn(firstConfig)

    val rawImpressionSource =
      RawImpressionSource(
        parquetStorageClient =
          buildParquetStorageClient(
            getStorageConfig(params.rawImpressionsStorageParams),
            kmsClient,
          ),
        rawImpressionUploadFilesStub = rawImpressionUploadFilesStub,
        rawImpressionUpload = parentUpload(params.vidLabelingJob),
        eventIdColumn = eventIdColumn,
        eventIdDigestExtractor = eventIdDigestExtractor,
        inputFiles = inputFiles,
      )

    require(params.hasModelStorageParams()) { "model_storage_params must be set" }

    // Schema-drift guard (#3993): every column any bundled model line reads must exist in the file
    // schema; fail fast at the first file rather than silently unsetting fields for every row.
    val mergedColumnKinds =
      LinkedHashMap<String, Set<org.wfanet.measurement.storage.ParquetValue.KindCase>>()
    for (modelLineConfig in configsByModelLine.values) {
      for ((column, kinds) in
        LabelerInputMapper(modelLineConfig.labelerInputFieldMappingList).referencedColumnKinds()) {
        mergedColumnKinds[column] = mergedColumnKinds[column]?.let { it intersect kinds } ?: kinds
      }
    }
    rawImpressionSource.validateSchema(mergedColumnKinds)

    val modelLineSpecs =
      modelLines.map { modelLine ->
        val config = configsByModelLine.getValue(modelLine)
        ModelLineSpec(
          modelLine = modelLine,
          modelBlobUri =
            requireNotNull(params.modelBlobPathsMap[modelLine]) {
              "model_blob_paths must contain an entry for $modelLine"
            },
          // The model blob URI is absolute, so this StorageConfig only supplies the billing/auth
          // project for the read (the VM SA reads the model bucket); it comes from
          // model_storage_params, the same field the memoized path uses.
          modelStorageConfig = getStorageConfig(params.modelStorageParams),
          activeWindow =
            ActiveWindow.of(
              config.activeStartTime.toInstant(),
              if (config.hasActiveEndTime()) config.activeEndTime.toInstant() else null,
            ),
          config = config,
          // Non-memoized: no rank index, so every fingerprint takes the hash path.
          rankIndex = null,
        )
      }

    val impressionConverter = buildImpressionConverter(firstModelLine, firstConfig)

    return VidLabeler(
        rawImpressionSource = rawImpressionSource,
        modelLineSpecs = modelLineSpecs,
        // The operator-header override filters the labeled set at the engine; empty = label all of
        // model_lines.
        overrideModelLines = params.overrideModelLinesList,
        vidModelLoader = vidModelLoader,
        impressionConverter = impressionConverter,
        encryptKmsClient = kmsClient,
        encryptKekUri = encryptKekUri,
        outputStorageParams = params.vidLabeledImpressionsStorageParams,
        storageConfig = getStorageConfig(params.vidLabeledImpressionsStorageParams),
        dataProvider = dataProvider,
      )
      .label()
  }

  /**
   * Marks this WorkItem's `VidLabelingJob` `SUCCEEDED` and, when the service reports this call
   * completed one or more model lines (last-job-out), drops that model line's single `done` marker
   * blob and then transitions its parent `RawImpressionUploadModelLine` to `COMPLETED`. The service
   * returns a model line in `completedModelLines` only to the caller whose mark finished its last
   * outstanding `VidLabelingJob`, so exactly one TEE finalizes each model line — a sibling TEE that
   * finishes the same model line earlier (while others still label it) gets nothing back for it and
   * writes no marker. Idempotent on Pub/Sub redelivery: the mark is keyed by a deterministic
   * `request_id`, the transition swallows the benign already-advanced races, and the done blob has
   * a deterministic key.
   *
   * The mark and the parent-line transitions are two separate RPCs, not a single atomic
   * `MarkLabelingJobSucceeded` that also flips the parent (as an earlier design draft described).
   * This follows the job-API convention where the caller drives the parent `COMPLETED` transition
   * (matching #4051/#4052/#4044). The `done` marker is written before the `COMPLETED` transition so
   * `COMPLETED` is the last, truth-bearing signal: a crash or persistent done-blob failure leaves
   * the parent in `LABELING` — retried on redelivery and reconciled by the Monitor's
   * stuck-`LABELING` recovery — instead of stranding a `COMPLETED`-but-unavailable upload.
   */
  private suspend fun markSucceededAndTransition(
    params: VidLabelerParams,
    vidLabelingJob: String,
    dataProvider: String,
    etag: String,
    attributes: Attributes,
    observedEventDates: Set<LocalDate>,
    inputFiles: List<String>,
    kmsClient: KmsClient,
  ) {
    val response =
      try {
        vidLabelingJobsStub.markVidLabelingJobSucceeded(
          markVidLabelingJobSucceededRequest {
            name = vidLabelingJob
            // [etag] was captured from GetVidLabelingJob before label(); together with the
            // deterministic request_id below it covers Pub/Sub *redelivery* under the single-writer
            // assumption (only this worker mutates the job), NOT a concurrent first-delivery
            // mutation by another writer.
            this.etag = etag
            // AIP-155 retry-idempotency key: a Pub/Sub redelivery reuses the same request_id so the
            // server returns the cached result instead of hitting the etag-mismatch path.
            requestId = RequestIds.forMarkVidLabelingJobSucceeded(vidLabelingJob)
          }
        )
      } catch (e: StatusException) {
        metrics.markSucceededFailuresCounter.add(1, attributes)
        throw e
      }

    if (!response.hasLastVidLabelingJobResult()) return

    val upload = parentUpload(vidLabelingJob)
    // One unfiltered List of every model line under the upload (a superset of the lines this call
    // completed) instead of one filtered List per completed model line. Keyed by model line so each
    // completed model line resolves to its parent row (name + etag) for the COMPLETED transition.
    val parentsByModelLine = listAllModelLines(upload).associateBy { it.cmmsModelLine }
    val completedModelLines = response.lastVidLabelingJobResult.completedModelLinesList
    // The date folder each completed model line's done marker goes in. Reuse the event dates the
    // labeler already read from each file's footer while streaming (no extra I/O); on the
    // skip-relabel recovery path no labeling ran this delivery, so fall back to reading the
    // footers.
    val eventDates = observedEventDates.ifEmpty { readEventDates(params, kmsClient, inputFiles) }
    if (completedModelLines.isNotEmpty()) {
      if (eventDates.isEmpty()) {
        logger.warning(
          "VidLabelingJob $vidLabelingJob reported completed model line(s) but carried no input " +
            "files; cannot resolve the footer event date, so no done marker is written"
        )
      } else {
        // TODO(world-federation-of-advertisers/cross-media-measurement#4145): future improvement —
        //   this only validates THIS (last-out) WorkItem's files. An upload whose files span
        //   several dates across separate single-date WorkItems still leaves the other WorkItems'
        //   date folders without a done marker; finalize done per (model line, date) upload-wide.
        check(eventDates.size == 1) {
          "VidLabelingJob $vidLabelingJob spans multiple event dates ${eventDates.sorted()}; the " +
            "done marker is written per (model line, date) and this path assumes one date per upload"
        }
      }
    }
    // Single shared event date for this WorkItem (null only when it carried no files).
    val eventDate = eventDates.singleOrNull()
    for (completedModelLine in completedModelLines) {
      val parent = parentsByModelLine[completedModelLine]
      if (parent == null) {
        logger.warning(
          "RawImpressionUploadModelLine not found for $completedModelLine under $upload; " +
            "cannot mark COMPLETED"
        )
        continue
      }
      // Write the `done` marker BEFORE the COMPLETED transition so COMPLETED is the last,
      // truth-bearing signal. A persistent writeDoneBlob failure then leaves the model line in
      // LABELING (recoverable) instead of stranding a COMPLETED-but-unavailable upload: on Pub/Sub
      // redelivery the idempotent markVidLabelingJobSucceeded replay re-reports this completed
      // model
      // line (recomputed from sibling job states), so writeDoneBlob is retried; only once it
      // succeeds does markParentCompleted commit COMPLETED. Only this TEE reached last-job-out for
      // `completedModelLine`, so only it finalizes the (model line, date): it drops the single
      // `done` marker in that model line's shared-event-date folder — the one VidLabelingSink wrote
      // its labeled output to — and DataAvailabilitySync finalizes it. Independent per model line:
      // a
      // FAILED/stuck sibling no longer withholds this line's availability.
      if (eventDate != null) {
        writeDoneBlob(
          params.vidLabeledImpressionsStorageParams,
          completedModelLine,
          eventDate,
          dataProvider,
        )
      }
      markParentCompleted(parent, dataProvider)
    }
  }

  /**
   * Transitions [parent] to `COMPLETED`, passing its etag for AIP-154 optimistic locking. Swallows
   * only the benign already-advanced races (FAILED_PRECONDITION / ABORTED) so a redelivered
   * last-job-out — or a concurrent worker that already advanced the line — is a no-op, and rethrows
   * everything else so a transient failure nacks the message. The etag CAS (not a client-side state
   * pre-check) is the source of truth, so a stale [parent] snapshot is safe.
   */
  private suspend fun markParentCompleted(
    parent: RawImpressionUploadModelLine,
    dataProvider: String,
  ) {
    try {
      rawImpressionUploadModelLinesStub.markRawImpressionUploadModelLineCompleted(
        markRawImpressionUploadModelLineCompletedRequest {
          name = parent.name
          etag = parent.etag
          requestId = RequestIds.forMarkRawImpressionUploadModelLineCompleted(parent.name)
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
   * Lists every [RawImpressionUploadModelLine] under [upload] (no `cmms_model_line` filter). Used
   * to resolve the parent rows of the model lines this call completed.
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

  /**
   * Reads the `event_date` footer of every file in [inputFiles] into a set (empty when [inputFiles]
   * is empty). The date lives only in each file's plaintext Parquet footer ("Option Y").
   *
   * Used only on the skip-relabel recovery path, where no labeling ran this delivery to collect the
   * dates from the sinks; the happy path reuses the dates [VidLabeler.label] already read.
   *
   * TODO(world-federation-of-advertisers/cross-media-measurement#4130): Cover the done-marker write
   *   end-to-end in [VidLabelerAppTest] once `ParquetStorageClient` can WRITE footer key-value
   *   metadata (it can only read it today), so a test can synthesize a raw file whose footer
   *   carries `event_date` and drive this read + the resulting `writeDoneBlob`.
   */
  private suspend fun readEventDates(
    params: VidLabelerParams,
    kmsClient: KmsClient,
    inputFiles: List<String>,
  ): Set<LocalDate> {
    if (inputFiles.isEmpty()) return emptySet()
    val parquetStorageClient =
      buildParquetStorageClient(getStorageConfig(params.rawImpressionsStorageParams), kmsClient)
    return inputFiles
      .map { inputFile ->
        val blobUri =
          rawImpressionUploadFilesStub
            .getRawImpressionUploadFile(getRawImpressionUploadFileRequest { name = inputFile })
            .blobUri
        val parquetBlob =
          parquetStorageClient.getBlob(blobUri) ?: error("Raw-impression blob not found: $blobUri")
        val eventDateString =
          requireNotNull(
            parquetBlob.readKeyValueMetadata()[RawImpressionFileMetadata.EVENT_DATE_KEY]?.takeIf {
              it.isNotEmpty()
            }
          ) {
            "raw-impression footer is missing the '${RawImpressionFileMetadata.EVENT_DATE_KEY}' metadata " +
              "entry for $blobUri; the producer must write each file's event date (ISO YYYY-MM-DD) " +
              "into its plaintext footer"
          }
        LocalDate.parse(eventDateString)
      }
      .toSet()
  }

  /**
   * Writes the single empty `done` marker for [cmmsModelLine] at
   * `<prefix>/model-line/<modelLineId>/<eventDate>/done` — the folder [VidLabelingSink] wrote this
   * model line's labeled output to — so `DataAvailabilitySync` finalizes that (model line, date).
   *
   * Written unconditionally (a full-object replace): re-dropping an existing marker on reprocessing
   * re-triggers `DataAvailabilitySync` for that date, the intended behavior when data is relabeled.
   */
  private suspend fun writeDoneBlob(
    outputStorageParams: VidLabelerParams.StorageParams,
    cmmsModelLine: String,
    eventDate: LocalDate,
    dataProvider: String,
  ) {
    val modelLineId =
      requireNotNull(ModelLineKey.fromName(cmmsModelLine)) {
          "completed model line is not a valid ModelLine resource name: $cmmsModelLine"
        }
        .modelLineId
    val storageConfig = getStorageConfig(outputStorageParams)
    val doneUri =
      "${outputStorageParams.impressionsBlobPrefix}/model-line/$modelLineId/$eventDate/done"
    val doneBlobUri = SelectedStorageClient.parseBlobUri(doneUri)
    SelectedStorageClient(doneBlobUri, storageConfig.rootDirectory, storageConfig.projectId)
      .writeBlob(doneBlobUri.key, ByteString.EMPTY)
    metrics.doneBlobsWrittenCounter.add(1, Attributes.of(metrics.DATA_PROVIDER_ATTR, dataProvider))
    logger.info("Wrote done marker $doneUri")
  }

  /**
   * Derives the parent `RawImpressionUpload` resource name from a `VidLabelingJob` resource name.
   *
   * Uses [VidLabelingJobKey] so a malformed or extended resource name fails loudly here instead of
   * silently producing a wrong parent URI that the next RPC rejects.
   */
  private fun parentUpload(vidLabelingJob: String): String {
    val key =
      requireNotNull(VidLabelingJobKey.fromName(vidLabelingJob)) {
        "Malformed VidLabelingJob resource name: $vidLabelingJob"
      }
    return key.parentKey.toName()
  }

  companion object {
    private val logger = Logger.getLogger(VidLabelerApp::class.java.name)

    /** LabelerInput field path whose mapped raw column carries the event id used for the digest. */
    private const val EVENT_ID_FIELD_PATH = "event_id.id"

    /**
     * The raw event-id column: the scalar column mapped to [EVENT_ID_FIELD_PATH]. The digest path
     * needs a single column to hash, so this entry must be a plain scalar source.
     */
    private fun resolveEventIdColumn(config: VidLabelerParams.ModelLineConfig): String {
      val mapping =
        requireNotNull(
          config.labelerInputFieldMappingList.find { it.fieldPath == EVENT_ID_FIELD_PATH }
        ) {
          "labeler_input_field_mapping must map '$EVENT_ID_FIELD_PATH' to the raw event-id column"
        }
      require(mapping.sourceCase == LabelerInputFieldMapping.SourceCase.SCALAR) {
        "labeler_input_field_mapping '$EVENT_ID_FIELD_PATH' must be a scalar column for the digest, " +
          "got ${mapping.sourceCase}"
      }
      return mapping.scalar.column
    }

    /** Nanoseconds per second, for converting [System.nanoTime] deltas to seconds. */
    private const val NANOS_PER_SECOND = 1_000_000_000.0
  }
}
