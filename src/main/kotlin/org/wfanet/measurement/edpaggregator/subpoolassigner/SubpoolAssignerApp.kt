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

package org.wfanet.measurement.edpaggregator.subpoolassigner

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.protobuf.Parser
import java.time.Instant
import org.jetbrains.annotations.VisibleForTesting
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigestExtractor
import org.wfanet.measurement.edpaggregator.rawimpressions.LabelerInputMapper
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionSource
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams.StorageParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.vidRankBuilderParams
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.ParquetStorageClient

/**
 * Phase-0 TEE/queue adapter for the memoized VID assignment pipeline.
 *
 * Following the ResultsFulfiller layering, this class is the thin [BaseTeeApplication] adapter: per
 * WorkItem it unpacks the [SubpoolAssignerParams] payload, resolves the WorkItem-scoped
 * dependencies (storage clients, KMS client, KEK URI, event-id column, active window, the pool-emit
 * labeler, and the [RawImpressionSource]), then constructs a [SubpoolAssigner] and delegates the
 * actual work to it. The process-wide bootstrap (gRPC channels/stubs, KMS map, Pub/Sub) lives in
 * `SubpoolAssignerAppRunner`; the per-WorkItem logic and metadata-storage bookkeeping live in
 * [SubpoolAssigner].
 *
 * The dispatcher publishes one [WorkItem] per fingerprint shard for each (`RawImpressionUpload`,
 * `ModelLine`) pair that has memoization enabled.
 *
 * @param subscriptionId The subscription ID for the queue subscriber.
 * @param queueSubscriber The [QueueSubscriber] instance for receiving work items.
 * @param parser The protobuf [Parser] for [WorkItem] messages.
 * @param workItemsClient gRPC client stub for [WorkItemsGrpcKt.WorkItemsCoroutineStub].
 * @param workItemAttemptsClient gRPC client stub for
 *   [WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub].
 * @param vidRankBuilderQueue Resource name of the Secure Computation queue for Phase-1
 *   (`VidRankBuilder`) work items, published by the last-shard-out fan-out.
 * @param kmsClients Per-DataProvider KMS clients used to wrap/unwrap DEKs for raw-impression and
 *   `SubpoolFingerprints` blobs.
 * @param getSubpoolMapStorageConfig Lambda to obtain the [StorageConfig] for reading and writing
 *   the per-(shard, subpool) `SubpoolFingerprints` blobs.
 * @param getRawImpressionsStorageConfig Lambda to obtain the [StorageConfig] for reading the
 *   raw-impression files uploaded by the EDP.
 * @param getModelStorageConfig Lambda to obtain the [StorageConfig] for reading the compiled VID
 *   model blob (which lives in its own GCS project).
 * @param rawImpressionUploadsStub Stub for the `RawImpressionUpload` metadata-storage service.
 * @param rawImpressionUploadModelLinesStub Stub for the `RawImpressionUploadModelLine`
 *   metadata-storage service.
 * @param rankerJobsStub Stub for the `RankerJob` metadata-storage service.
 * @param poolAssignmentJobsStub Stub to mark this shard's `PoolAssignmentJob` `SUCCEEDED` and learn
 *   whether it is the last shard out.
 * @param rawImpressionUploadFilesStub Stub for discovering this upload's raw-impression files.
 *   Built by the runner from `raw_impression_metadata_storage_connection`.
 * @param buildParquetStorageClient Builds a [ParquetStorageClient] for the raw-impressions storage,
 *   PME-decrypting raw impressions via the per-EDP [KmsClient].
 * @param buildSubpoolMapStorageClient Builds a [StorageClient] for the subpool-map storage.
 * @param loadPoolEmitLabeler Loads the compiled VID model in pool-emit mode from a
 *   `model_blob_path`.
 * @param getSubpoolMapKekUri Resolves the key-encryption-key URI used to wrap the subpool-map DEK.
 * @param eventIdDigestExtractor Computes the 12-byte `EventIdDigest` of an event id.
 */
class SubpoolAssignerApp(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  private val workItemsClient: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub,
  private val vidRankBuilderQueue: String,
  private val kmsClients: Map<String, KmsClient>,
  private val getSubpoolMapStorageConfig: (StorageParams) -> StorageConfig,
  private val getRawImpressionsStorageConfig: (StorageParams) -> StorageConfig,
  private val getModelStorageConfig: (StorageParams) -> StorageConfig,
  private val rawImpressionUploadsStub: RawImpressionUploadServiceCoroutineStub,
  private val rawImpressionUploadModelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val rankerJobsStub: RankerJobServiceCoroutineStub,
  private val poolAssignmentJobsStub: PoolAssignmentJobServiceCoroutineStub,
  private val rawImpressionUploadFilesStub: RawImpressionUploadFileServiceCoroutineStub,
  private val buildParquetStorageClient: (StorageConfig, KmsClient) -> ParquetStorageClient,
  private val buildSubpoolMapStorageClient: (StorageConfig) -> ConditionalOperationStorageClient,
  private val loadPoolEmitLabeler:
    suspend (modelStorageConfig: StorageConfig, modelBlobPath: String) -> PoolEmitLabeler,
  private val getSubpoolMapKekUri: (dataProvider: String) -> String,
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
    val params = workItemParams.appParams.unpack(SubpoolAssignerParams::class.java)

    val dataProvider = params.dataProvider
    require(dataProvider.isNotEmpty()) { "data_provider must not be empty" }
    require(params.hasRawImpressionStorageParams()) { "raw_impression_storage_params must be set" }
    require(params.hasSubpoolMapStorageParams()) { "subpool_map_storage_params must be set" }
    require(params.hasModelStorageParams()) { "model_storage_params must be set" }
    require(params.totalShards > 0) { "total_shards must be > 0" }

    val kmsClient =
      requireNotNull(kmsClients[dataProvider]) { "KMS client not found for $dataProvider" }

    // The raw event-id column is the raw-impression field mapped to LabelerInput's `event_id.id`
    // (same convention as Phase 2's VidLabelerApp).
    val eventIdColumn =
      requireNotNull(params.labelerInputFieldMappingMap[EVENT_ID_FIELD_PATH]) {
        "labeler_input_field_mapping must map '$EVENT_ID_FIELD_PATH' to the raw event-id column"
      }

    val rawImpressionSource =
      RawImpressionSource(
        parquetStorageClient =
          buildParquetStorageClient(
            getRawImpressionsStorageConfig(params.rawImpressionStorageParams),
            kmsClient,
          ),
        rawImpressionUploadFilesStub = rawImpressionUploadFilesStub,
        rawImpressionUpload = params.rawImpressionUpload,
        eventIdColumn = eventIdColumn,
        shardIndex = params.shardIndex,
        totalShards = params.totalShards,
        eventIdDigestExtractor = eventIdDigestExtractor,
      )

    val mapper = LabelerInputMapper(params.labelerInputFieldMappingMap)
    val activeWindow =
      ActiveWindow.of(
        params.activeStartTime.toInstant(),
        if (params.hasActiveEndTime()) params.activeEndTime.toInstant() else null,
      )
    val store =
      SubpoolFingerprintsStore(
        buildSubpoolMapStorageClient(getSubpoolMapStorageConfig(params.subpoolMapStorageParams)),
        kmsClient,
      )

    // Static Phase-1 params, passed through from this shard's SubpoolAssignerParams. The
    // last-shard-out fan-out fills in the per-RankerJob fields (encrypted_subpool_maps_dek,
    // ranker_job, subpool_map_blob_uris, max_event_date) on top of this template.
    val vidRankBuilderParamsTemplate = buildVidRankBuilderParamsTemplate(params)

    loadPoolEmitLabeler(getModelStorageConfig(params.modelStorageParams), params.modelBlobPath)
      .use { labeler ->
        SubpoolAssigner(
            rawImpressionSource = rawImpressionSource,
            mapper = mapper,
            labeler = labeler,
            activeWindow = activeWindow,
            store = store,
            kekUri = getSubpoolMapKekUri(dataProvider),
            blobPrefix = params.subpoolMapStorageParams.blobPrefix,
            poolAssignmentJobsStub = poolAssignmentJobsStub,
            rawImpressionUploadModelLinesStub = rawImpressionUploadModelLinesStub,
            rankerJobsStub = rankerJobsStub,
            rawImpressionUploadsStub = rawImpressionUploadsStub,
            workItemsStub = workItemsClient,
            rawImpressionUpload = params.rawImpressionUpload,
            modelLine = params.modelLine,
            poolAssignmentJob = params.poolAssignmentJob,
            shardIndex = params.shardIndex,
            totalShards = params.totalShards,
            vidRankBuilderQueue = vidRankBuilderQueue,
            vidRankBuilderParamsTemplate = vidRankBuilderParamsTemplate,
          )
          .assign()
      }
  }

  companion object {
    /** LabelerInput field path whose mapped raw column carries the event id used for the digest. */
    private const val EVENT_ID_FIELD_PATH = "event_id.id"

    /**
     * Maps the static [SubpoolAssignerParams] fields to a partial [VidRankBuilderParams] template.
     * The last-shard-out fan-out fills in the per-`RankerJob` fields (encrypted_subpool_maps_dek,
     * ranker_job, subpool_map_blob_uris, max_event_date) on top of this.
     */
    @VisibleForTesting
    fun buildVidRankBuilderParamsTemplate(params: SubpoolAssignerParams): VidRankBuilderParams {
      require(params.hasVidLabeledImpressionsStorageParams()) {
        "vid_labeled_impressions_storage_params must be set"
      }
      require(params.hasVidRankMapStorageParams()) { "vid_rank_map_storage_params must be set" }
      require(params.maxFileBatchSizeBytes > 0) {
        "max_file_batch_size_bytes must be set (> 0) by the dispatcher"
      }
      return vidRankBuilderParams {
        dataProvider = params.dataProvider
        rawImpressionStorageParams =
          params.rawImpressionStorageParams.toVidRankBuilderStorageParams()
        vidLabeledImpressionsStorageParams =
          params.vidLabeledImpressionsStorageParams.toVidRankBuilderStorageParams()
        subpoolMapStorageParams = params.subpoolMapStorageParams.toVidRankBuilderStorageParams()
        vidRankMapStorageParams = params.vidRankMapStorageParams.toVidRankBuilderStorageParams()
        modelStorageParams = params.modelStorageParams.toVidRankBuilderStorageParams()
        rawImpressionUpload = params.rawImpressionUpload
        modelLine = params.modelLine
        modelBlobPath = params.modelBlobPath
        labelerInputFieldMapping.putAll(params.labelerInputFieldMappingMap)
        eventTemplateFieldMapping.putAll(params.eventTemplateFieldMappingMap)
        totalShards = params.totalShards
        // Phase-2 bin-packing cap (REQUIRED), forwarded verbatim so the Phase-1 last-out can batch
        // the upload's files without round-tripping to the dispatcher.
        maxFileBatchSizeBytes = params.maxFileBatchSizeBytes
        // Active-window pass-through (OPTIONAL): forwarded verbatim so the Phase-1 last-out can
        // stamp it on the Phase-2 VidLabeler ModelLineConfig, which drops out-of-window rows.
        if (params.hasActiveStartTime()) {
          activeStartTime = params.activeStartTime
        }
        if (params.hasActiveEndTime()) {
          activeEndTime = params.activeEndTime
        }
      }
    }

    private fun com.google.protobuf.Timestamp.toInstant(): Instant =
      Instant.ofEpochSecond(seconds, nanos.toLong())

    /** Copies the shared `{gcs_project_id, blob_prefix}` shape across the two param protos. */
    private fun StorageParams.toVidRankBuilderStorageParams(): VidRankBuilderParams.StorageParams =
      VidRankBuilderParamsKt.storageParams {
        gcsProjectId = this@toVidRankBuilderStorageParams.gcsProjectId
        blobPrefix = this@toVidRankBuilderStorageParams.blobPrefix
      }
  }
}
