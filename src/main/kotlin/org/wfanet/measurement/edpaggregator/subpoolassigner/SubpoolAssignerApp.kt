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
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigestExtractor
import org.wfanet.measurement.edpaggregator.rawimpressions.LabelerInputMapper
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionSource
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
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
 * @param rawImpressionUploadsStub Stub for the `RawImpressionUpload` metadata-storage service.
 * @param rawImpressionUploadModelLinesStub Stub for the `RawImpressionUploadModelLine`
 *   metadata-storage service.
 * @param rankerJobsStub Stub for the `RankerJob` metadata-storage service.
 * @param poolAssignmentJobsStub Stub to mark this shard's `PoolAssignmentJob` `SUCCEEDED` and learn
 *   whether it is the last shard out.
 * @param rawImpressionUploadFilesStub Stub for discovering this upload's raw-impression files.
 *   Built by the runner from `raw_impression_metadata_storage_connection`.
 * @param buildParquetStorageClient Builds a [ParquetStorageClient] for the raw-impressions storage.
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
  private val rawImpressionUploadsStub: RawImpressionUploadServiceCoroutineStub,
  private val rawImpressionUploadModelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val rankerJobsStub: RankerJobServiceCoroutineStub,
  private val poolAssignmentJobsStub: PoolAssignmentJobServiceCoroutineStub,
  // The following collaborators are defaulted so the runner (which supplies the production
  // wiring)
  // can be filled in separately; the defaults throw if invoked, but keep the app constructible.
  private val rawImpressionUploadFilesStub: RawImpressionUploadFileServiceCoroutineStub? = null,
  private val buildParquetStorageClient: (StorageConfig) -> ParquetStorageClient = {
    TODO("Wire ParquetStorageClient construction from StorageConfig in SubpoolAssignerAppRunner")
  },
  private val buildSubpoolMapStorageClient: (StorageConfig) -> ConditionalOperationStorageClient = {
    TODO(
      "Wire subpool-map StorageClient construction from StorageConfig in SubpoolAssignerAppRunner"
    )
  },
  private val loadPoolEmitLabeler: (modelBlobPath: String) -> PoolEmitLabeler = {
    TODO("Load the compiled VID model in pool-emit mode (C++/JNI) in SubpoolAssignerAppRunner")
  },
  private val getSubpoolMapKekUri: (dataProvider: String) -> String = {
    TODO("Resolve the subpool-map KEK URI for the DataProvider in SubpoolAssignerAppRunner")
  },
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
    require(params.totalShards > 0) { "total_shards must be > 0" }

    val kmsClient =
      requireNotNull(kmsClients[dataProvider]) { "KMS client not found for $dataProvider" }
    val filesStub =
      requireNotNull(rawImpressionUploadFilesStub) {
        "RawImpressionUploadFileService stub not configured"
      }

    // The raw event-id column is the raw-impression field mapped to LabelerInput's `event_id.id`.
    // The memoized digest path needs a single column to hash, so this entry must be a plain scalar
    // column (not an enum/age/composite source).
    // TODO(@Marco-Premier): confirm this is the canonical source for the digest column, vs. a
    //   dedicated config knob.
    val eventIdMapping =
      requireNotNull(
        params.labelerInputFieldMappingList.find { it.fieldPath == EVENT_ID_FIELD_PATH }
      ) {
        "labeler_input_field_mapping must map '$EVENT_ID_FIELD_PATH' to the raw event-id column"
      }
    require(eventIdMapping.sourceCase == LabelerInputFieldMapping.SourceCase.SCALAR) {
      "labeler_input_field_mapping '$EVENT_ID_FIELD_PATH' must be a scalar column for the memoized " +
        "digest, got ${eventIdMapping.sourceCase}"
    }
    val eventIdColumn = eventIdMapping.scalar.column

    val rawImpressionSource =
      RawImpressionSource(
        parquetStorageClient =
          buildParquetStorageClient(
            getRawImpressionsStorageConfig(params.rawImpressionStorageParams)
          ),
        rawImpressionUploadFilesStub = filesStub,
        rawImpressionUpload = params.rawImpressionUpload,
        eventIdColumn = eventIdColumn,
        shardIndex = params.shardIndex,
        totalShards = params.totalShards,
        eventIdDigestExtractor = eventIdDigestExtractor,
      )

    val mapper = LabelerInputMapper(params.labelerInputFieldMappingList)
    // Schema-drift guard: fail fast if a mapped raw column is missing from the upload's parquet
    // schema (e.g. renamed upstream) instead of silently unsetting the field on every impression.
    rawImpressionSource.validateSchema(mapper.referencedColumnKinds())
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

    loadPoolEmitLabeler(params.modelBlobPath).use { labeler ->
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

  /**
   * Maps the static `SubpoolAssignerParams` fields to a partial [VidRankBuilderParams] template.
   */
  private fun buildVidRankBuilderParamsTemplate(
    params: SubpoolAssignerParams
  ): VidRankBuilderParams {
    require(params.hasVidLabeledImpressionsStorageParams()) {
      "vid_labeled_impressions_storage_params must be set"
    }
    require(params.hasVidRankMapStorageParams()) { "vid_rank_map_storage_params must be set" }
    return vidRankBuilderParams {
      dataProvider = params.dataProvider
      rawImpressionStorageParams = params.rawImpressionStorageParams.toVidRankBuilderStorageParams()
      vidLabeledImpressionsStorageParams =
        params.vidLabeledImpressionsStorageParams.toVidRankBuilderStorageParams()
      subpoolMapStorageParams = params.subpoolMapStorageParams.toVidRankBuilderStorageParams()
      vidRankMapStorageParams = params.vidRankMapStorageParams.toVidRankBuilderStorageParams()
      rawImpressionUpload = params.rawImpressionUpload
      modelLine = params.modelLine
      modelBlobPath = params.modelBlobPath
      labelerInputFieldMapping.addAll(params.labelerInputFieldMappingList)
      eventTemplateFieldMapping.putAll(params.eventTemplateFieldMappingMap)
      totalShards = params.totalShards
    }
  }

  companion object {
    /** LabelerInput field path whose mapped raw column carries the event id used for the digest. */
    private const val EVENT_ID_FIELD_PATH = "event_id.id"

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
