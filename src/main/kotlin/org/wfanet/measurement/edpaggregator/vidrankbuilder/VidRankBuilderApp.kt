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

package org.wfanet.measurement.edpaggregator.vidrankbuilder

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.protobuf.Parser
import java.time.LocalDate
import java.time.ZoneOffset
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.rawimpressions.OutputManifestStore
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.OutputManifestRecord
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams.StorageParams
import org.wfanet.measurement.edpaggregator.v1alpha.getRankerJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Phase-1 TEE/queue adapter for the memoized VID assignment pipeline.
 *
 * Following the ResultsFulfiller / SubpoolAssigner layering, this class is the thin
 * [BaseTeeApplication] adapter: per WorkItem it unpacks the [VidRankBuilderParams] payload,
 * resolves the WorkItem-scoped dependencies (storage clients, KMS client, KEK URI, the
 * [SubpoolFingerprints] and rank-index stores, the retention pass and per-subpool [SubpoolRanker]),
 * then constructs a [VidRankBuilder] and delegates the actual work to it. The process-wide
 * bootstrap (gRPC channels/stubs, KMS map, Pub/Sub) lives in `VidRankBuilderAppRunner`.
 *
 * One ranker VM per `RankerJob`. A `RankerJob` may cover one or more bin-packed subpools; within a
 * subpool there is no intra-subpool sharding. One WorkItem is published per `RankerJob` row.
 *
 * @param kmsClients Per-DataProvider KMS clients used to wrap/unwrap DEKs.
 * @param retentionDaysByDataProvider Per-DataProvider rank-index retention window (in days),
 *   sourced from the shared EDPA config (which also lists non-memoized EDPs that leave it 0);
 *   validated `> 0` per WorkItem in [runWork], not at construction. MUST exceed the EDP's maximum
 *   measurement-report window.
 * @param rankerJobsStub stub to gate on / mark this `RankerJob`.
 * @param rankIndexBlobsStub stub for locating the prior snapshot, retention, and new blob rows.
 * @param rawImpressionUploadModelLinesStub stub to flip the parent `RANKING` -> `LABELING`.
 * @param vidLabelingJobsStub stub for the last-job-out Phase-2 fan-out.
 * @param rawImpressionUploadFilesStub stub to list the upload's files for Phase-2 sharding.
 * @param vidLabelerQueue Secure Computation queue the last-job-out publishes Phase-2 VidLabeler
 *   WorkItems to.
 * @param rpcThrottlers process-scoped rate limiters shared by the TEE lifecycle and Phase-1 RPCs.
 * @param buildSubpoolMapStorageClient Builds the subpool-map [ConditionalOperationStorageClient]
 *   from its [StorageParams] (bucket parsed from the `gs://` blob_prefix).
 * @param buildVidRankMapStorageClient Builds the vid-rank-map [ConditionalOperationStorageClient]
 *   from its [StorageParams] (bucket parsed from the `gs://` blob_prefix).
 * @param today Supplies the UTC date treated as "now" for retention (injectable for tests).
 * @param workerDispatcher Base CPU dispatcher for the parallel forward rank build; [SubpoolRanker]
 *   caps it to [rankStripes] via `limitedParallelism` (mirrors `RawImpressionSource`). Defaults to
 *   [Dispatchers.Default] (already ≈ #cores).
 * @param rankStripes Number of map stripes ≈ #cores for the parallel forward build.
 * @param maxInFlightRecords Parse-phase read-ahead bound (backpressure) for [SubpoolRanker].
 */
class VidRankBuilderApp(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  private val workItemsClient: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub,
  private val kmsClients: Map<String, KmsClient>,
  private val retentionDaysByDataProvider: Map<String, Int>,
  private val rankerJobsStub: RankerJobServiceCoroutineStub,
  private val rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
  private val rawImpressionUploadModelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val vidLabelingJobsStub: VidLabelingJobServiceCoroutineStub,
  private val rawImpressionUploadFilesStub: RawImpressionUploadFileServiceCoroutineStub,
  private val vidLabelerQueue: String,
  private val rpcThrottlers: VidLabelingRpcThrottlers,
  private val buildSubpoolMapStorageClient: (StorageParams) -> ConditionalOperationStorageClient,
  private val buildVidRankMapStorageClient: (StorageParams) -> ConditionalOperationStorageClient,
  private val today: () -> LocalDate = { LocalDate.now(ZoneOffset.UTC) },
  private val workerDispatcher: CoroutineDispatcher = Dispatchers.Default,
  private val rankStripes: Int = ConcurrentRankAllocator.DEFAULT_STRIPES,
  private val maxInFlightRecords: Int = maxOf(2, ConcurrentRankAllocator.DEFAULT_STRIPES * 2),
) :
  BaseTeeApplication(
    subscriptionId = subscriptionId,
    queueSubscriber = queueSubscriber,
    parser = parser,
    workItemsStub = workItemsClient,
    workItemAttemptsStub = workItemAttemptsClient,
    controlPlaneThrottler = rpcThrottlers.controlPlane,
  ) {

  override suspend fun runWork(message: Any) {
    val workItemParams = message.unpack(WorkItemParams::class.java)
    val params = workItemParams.appParams.unpack(VidRankBuilderParams::class.java)

    val dataProvider = params.dataProvider
    require(dataProvider.isNotEmpty()) { "data_provider must not be empty" }
    require(params.hasSubpoolMapStorageParams()) { "subpool_map_storage_params must be set" }
    require(params.hasVidRankMapStorageParams()) { "vid_rank_map_storage_params must be set" }
    require(params.hasEncryptedSubpoolMapsDek()) { "encrypted_subpool_maps_dek must be set" }
    require(params.encryptedSubpoolMapsDek.kekUri.isNotEmpty()) {
      "encrypted_subpool_maps_dek.kek_uri must be set"
    }
    require(params.totalShards > 0) { "total_shards must be > 0" }

    val kmsClient =
      requireNotNull(kmsClients[dataProvider]) { "KMS client not found for $dataProvider" }
    val retentionDays =
      requireNotNull(retentionDaysByDataProvider[dataProvider]) {
        "retention_days not configured for $dataProvider"
      }
    // Validated here, not at runner startup: the shared all-EDP config also lists EDPs that never
    // run the memoized ranker and legitimately leave retention_days unset (proto3 default 0). A
    // RankerJob for this DataProvider means it IS memoized, so a non-positive window is a real
    // misconfig — 0 means evict-everything, and the window is compliance-tied (it MUST exceed the
    // EDP's maximum measurement-report window or memoization's VID-collision guarantee breaks).
    require(retentionDays > 0) {
      "retention_days must be > 0 for $dataProvider (it runs the memoized VID ranker), got " +
        "$retentionDays"
    }

    val subpoolStorage = buildSubpoolMapStorageClient(params.subpoolMapStorageParams)
    val mergedGenerations = selectedMergedGenerations(params, subpoolStorage)
    val subpoolFingerprintsStore = SubpoolFingerprintsStore(subpoolStorage, kmsClient)
    val rankIndexStore =
      RankIndexStore(buildVidRankMapStorageClient(params.vidRankMapStorageParams), kmsClient)

    val runDate = today()
    val retention =
      SubpoolRetention(
        rankIndexBlobsStub = rankIndexBlobsStub,
        rankIndexStore = rankIndexStore,
        dataProvider = dataProvider,
        modelLine = params.modelLine,
        retentionDays = retentionDays,
        today = runDate,
        rpcThrottlers = rpcThrottlers,
      )

    val subpoolRanker =
      SubpoolRanker(
        subpoolFingerprintsStore = subpoolFingerprintsStore,
        rankIndexStore = rankIndexStore,
        rankIndexBlobsStub = rankIndexBlobsStub,
        rawImpressionUploadModelLinesStub = rawImpressionUploadModelLinesStub,
        retention = retention,
        dataProvider = dataProvider,
        rawImpressionUpload = params.rawImpressionUpload,
        modelLine = params.modelLine,
        allowedPriorSnapshotRawImpressionUploads =
          params.allowedPriorSnapshotRawImpressionUploadsList.toSet(),
        vidRankMapBlobPrefix =
          SelectedStorageClient.parseBlobUri(params.vidRankMapStorageParams.blobPrefix).key,
        kekUri = params.encryptedSubpoolMapsDek.kekUri,
        encryptedSubpoolMapsDek = params.encryptedSubpoolMapsDek,
        maxEventDate = params.maxEventDate,
        retentionDays = retentionDays,
        today = runDate,
        rpcThrottlers = rpcThrottlers,
        mergedBlobGenerations = mergedGenerations,
        workerDispatcher = workerDispatcher,
        stripes = rankStripes,
        maxInFlightRecords = maxInFlightRecords,
      )

    VidRankBuilder(
        subpoolRanker = subpoolRanker,
        rankerJobsStub = rankerJobsStub,
        rawImpressionUploadModelLinesStub = rawImpressionUploadModelLinesStub,
        vidLabelingJobsStub = vidLabelingJobsStub,
        rawImpressionUploadFilesStub = rawImpressionUploadFilesStub,
        workItemsStub = workItemsClient,
        rawImpressionUpload = params.rawImpressionUpload,
        modelLine = params.modelLine,
        rankerJob = params.rankerJob,
        subpoolMapBlobUris = params.subpoolMapBlobUrisMap,
        subpoolRankedSizes = params.subpoolRankedSizesMap,
        vidLabelerParamsTemplate = buildVidLabelerParamsTemplate(params),
        vidLabelerQueue = vidLabelerQueue,
        // Forwarded verbatim from Phase-0 via VidRankBuilderParams (REQUIRED); VidRankBuilder's
        // `require(maxFileBatchSizeBytes > 0)` rejects an unset/zero value at the boundary.
        maxFileBatchSizeBytes = params.maxFileBatchSizeBytes,
        rpcThrottlers = rpcThrottlers,
      )
      .run()
  }

  private suspend fun selectedMergedGenerations(
    params: VidRankBuilderParams,
    storage: ConditionalOperationStorageClient,
  ): Map<Long, Long> {
    val job =
      rpcThrottlers.metadataRead.onReady {
        rankerJobsStub.getRankerJob(getRankerJobRequest { name = params.rankerJob })
      }
    if (job.state == RankerJob.State.SUCCEEDED) return emptyMap()
    var token = ""
    var parent: org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine? = null
    do {
      val page =
        rpcThrottlers.metadataRead.onReady {
          rawImpressionUploadModelLinesStub.listRawImpressionUploadModelLines(
            listRawImpressionUploadModelLinesRequest {
              this.parent = params.rawImpressionUpload
              filter =
                ListRawImpressionUploadModelLinesRequestKt.filter {
                  cmmsModelLine = params.modelLine
                }
              pageToken = token
            }
          )
        }
      parent =
        page.rawImpressionUploadModelLinesList.singleOrNull { it.cmmsModelLine == params.modelLine }
          ?: parent
      token = page.nextPageToken
    } while (token.isNotEmpty())
    val selected = parent ?: return emptyMap()
    if (!selected.hasMergedOutputManifest()) return emptyMap()
    val store = OutputManifestStore(storage, params.subpoolMapStorageParams.blobPrefix)
    val records = store.read(selected.mergedOutputManifest).toList()
    val header = records.first().header
    check(
      header.phase == OutputManifestRecord.Header.Phase.POOL_MERGE &&
        header.owner == selected.name &&
        header.rawImpressionUpload == params.rawImpressionUpload &&
        header.modelLinesList == listOf(params.modelLine) &&
        header.encryptedDek == params.encryptedSubpoolMapsDek
    ) {
      "Ranker payload disagrees with selected merge"
    }
    val refs =
      records.drop(1).associate {
        check(it.hasSubpoolBlob()) { "Unexpected merged output record" }
        it.subpoolBlob.poolOffset to it.subpoolBlob.blob
      }
    check(refs.size == records.size - 1 && refs.keys == selected.poolOffsetsList.toSet())
    check(params.subpoolMapBlobUrisMap.keys == job.poolOffsetsList.toSet())
    return params.subpoolMapBlobUrisMap.mapValues { (offset, uri) ->
      val ref = checkNotNull(refs[offset]) { "Ranker input is absent from selected merge" }
      check(ref.blobUri == uri) { "Ranker URI disagrees with selected merge" }
      store.readBlob(ref)
      ref.blobGeneration
    }
  }

  /**
   * Builds the memoized [VidLabelerParams] template for the Phase-2 fan-out from the pass-through
   * fields the SubpoolAssigner stamped on [params]. The last-`RankerJob`-out copies it per
   * `VidLabelingJob`, filling the job's name and its files; the Phase-2 TEE resolves the rank-index
   * blobs from the `RankIndexBlobService` rather than receiving their URIs here.
   */
  private fun buildVidLabelerParamsTemplate(params: VidRankBuilderParams): VidLabelerParams =
    org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingPhaseDispatcher
      .templateFromRankBuilder(params)
}
