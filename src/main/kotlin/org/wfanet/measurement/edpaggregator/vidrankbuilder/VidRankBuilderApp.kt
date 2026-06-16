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
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams.StorageParams
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.storage.StorageClient

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
 * The not-yet-wired collaborators ([buildSubpoolMapStorageClient], [buildVidRankMapStorageClient],
 * [getVidRankMapKekUri]) are defaulted to throwing TODOs so the app stays constructible while the
 * runner is filled in separately (mirrors `SubpoolAssignerApp`).
 *
 * @param kmsClients Per-DataProvider KMS clients used to wrap/unwrap DEKs.
 * @param getSubpoolMapStorageConfig [StorageConfig] for reading the Phase-0 `SubpoolFingerprints`
 *   blobs.
 * @param getVidRankMapStorageConfig [StorageConfig] for reading prior cumulative rank-index blobs
 *   and writing the new day-only + cumulative blobs.
 * @param rankerJobsStub stub to gate on / mark this `RankerJob`.
 * @param rankIndexBlobsStub stub for locating the prior snapshot, retention, and new blob rows.
 * @param rawImpressionUploadModelLinesStub stub to flip the parent `RANKING` -> `LABELING`.
 * @param vidLabelingJobsStub stub for the last-job-out Phase-2 fan-out.
 * @param rawImpressionUploadFilesStub stub to list the upload's files for Phase-2 sharding.
 * @param vidLabelerQueue Secure Computation queue for Phase-2 (currently unused; see
 *   [VidRankBuilder] fan-out TODO).
 * @param buildSubpoolMapStorageClient Builds the subpool-map [StorageClient].
 * @param buildVidRankMapStorageClient Builds the vid-rank-map [StorageClient].
 * @param getVidRankMapKekUri Resolves the KEK URI used to wrap each rank-index blob's DEK.
 * @param today Supplies the UTC date treated as "now" for retention (injectable for tests).
 */
class VidRankBuilderApp(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  private val workItemsClient: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub,
  private val kmsClients: Map<String, KmsClient>,
  private val getSubpoolMapStorageConfig: (StorageParams) -> StorageConfig,
  private val getVidRankMapStorageConfig: (StorageParams) -> StorageConfig,
  private val rankerJobsStub: RankerJobServiceCoroutineStub,
  private val rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
  private val rawImpressionUploadModelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val vidLabelingJobsStub: VidLabelingJobServiceCoroutineStub,
  private val rawImpressionUploadFilesStub: RawImpressionUploadFileServiceCoroutineStub,
  private val vidLabelerQueue: String,
  private val buildSubpoolMapStorageClient: (StorageConfig) -> StorageClient = {
    TODO(
      "Wire subpool-map StorageClient construction from StorageConfig in VidRankBuilderAppRunner"
    )
  },
  private val buildVidRankMapStorageClient: (StorageConfig) -> StorageClient = {
    TODO(
      "Wire vid-rank-map StorageClient construction from StorageConfig in VidRankBuilderAppRunner"
    )
  },
  private val getVidRankMapKekUri: (dataProvider: String) -> String = {
    TODO("Resolve the vid-rank-map KEK URI for the DataProvider in VidRankBuilderAppRunner")
  },
  private val today: () -> LocalDate = { LocalDate.now(ZoneOffset.UTC) },
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
    val params = workItemParams.appParams.unpack(VidRankBuilderParams::class.java)

    val dataProvider = params.dataProvider
    require(dataProvider.isNotEmpty()) { "data_provider must not be empty" }
    require(params.hasSubpoolMapStorageParams()) { "subpool_map_storage_params must be set" }
    require(params.hasVidRankMapStorageParams()) { "vid_rank_map_storage_params must be set" }
    require(params.hasEncryptedSubpoolMapsDek()) { "encrypted_subpool_maps_dek must be set" }
    require(params.totalShards > 0) { "total_shards must be > 0" }

    val kmsClient =
      requireNotNull(kmsClients[dataProvider]) { "KMS client not found for $dataProvider" }

    val subpoolFingerprintsStore =
      SubpoolFingerprintsStore(
        buildSubpoolMapStorageClient(getSubpoolMapStorageConfig(params.subpoolMapStorageParams)),
        kmsClient,
      )
    val rankIndexStore =
      RankIndexStore(
        buildVidRankMapStorageClient(getVidRankMapStorageConfig(params.vidRankMapStorageParams)),
        kmsClient,
      )

    val runDate = today()
    val retention =
      SubpoolRetention(
        rankIndexBlobsStub = rankIndexBlobsStub,
        rankIndexStore = rankIndexStore,
        dataProvider = dataProvider,
        modelLine = params.modelLine,
        // TODO(@Marco-Premier): source RETENTION_DAYS from static runner config instead of the
        //   default; it MUST exceed the deployment's maximum measurement-report window.
        retentionDays = DEFAULT_RETENTION_DAYS,
        today = runDate,
      )

    val subpoolRanker =
      SubpoolRanker(
        subpoolFingerprintsStore = subpoolFingerprintsStore,
        rankIndexStore = rankIndexStore,
        rankIndexBlobsStub = rankIndexBlobsStub,
        retention = retention,
        dataProvider = dataProvider,
        rawImpressionUpload = params.rawImpressionUpload,
        modelLine = params.modelLine,
        vidRankMapBlobPrefix = params.vidRankMapStorageParams.blobPrefix,
        kekUri = getVidRankMapKekUri(dataProvider),
        encryptedSubpoolMapsDek = params.encryptedSubpoolMapsDek,
        maxEventDate = params.maxEventDate,
        retentionDays = DEFAULT_RETENTION_DAYS,
        today = runDate,
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
        totalShards = params.totalShards,
        vidLabelerQueue = vidLabelerQueue,
      )
      .run()
  }

  companion object {
    /**
     * Default retention window in days. TODO(@Marco-Premier): replace with static runner config —
     * MUST exceed the deployment's maximum measurement-report window (see Data Deletion).
     */
    private const val DEFAULT_RETENTION_DAYS = 90
  }
}
