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
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams.StorageParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.storage.SelectedStorageClient
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
 * @param kmsClients Per-DataProvider KMS clients used to wrap/unwrap DEKs.
 * @param retentionDaysByDataProvider Per-DataProvider rank-index retention window (in days),
 *   sourced from the EDPA config; MUST exceed the EDP's maximum measurement-report window.
 * @param rankerJobsStub stub to gate on / mark this `RankerJob`.
 * @param rankIndexBlobsStub stub for locating the prior snapshot, retention, and new blob rows.
 * @param rawImpressionUploadModelLinesStub stub to flip the parent `RANKING` -> `LABELING`.
 * @param vidLabelingJobsStub stub for the last-job-out Phase-2 fan-out.
 * @param rawImpressionUploadFilesStub stub to list the upload's files for Phase-2 sharding.
 * @param vidLabelerQueue Secure Computation queue the last-job-out publishes Phase-2 VidLabeler
 *   WorkItems to.
 * @param buildSubpoolMapStorageClient Builds the subpool-map [StorageClient] from its
 *   [StorageParams] (bucket parsed from the `gs://` blob_prefix).
 * @param buildVidRankMapStorageClient Builds the vid-rank-map [StorageClient] from its
 *   [StorageParams] (bucket parsed from the `gs://` blob_prefix).
 * @param today Supplies the UTC date treated as "now" for retention (injectable for tests).
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
  private val buildSubpoolMapStorageClient: (StorageParams) -> StorageClient,
  private val buildVidRankMapStorageClient: (StorageParams) -> StorageClient,
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

    val subpoolFingerprintsStore =
      SubpoolFingerprintsStore(
        buildSubpoolMapStorageClient(params.subpoolMapStorageParams),
        kmsClient,
      )
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
        vidRankMapBlobPrefix =
          SelectedStorageClient.parseBlobUri(params.vidRankMapStorageParams.blobPrefix).key,
        kekUri = params.encryptedSubpoolMapsDek.kekUri,
        encryptedSubpoolMapsDek = params.encryptedSubpoolMapsDek,
        maxEventDate = params.maxEventDate,
        retentionDays = retentionDays,
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
        vidLabelerParamsTemplate = buildVidLabelerParamsTemplate(params),
        vidLabelerQueue = vidLabelerQueue,
        // Forwarded verbatim from Phase-0 via VidRankBuilderParams (REQUIRED); VidRankBuilder's
        // `require(maxFileBatchSizeBytes > 0)` rejects an unset/zero value at the boundary.
        maxFileBatchSizeBytes = params.maxFileBatchSizeBytes,
      )
      .run()
  }

  /**
   * Builds the memoized [VidLabelerParams] template for the Phase-2 fan-out from the pass-through
   * fields the SubpoolAssigner stamped on [params]. The last-`RankerJob`-out copies it per
   * `VidLabelingJob`, filling the job's name and its files; the Phase-2 TEE resolves the rank-index
   * blobs from the `RankIndexBlobService` rather than receiving their URIs here.
   */
  private fun buildVidLabelerParamsTemplate(params: VidRankBuilderParams): VidLabelerParams {
    require(params.modelBlobPath.isNotEmpty()) { "model_blob_path must be set" }
    require(params.hasRawImpressionStorageParams()) { "raw_impression_storage_params must be set" }
    require(params.hasVidLabeledImpressionsStorageParams()) {
      "vid_labeled_impressions_storage_params must be set"
    }
    return vidLabelerParams {
      dataProvider = params.dataProvider
      rawImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = params.rawImpressionStorageParams.gcsProjectId
          impressionsBlobPrefix = params.rawImpressionStorageParams.blobPrefix
        }
      vidLabeledImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = params.vidLabeledImpressionsStorageParams.gcsProjectId
          impressionsBlobPrefix = params.vidLabeledImpressionsStorageParams.blobPrefix
        }
      modelLineConfigs.put(
        params.modelLine,
        VidLabelerParamsKt.modelLineConfig {
          labelerInputFieldMapping.putAll(params.labelerInputFieldMappingMap)
          eventTemplateFieldMapping.putAll(params.eventTemplateFieldMappingMap)
          // Active-window pass-through (OPTIONAL): Phase-2 drops impressions outside this window.
          if (params.hasActiveStartTime()) {
            activeStartTime = params.activeStartTime
          }
          if (params.hasActiveEndTime()) {
            activeEndTime = params.activeEndTime
          }
        },
      )
      memoizedParams =
        VidLabelerParamsKt.memoizedParams {
          modelLine = params.modelLine
          modelBlobPath = params.modelBlobPath
          vidRankMapStorageParams =
            VidLabelerParamsKt.storageParams {
              gcsProjectId = params.vidRankMapStorageParams.gcsProjectId
              impressionsBlobPrefix = params.vidRankMapStorageParams.blobPrefix
            }
        }
    }
  }
}
