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

import com.google.cloud.storage.StorageOptions
import com.google.crypto.tink.KmsClient
import com.google.protobuf.Parser
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.edpaggregator.BaseTeeAppRunner
import org.wfanet.measurement.edpaggregator.runBlockingWithTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams.StorageParams
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient
import picocli.CommandLine

/**
 * CLI entry point for the [VidRankBuilderApp] Phase-1 TEE container.
 *
 * Pulls EDPA mTLS material from Secret Manager, builds per-`DataProvider` [KmsClient]s from the
 * EDPA-level `event-data-provider-configs.textproto` via Workload Identity Federation, opens a
 * mutual-TLS channel to the Secure Computation control plane for `WorkItem` / `WorkItemAttempt`
 * writes and a mutual-TLS channel to the EDP Aggregator metadata-storage public API for the
 * `RankerJob`, `RankIndexBlob`, `RawImpressionUploadModelLine`, `VidLabelingJob`, and
 * `RawImpressionUploadFile` services, subscribes to the Phase-1 Pub/Sub topic, and hands everything
 * to [VidRankBuilderApp.run].
 */
@CommandLine.Command(name = "vid_rank_builder_app_runner")
class VidRankBuilderAppRunner : BaseTeeAppRunner() {

  @CommandLine.Option(
    names = ["--vid-labeler-queue"],
    description = ["Resource name of the Secure Computation queue for Phase-2 VidLabeler work."],
    required = true,
  )
  private lateinit var vidLabelerQueue: String

  override fun run() {
    saveCommonEdpaCerts()
    val kmsClientsMap: Map<String, KmsClient> = buildKmsClientsMap()
    // Per-EDP rank-index retention, validated positive at startup so a missing config
    // (proto3 int defaults to 0 -> evict-everything) makes the runner refuse to boot rather than
    // silently corrupt rank state. Compliance-tied: it MUST also exceed the EDP's maximum
    // measurement-report window (operator's responsibility to configure).
    val retentionDaysByDataProvider: Map<String, Int> =
      edpsConfig.eventDataProviderConfigList.associate { edpConfig ->
        require(edpConfig.retentionDays > 0) {
          "retention_days must be > 0 for ${edpConfig.dataProvider}, got ${edpConfig.retentionDays}"
        }
        edpConfig.dataProvider to edpConfig.retentionDays
      }

    val pubSubClient = DefaultGooglePubSubClient()
    val queueSubscriber = createQueueSubscriber(pubSubClient)
    val parser: Parser<WorkItem> = WorkItem.parser()

    val secureComputationPublicChannel = buildSecureComputationPublicChannel()
    val workItemsClient = WorkItemsGrpcKt.WorkItemsCoroutineStub(secureComputationPublicChannel)
    val workItemAttemptsClient =
      WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(secureComputationPublicChannel)

    val metadataStorageChannel = buildMetadataStoragePublicChannel()
    val rankerJobsClient = RankerJobServiceCoroutineStub(metadataStorageChannel)
    val rankIndexBlobsClient = RankIndexBlobServiceCoroutineStub(metadataStorageChannel)
    val rawImpressionUploadModelLinesClient =
      RawImpressionUploadModelLineServiceCoroutineStub(metadataStorageChannel)
    val vidLabelingJobsClient = VidLabelingJobServiceCoroutineStub(metadataStorageChannel)
    val rawImpressionUploadFilesClient =
      RawImpressionUploadFileServiceCoroutineStub(metadataStorageChannel)

    val vidRankBuilderApp =
      VidRankBuilderApp(
        subscriptionId = subscriptionId,
        queueSubscriber = queueSubscriber,
        parser = parser,
        workItemsClient = workItemsClient,
        workItemAttemptsClient = workItemAttemptsClient,
        kmsClients = kmsClientsMap,
        retentionDaysByDataProvider = retentionDaysByDataProvider,
        buildSubpoolMapStorageClient = ::buildStorageClient,
        buildVidRankMapStorageClient = ::buildStorageClient,
        rankerJobsStub = rankerJobsClient,
        rankIndexBlobsStub = rankIndexBlobsClient,
        rawImpressionUploadModelLinesStub = rawImpressionUploadModelLinesClient,
        vidLabelingJobsStub = vidLabelingJobsClient,
        rawImpressionUploadFilesStub = rawImpressionUploadFilesClient,
        vidLabelerQueue = vidLabelerQueue,
      )

    runBlockingWithTelemetry { vidRankBuilderApp.run() }
  }

  /**
   * Builds a bucket-rooted [StorageClient] for [storageParams], parsing the bucket from the `gs://`
   * blob_prefix and authenticating to GCS as the Confidential Space VM's attached service account.
   * Mirrors the deployed VID Labeling functions' storage-client construction.
   */
  private fun buildStorageClient(storageParams: StorageParams): StorageClient {
    val blobUri = SelectedStorageClient.parseBlobUri(storageParams.blobPrefix)
    val storageOptions =
      StorageOptions.newBuilder()
        .apply {
          if (storageParams.gcsProjectId.isNotEmpty()) {
            setProjectId(storageParams.gcsProjectId)
          }
        }
        .build()
    return GcsStorageClient(storageOptions.service, blobUri.bucket)
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(VidRankBuilderAppRunner(), args)
  }
}
