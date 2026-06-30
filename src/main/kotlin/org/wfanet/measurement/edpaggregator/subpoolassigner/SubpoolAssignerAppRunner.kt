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
import com.google.protobuf.Parser
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.edpaggregator.BaseVidLabelingTeeAppRunner
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.gcsHadoopConfiguration
import org.wfanet.measurement.edpaggregator.runBlockingWithTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams.StorageParams
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import picocli.CommandLine

/**
 * CLI entry point for the [SubpoolAssignerApp] Phase-0 TEE container.
 *
 * Pulls EDPA mTLS material from Secret Manager, builds per-`DataProvider` [KmsClient]s from the
 * EDPA-level `event-data-provider-configs.textproto` via Workload Identity Federation, opens a
 * mutual-TLS channel to the Secure Computation control plane for `WorkItem` / `WorkItemAttempt`
 * writes and a mutual-TLS channel to the EDP Aggregator metadata-storage public API for the
 * `RawImpressionUpload`, `RawImpressionUploadFile`, `RawImpressionUploadModelLine`, `RankerJob`,
 * and `PoolAssignmentJob` services, subscribes to the Phase-0 Pub/Sub topic, wires the production
 * storage / Parquet / pool-emit-model / per-EDP KEK seams (via [BaseVidLabelingTeeAppRunner]), and
 * hands everything to [SubpoolAssignerApp.run].
 */
@CommandLine.Command(name = "subpool_assigner_app_runner")
class SubpoolAssignerAppRunner :
  BaseVidLabelingTeeAppRunner(
    hadoopConfigurationFor = { cfg -> gcsHadoopConfiguration(requireNotNull(cfg.projectId)) }
  ) {

  @CommandLine.Option(
    names = ["--vid-rank-builder-queue"],
    description =
      ["Resource name of the Secure Computation queue for Phase-1 VidRankBuilder work."],
    required = true,
  )
  private lateinit var vidRankBuilderQueue: String

  private val getStorageConfig: (StorageParams) -> StorageConfig = { storageParams ->
    storageConfig(storageParams.gcsProjectId)
  }

  override fun run() {
    saveCommonEdpaCerts()
    val kmsClientsMap: Map<String, KmsClient> = buildKmsClientsMap()

    val pubSubClient = DefaultGooglePubSubClient()
    val queueSubscriber = createQueueSubscriber(pubSubClient)
    val parser: Parser<WorkItem> = WorkItem.parser()

    val secureComputationPublicChannel = buildSecureComputationPublicChannel()
    val workItemsClient = WorkItemsGrpcKt.WorkItemsCoroutineStub(secureComputationPublicChannel)
    val workItemAttemptsClient =
      WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(secureComputationPublicChannel)

    val metadataStorageChannel = buildMetadataStoragePublicChannel()
    val rawImpressionUploadsClient = RawImpressionUploadServiceCoroutineStub(metadataStorageChannel)
    val rawImpressionUploadFilesClient =
      RawImpressionUploadFileServiceCoroutineStub(metadataStorageChannel)
    val rawImpressionUploadModelLinesClient =
      RawImpressionUploadModelLineServiceCoroutineStub(metadataStorageChannel)
    val rankerJobsClient = RankerJobServiceCoroutineStub(metadataStorageChannel)
    val poolAssignmentJobsClient = PoolAssignmentJobServiceCoroutineStub(metadataStorageChannel)

    val subpoolAssignerApp =
      SubpoolAssignerApp(
        subscriptionId = subscriptionId,
        queueSubscriber = queueSubscriber,
        parser = parser,
        workItemsClient = workItemsClient,
        workItemAttemptsClient = workItemAttemptsClient,
        vidRankBuilderQueue = vidRankBuilderQueue,
        kmsClients = kmsClientsMap,
        getSubpoolMapStorageConfig = getStorageConfig,
        getRawImpressionsStorageConfig = getStorageConfig,
        getModelStorageConfig = getStorageConfig,
        rawImpressionUploadsStub = rawImpressionUploadsClient,
        rawImpressionUploadModelLinesStub = rawImpressionUploadModelLinesClient,
        rankerJobsStub = rankerJobsClient,
        poolAssignmentJobsStub = poolAssignmentJobsClient,
        rawImpressionUploadFilesStub = rawImpressionUploadFilesClient,
        buildParquetStorageClient = { cfg, kms -> buildParquetStorageClient(cfg, kms) },
        buildSubpoolMapStorageClient = { cfg -> buildStorageClient(cfg) },
        loadPoolEmitLabeler = { modelStorageConfig, modelBlobPath ->
          VirtualPeoplePoolEmitLabeler.fromCompiledNodeBlob(
            readCompiledModelBlob(modelStorageConfig, modelBlobPath)
          )
        },
        getSubpoolMapKekUri = { dataProvider -> kekUriFor(dataProvider) },
      )

    runBlockingWithTelemetry { subpoolAssignerApp.run() }
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(SubpoolAssignerAppRunner(), args)
  }
}
