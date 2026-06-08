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
import org.wfanet.measurement.edpaggregator.BaseTeeAppRunner
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.runBlockingWithTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams.StorageParams
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import picocli.CommandLine

/**
 * CLI entry point for the [SubpoolAssignerApp] Phase-0 TEE container.
 *
 * Pulls EDPA mTLS material from Secret Manager, builds per-`DataProvider`
 * [KmsClient]s from the EDPA-level `event-data-provider-configs.textproto`
 * via Workload Identity Federation, opens a mutual-TLS channel to the
 * Secure Computation control plane for `WorkItem` / `WorkItemAttempt` writes,
 * subscribes to the Phase-0 Pub/Sub topic, and hands everything to
 * [SubpoolAssignerApp.run].
 */
@CommandLine.Command(name = "subpool_assigner_app_runner")
class SubpoolAssignerAppRunner : BaseTeeAppRunner() {

  private val getStorageConfig: (StorageParams) -> StorageConfig = { storageParams ->
    StorageConfig(projectId = storageParams.gcsProjectId)
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

    val metadataStoragePublicChannel = buildMetadataStoragePublicChannel()
    val impressionMetadataStub = ImpressionMetadataServiceCoroutineStub(metadataStoragePublicChannel)

    val subpoolAssignerApp =
      SubpoolAssignerApp(
        subscriptionId = subscriptionId,
        queueSubscriber = queueSubscriber,
        parser = parser,
        workItemsClient = workItemsClient,
        workItemAttemptsClient = workItemAttemptsClient,
        impressionMetadataStub = impressionMetadataStub,
        kmsClients = kmsClientsMap,
        getSubpoolMapStorageConfig = getStorageConfig,
        getRawImpressionsStorageConfig = getStorageConfig,
      )

    runBlockingWithTelemetry { subpoolAssignerApp.run() }
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(SubpoolAssignerAppRunner(), args)
  }
}
