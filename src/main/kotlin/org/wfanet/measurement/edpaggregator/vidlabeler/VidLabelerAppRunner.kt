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
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.ExtensionRegistry
import java.util.concurrent.ConcurrentHashMap
import org.jetbrains.annotations.VisibleForTesting
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig.getResultsFulfillerConfigAsByteArray
import org.wfanet.measurement.edpaggregator.BaseVidLabelingTeeAppRunner
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.gcsHadoopConfiguration
import org.wfanet.measurement.edpaggregator.runBlockingWithTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams.StorageParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import picocli.CommandLine

/**
 * CLI entry point for the [VidLabelerApp] Phase-2 TEE container.
 *
 * Pulls EDPA mTLS material from Secret Manager, builds per-`DataProvider` [KmsClient]s from the
 * EDPA-level `event-data-provider-configs.textproto` via Workload Identity Federation, opens a
 * mutual-TLS channel to the Secure Computation control plane for `WorkItem` / `WorkItemAttempt`
 * reads and a mutual-TLS channel to the EDP Aggregator metadata-storage public API for the
 * `VidLabelingJob`, `RawImpressionUploadModelLine`, `RawImpressionUploadFile`, and `RankIndexBlob`
 * services, subscribes to the Phase-2 Pub/Sub queue, wires the production storage / model /
 * converter seams, and hands everything to [VidLabelerApp.run].
 */
@CommandLine.Command(name = "vid_labeler_app_runner")
class VidLabelerAppRunner :
  BaseVidLabelingTeeAppRunner(
    hadoopConfigurationFor = { cfg -> gcsHadoopConfiguration(requireNotNull(cfg.projectId)) }
  ) {

  private val getStorageConfig: (StorageParams) -> StorageConfig = { storageParams ->
    storageConfig(storageParams.gcsProjectId)
  }

  // Caches resolved EventTemplate descriptors by (blob URI, type name) so a descriptor blob is read
  // and parsed once per process instead of once per WorkItem.
  private val eventDescriptorCache =
    ConcurrentHashMap<Pair<String, String>, Descriptors.Descriptor>()

  override fun run() {
    saveCommonEdpaCerts()
    val kmsClients: Map<String, KmsClient> = buildKmsClientsMap()

    val pubSubClient = DefaultGooglePubSubClient()
    val queueSubscriber = createQueueSubscriber(pubSubClient)

    val secureComputationPublicChannel = buildSecureComputationPublicChannel()
    val workItemsClient = WorkItemsGrpcKt.WorkItemsCoroutineStub(secureComputationPublicChannel)
    val workItemAttemptsClient =
      WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(secureComputationPublicChannel)

    val metadataStorageChannel = buildMetadataStoragePublicChannel()
    val vidLabelingJobsClient = VidLabelingJobServiceCoroutineStub(metadataStorageChannel)
    val rawImpressionUploadModelLinesClient =
      RawImpressionUploadModelLineServiceCoroutineStub(metadataStorageChannel)
    val rawImpressionUploadFilesClient =
      RawImpressionUploadFileServiceCoroutineStub(metadataStorageChannel)
    val rankIndexBlobsClient = RankIndexBlobServiceCoroutineStub(metadataStorageChannel)

    val vidLabelerApp =
      VidLabelerApp(
        subscriptionId = subscriptionId,
        queueSubscriber = queueSubscriber,
        parser = WorkItem.parser(),
        workItemsClient = workItemsClient,
        workItemAttemptsClient = workItemAttemptsClient,
        kmsClients = kmsClients,
        // TODO(world-federation-of-advertisers/cross-media-measurement#4093): populate per-EDP
        //   encrypt KEK URIs from EventDataProviderConfig.KmsConfig.kek_uri (added in #4093) so the
        //   non-memoized Phase-2 path can wrap its labeled output. Empty until then; the memoized
        //   path derives its KEK from the rank-index blobs and does not use this.
        encryptKekUris = emptyMap(),
        getStorageConfig = getStorageConfig,
        vidLabelingJobsStub = vidLabelingJobsClient,
        rawImpressionUploadModelLinesStub = rawImpressionUploadModelLinesClient,
        rankIndexBlobsStub = rankIndexBlobsClient,
        rawImpressionUploadFilesStub = rawImpressionUploadFilesClient,
        buildParquetStorageClient = { cfg, kms -> buildParquetStorageClient(cfg, kms) },
        buildVidRankMapStorageClient = { cfg -> buildStorageClient(cfg) },
        loadAssigner = { modelStorageConfig, modelBlobUri ->
          VirtualPeopleVidAssigner.fromCompiledNodeBlob(
            readCompiledModelBlob(modelStorageConfig, modelBlobUri)
          )
        },
        buildImpressionConverter = { _, config ->
          ParquetImpressionConverter(eventDescriptor = resolveEventDescriptor(config))
        },
      )

    runBlockingWithTelemetry { vidLabelerApp.run() }
  }

  /**
   * Resolves the [config]'s EventTemplate event [Descriptors.Descriptor] by loading the
   * `FileDescriptorSet` at [VidLabelerParams.ModelLineConfig.getEventTemplateDescriptorBlobUri]
   * from EDPA config storage and finding [VidLabelerParams.ModelLineConfig.getEventTemplateType]
   * within it (mirrors `ResultsFulfillerAppRunner.buildModelLineMap`). Cached per (blob URI, type
   * name).
   */
  @VisibleForTesting
  suspend fun resolveEventDescriptor(
    config: VidLabelerParams.ModelLineConfig
  ): Descriptors.Descriptor {
    val blobUri = config.eventTemplateDescriptorBlobUri
    val typeName = config.eventTemplateType
    require(blobUri.isNotEmpty()) { "event_template_descriptor_blob_uri must be set" }
    require(typeName.isNotEmpty()) { "event_template_type must be set" }
    eventDescriptorCache[blobUri to typeName]?.let {
      return it
    }
    val descriptorBytes = getResultsFulfillerConfigAsByteArray(googleProjectId, blobUri)
    val fileDescriptorSet =
      DescriptorProtos.FileDescriptorSet.parseFrom(descriptorBytes, EXTENSION_REGISTRY)
    val descriptors: List<Descriptors.Descriptor> =
      ProtoReflection.buildDescriptors(listOf(fileDescriptorSet), COMPILED_PROTOBUF_TYPES)
    val eventDescriptor =
      descriptors.firstOrNull { it.fullName == typeName }
        ?: error("EventTemplate descriptor not found for type: $typeName")
    return eventDescriptorCache.computeIfAbsent(blobUri to typeName) { eventDescriptor }
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(VidLabelerAppRunner(), args)

    /**
     * [Descriptors.FileDescriptor]s of protobuf types known at compile time that may be referenced
     * from a loaded [DescriptorProtos.FileDescriptorSet].
     */
    private val COMPILED_PROTOBUF_TYPES: Iterable<Descriptors.FileDescriptor> =
      ProtoReflection.WELL_KNOWN_TYPES.asSequence().asIterable()

    /** Extension registry so EventTemplate annotations on the loaded descriptors parse. */
    private val EXTENSION_REGISTRY =
      ExtensionRegistry.newInstance()
        .also { EventAnnotationsProto.registerAllExtensions(it) }
        .unmodifiable
  }
}
