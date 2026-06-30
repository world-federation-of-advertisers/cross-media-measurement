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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig.getResultsFulfillerConfigAsByteArray
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.BaseTeeAppRunner
import org.wfanet.measurement.edpaggregator.StorageConfig
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
import org.wfanet.measurement.storage.ParquetEncryptionConfig
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
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
class VidLabelerAppRunner : BaseTeeAppRunner() {

  private val getStorageConfig: (StorageParams) -> StorageConfig = { storageParams ->
    StorageConfig(projectId = storageParams.gcsProjectId)
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
        getStorageConfig = getStorageConfig,
        vidLabelingJobsStub = vidLabelingJobsClient,
        rawImpressionUploadModelLinesStub = rawImpressionUploadModelLinesClient,
        rankIndexBlobsStub = rankIndexBlobsClient,
        rawImpressionUploadFilesStub = rawImpressionUploadFilesClient,
        buildParquetStorageClient = { cfg, kms ->
          ParquetStorageClient(
            conf = productionConfiguration(requireNotNull(cfg.projectId)),
            // RawImpressionSource hands ParquetStorageClient absolute gs:// URIs, so the root is
            // only the FileSystem selector; the read never resolves against it.
            rootPath = Path(GCS_ROOT_URI),
            encryptionConfig = ParquetEncryptionConfig(kmsProvider = { kms }),
          )
        },
        buildVidRankMapStorageClient = { cfg -> buildSelectedStorageClient(cfg, GCS_ROOT_URI) },
        loadAssigner = { modelBlobUri ->
          val blobUri = SelectedStorageClient.parseBlobUri(modelBlobUri)
          val modelBlob =
            SelectedStorageClient(blobUri, /* rootDirectory= */ null, googleProjectId)
              .getBlob(blobUri.key) ?: error("Compiled-model blob not found: $modelBlobUri")
          VirtualPeopleVidAssigner.fromCompiledNodeBlob(modelBlob.read().flatten())
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
   * from EDPA config storage and finding
   * [VidLabelerParams.ModelLineConfig.getEventTemplateTypeName] within it (mirrors
   * `ResultsFulfillerAppRunner.buildModelLineMap`). Cached per (blob URI, type name).
   */
  private suspend fun resolveEventDescriptor(
    config: VidLabelerParams.ModelLineConfig
  ): Descriptors.Descriptor {
    val blobUri = config.eventTemplateDescriptorBlobUri
    val typeName = config.eventTemplateTypeName
    require(blobUri.isNotEmpty()) { "event_template_descriptor_blob_uri must be set" }
    require(typeName.isNotEmpty()) { "event_template_type_name must be set" }
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

    /** Root URI selecting the GCS-backed [SelectedStorageClient] for absolute `gs://` blob URIs. */
    private const val GCS_ROOT_URI = "gs://"

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

    /**
     * Hadoop [Configuration] selecting the GCS connector with Compute-Engine (VM SA) auth, for the
     * `ParquetStorageClient` reading raw impressions.
     */
    private fun productionConfiguration(projectId: String): Configuration =
      Configuration().apply {
        set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        set("fs.gs.auth.type", "COMPUTE_ENGINE")
        set("fs.gs.project.id", projectId)
      }

    /**
     * Builds a [SelectedStorageClient] for [rootUri], passing the optional
     * [StorageConfig.projectId] through to the underlying GCS client. The concrete type also
     * satisfies [ConditionalOperationStorageClient] consumers (e.g. [RankIndexStore] via
     * `buildVidRankMapStorageClient`).
     */
    private fun buildSelectedStorageClient(
      cfg: StorageConfig,
      rootUri: String,
    ): SelectedStorageClient =
      SelectedStorageClient(
        SelectedStorageClient.parseBlobUri(rootUri),
        cfg.rootDirectory,
        cfg.projectId,
      )
  }
}
