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
import com.google.protobuf.TypeRegistry
import java.util.concurrent.ConcurrentHashMap
import org.jetbrains.annotations.VisibleForTesting
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig.getResultsFulfillerConfigAsByteArray
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.edpaggregator.BaseVidLabelingTeeAppRunner
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.gcsHadoopConfiguration
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
    // Carry the blob prefix so buildStorageClient can root the multi-key rank-map store at its
    // bucket (VidLabelerParams.StorageParams names the prefix field impressions_blob_prefix).
    storageConfig(storageParams.gcsProjectId).copy(blobPrefix = storageParams.impressionsBlobPrefix)
  }

  // Caches resolved EventTemplate descriptors by (blob URI, type name) so a descriptor blob is read
  // and parsed once per process instead of once per WorkItem.
  private val eventDescriptorCache =
    ConcurrentHashMap<Pair<String, String>, Descriptors.Descriptor>()

  // Caches the parsed PopulationSpec + resolved ranges per (blob URI, event type) so a multi-MB
  // spec is read and indexed once per process rather than once per WorkItem.
  private val populationAttributeWriterCache =
    ConcurrentHashMap<WriterCacheKey, PopulationAttributeWriter>()

  /** Cache key for [populationAttributeWriterCache]. */
  private data class WriterCacheKey(
    val populationSpecBlobUri: String,
    val eventTemplateDescriptorBlobUri: String,
    val eventTemplateType: String,
  )

  override fun run() {
    saveCommonEdpaCerts()
    val kmsClients: Map<String, KmsClient> = buildKmsClientsMap()
    // Per-EDP output KEK for the non-memoized Phase-2 path (the memoized path derives its KEK
    // from the rank-index blobs). Only EDPs that use the VID Labeling pipeline set
    // kms_config.kek_uri; AWS/direct-path EDPs (e.g. edpa_meta) leave it unset in the shared
    // all-EDP config and never produce Phase-2 work, so skip them here instead of failing to boot.
    val encryptKekUris: Map<String, String> = buildMap {
      for (dataProvider in kmsClients.keys) {
        val kekUri = kekUriForOrNull(dataProvider)
        if (kekUri != null) {
          put(dataProvider, kekUri)
        }
      }
    }

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
        encryptKekUris = encryptKekUris,
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
          ParquetImpressionConverter(
            eventDescriptor = resolveEventDescriptor(config),
            populationAttributeWriter = resolvePopulationAttributeWriter(config),
          )
        },
        // Process-scoped: one cache shared across every WorkItem this container processes, so the
        // memoized rank index is reused across WorkItems when the Phase-1 snapshot set is
        // unchanged.
        memoizedRankIndexCache = MemoizedRankIndexCache(),
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

  /**
   * Resolves the [config]'s [PopulationAttributeWriter] by loading the `PopulationSpec` textproto
   * at [VidLabelerParams.ModelLineConfig.getPopulationSpecBlobUri] from EDPA config storage
   * (mirrors `ResultsFulfillerAppRunner.buildModelLineMap`, which loads the same blob for the same
   * model line). Cached per (blob URI, event type name).
   */
  @VisibleForTesting
  suspend fun resolvePopulationAttributeWriter(
    config: VidLabelerParams.ModelLineConfig
  ): PopulationAttributeWriter {
    val blobUri = config.populationSpecBlobUri
    require(blobUri.isNotEmpty()) {
      "population_spec_blob_uri must be set; without it the labeled output would carry the " +
        "DataProvider's uploaded demographics instead of the ones the model assigned"
    }
    // Keyed on the descriptor blob too: two model lines can share a spec URI and event type name
    // while resolving that type from different descriptor blobs, and the writer is bound to the
    // descriptor it was built against.
    val cacheKey =
      WriterCacheKey(blobUri, config.eventTemplateDescriptorBlobUri, config.eventTemplateType)
    populationAttributeWriterCache[cacheKey]?.let {
      return it
    }
    val eventDescriptor = resolveEventDescriptor(config)
    // The spec's SubPopulation.attributes are Any-packed event templates. TypeRegistry.Builder.add
    // registers the type's whole file and recurses through its dependencies, so the event type's
    // transitive closure covers every template the spec can reference.
    val typeRegistry: TypeRegistry = TypeRegistry.newBuilder().add(eventDescriptor).build()
    val specBytes = getResultsFulfillerConfigAsByteArray(googleProjectId, blobUri)
    val populationSpec =
      specBytes.inputStream().reader(Charsets.UTF_8).use { reader ->
        parseTextProto(reader, PopulationSpec.getDefaultInstance(), typeRegistry)
      }
    val writer = PopulationAttributeWriter(eventDescriptor, populationSpec)
    return populationAttributeWriterCache.computeIfAbsent(cacheKey) { writer }
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
