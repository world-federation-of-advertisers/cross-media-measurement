/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.Parser
import com.google.protobuf.TypeRegistry
import java.io.File
import java.time.Duration
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.HeaderKt.TrusTeeKt.EnvelopeEncryptionKt.awsKmsParams
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig.getResultsFulfillerConfigAsByteArray
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MaximumRateThrottler
import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfig
import org.wfanet.measurement.edpaggregator.BaseTeeAppRunner
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfillerMetrics.Companion.measured
import org.wfanet.measurement.edpaggregator.runBlockingWithTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.StorageParams
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.ParallelInMemoryVidIndexMap
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import picocli.CommandLine

@CommandLine.Command(name = "results_fulfiller_app_runner")
class ResultsFulfillerAppRunner : BaseTeeAppRunner() {
  private val metrics by lazy { ResultsFulfillerMetrics.create() }

  @CommandLine.Option(
    names = ["--trusted-cert-collection-secret-id"],
    description = ["Secret ID of trusted root collections file."],
    required = true,
  )
  private lateinit var trustedCertCollectionSecretId: String

  @CommandLine.Option(
    names = ["--trusted-cert-collection-file-path"],
    description = ["Local path where the --trusted-cert-collection-secret-id secret is stored."],
    required = true,
  )
  private lateinit var trustedCertCollectionFilePath: String

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*", heading = "Duchy info\n")
  lateinit var duchyInfos: List<DuchyFlags>
    private set

  class DuchyFlags {
    @CommandLine.Option(names = ["--duchy-id"], required = true, description = ["Id of the duchy"])
    lateinit var duchyId: String

    @CommandLine.Option(
      names = ["--duchy-target"],
      required = true,
      description = ["Target of the duchy"],
    )
    lateinit var duchyTarget: String

    @CommandLine.Option(
      names = ["--duchy-cert-host"],
      required = false,
      description = ["Duchy mTLS cert hostname override for localhost testing."],
    )
    var duchyCertHost: String? = null
  }

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*", heading = "Model line info\n")
  lateinit var modelLines: List<ModelLineFlags>
    private set

  class ModelLineFlags {
    @CommandLine.Option(
      names = ["--model-line"],
      required = true,
      description = ["model line resource name"],
    )
    lateinit var modelLine: String

    @CommandLine.Option(
      names = ["--population-spec-file-blob-uri"],
      required = true,
      description = ["Blob uri to the proto."],
    )
    lateinit var populationSpecFileBlobUri: String

    @CommandLine.Option(
      names = ["--event-template-descriptor-blob-uri"],
      description =
        ["Config storage blob URI to the FileDescriptorSet for EventTemplate metadata types."],
      required = true,
    )
    lateinit var eventTemplateDescriptorBlobUri: String

    @CommandLine.Option(
      names = ["--event-template-type-name"],
      description = ["Fully qualified type name url of the event proto message."],
      required = true,
    )
    lateinit var eventTemplateTypeName: String
  }

  @CommandLine.Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target of the Kingdom public API server"],
    required = true,
  )
  private lateinit var kingdomPublicApiTarget: String

  @CommandLine.Option(
    names = ["--kingdom-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Kingdom public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --kingdom-public-api-target.",
      ],
    required = false,
  )
  private var kingdomPublicApiCertHost: String? = null

  @CommandLine.Option(
    names = ["--pipeline-batch-size"],
    description = ["Number of events to process in each batch."],
    defaultValue = "256",
  )
  private var pipelineBatchSize: Int = 256

  @CommandLine.Option(
    names = ["--pipeline-channel-capacity"],
    description = ["Per-worker channel capacity in number of batches."],
    defaultValue = "64",
  )
  private var pipelineChannelCapacity: Int = 64

  @CommandLine.Option(
    names = ["--pipeline-thread-pool-size"],
    description =
      ["Size of the thread pool for the coroutine dispatcher. Defaults to available CPU cores."],
    defaultValue = "0",
  )
  private var pipelineThreadPoolSize: Int = 0

  @CommandLine.Option(
    names = ["--pipeline-workers"],
    description =
      [
        "Number of parallel worker coroutines for processing batches. Defaults to available CPU cores."
      ],
    defaultValue = "0",
  )
  private var pipelineWorkers: Int = 0

  @CommandLine.Option(
    names = ["--pipeline-read-concurrency"],
    description =
      [
        "Maximum number of impression blobs read and DEK-decrypted concurrently. Bounds the " +
          "outbound Cloud Storage and Cloud KMS fan-out so a work item cannot exhaust Cloud NAT " +
          "ports or overwhelm KMS."
      ],
    defaultValue = "16",
  )
  private var pipelineReadConcurrency: Int = 16

  @CommandLine.Option(
    names = ["--get-requisition-min-interval"],
    description =
      [
        "Minimum interval between outbound GetRequisition calls to Kingdom. Paces this app's " +
          "GetRequisition traffic to stay within Kingdom's per-principal rate limit for that " +
          "method, which is shared across every concurrent instance of this app authenticating " +
          "as the same data provider."
      ],
    defaultValue = "100ms",
  )
  private lateinit var getRequisitionMinInterval: Duration

  @CommandLine.Option(
    names = ["--kingdom-requisitions-min-interval"],
    description =
      [
        "Minimum interval between outbound FulfillDirectRequisition/RefuseRequisition calls to " +
          "Kingdom. These share a Kingdom rate-limit bucket with every other Requisitions method " +
          "besides GetRequisition, so this should stay within that bucket's sustained rate."
      ],
    defaultValue = "200ms",
  )
  private lateinit var kingdomRequisitionsMinInterval: Duration

  private val getImpressionsStorageConfig: (StorageParams) -> StorageConfig = { storageParams ->
    StorageConfig(projectId = storageParams.gcsProjectId)
  }

  override fun run() {
    require(getRequisitionMinInterval > Duration.ZERO) {
      "--get-requisition-min-interval must be positive, got $getRequisitionMinInterval"
    }
    require(kingdomRequisitionsMinInterval > Duration.ZERO) {
      "--kingdom-requisitions-min-interval must be positive, got $kingdomRequisitionsMinInterval"
    }

    // Pull certificates needed to operate from Google Secrets.
    saveCommonEdpaCerts()
    saveExtraEdpaCerts()
    saveEdpsCerts()

    val (kmsClientsMap, trusTeeConfigMap) = buildKmsAndTrusTeeMaps()

    val pubSubClient = DefaultGooglePubSubClient()
    val queueSubscriber = createQueueSubscriber(pubSubClient)
    val parser: Parser<WorkItem> = WorkItem.parser()

    val secureComputationPublicChannel = buildSecureComputationPublicChannel()
    val workItemsClient = WorkItemsGrpcKt.WorkItemsCoroutineStub(secureComputationPublicChannel)
    val workItemAttemptsClient =
      WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(secureComputationPublicChannel)

    val metadataStoragePublicChannel = buildMetadataStoragePublicChannel()

    val requisitionMetadataClient =
      RequisitionMetadataServiceCoroutineStub(metadataStoragePublicChannel)
    val impressionMetadataClient =
      ImpressionMetadataServiceCoroutineStub(metadataStoragePublicChannel)
    val trustedRootCaCollectionFile = File(trustedCertCollectionFilePath)
    val duchiesMap = buildDuchyMap()

    val requisitionStubFactory =
      RequisitionStubFactoryImpl(
        cmmsCertHost = kingdomPublicApiCertHost,
        cmmsTarget = kingdomPublicApiTarget,
        trustedCertCollection = trustedRootCaCollectionFile,
        duchies = duchiesMap,
        grpcTelemetry = grpcTelemetry,
      )

    val modelLinesMap = runBlockingWithTelemetry { buildModelLineMap() }

    val cpuCount = Runtime.getRuntime().availableProcessors()
    val pipelineConfiguration =
      PipelineConfiguration(
        batchSize = pipelineBatchSize,
        channelCapacity = pipelineChannelCapacity,
        threadPoolSize = if (pipelineThreadPoolSize > 0) pipelineThreadPoolSize else cpuCount,
        workers = if (pipelineWorkers > 0) pipelineWorkers else cpuCount,
        readConcurrency = pipelineReadConcurrency,
      )
    pipelineConfiguration.validate()

    val resultsFulfillerApp =
      ResultsFulfillerApp(
        subscriptionId = subscriptionId,
        queueSubscriber = queueSubscriber,
        parser = parser,
        workItemsClient = workItemsClient,
        requisitionMetadataStub = requisitionMetadataClient,
        impressionMetadataStub = impressionMetadataClient,
        workItemAttemptsClient = workItemAttemptsClient,
        requisitionStubFactory = requisitionStubFactory,
        kmsClients = kmsClientsMap,
        trusTeeConfigs = trusTeeConfigMap,
        getImpressionsMetadataStorageConfig = getImpressionsStorageConfig,
        getImpressionsStorageConfig = getImpressionsStorageConfig,
        getRequisitionsStorageConfig = getImpressionsStorageConfig,
        modelLineInfoMap = modelLinesMap,
        pipelineConfiguration = pipelineConfiguration,
        requisitionsThrottler =
          MaximumRateThrottler(1_000_000_000.0 / getRequisitionMinInterval.toNanos()),
        kingdomThrottler =
          MaximumRateThrottler(1_000_000_000.0 / kingdomRequisitionsMinInterval.toNanos()),
        metrics = metrics,
      )

    runBlockingWithTelemetry { resultsFulfillerApp.run() }
  }

  /** Builds the per-EDP KMS-client and TrusTee-config maps in a single pass. */
  private fun buildKmsAndTrusTeeMaps(): Pair<Map<String, KmsClient>, Map<String, TrusTeeConfig>> {
    val entries =
      edpsConfig.eventDataProviderConfigList.map { edpConfig ->
        val kmsClient = buildKmsClient(edpConfig)
        val apiAwsKmsParams =
          when (edpConfig.kmsConfig.kmsType) {
            EventDataProviderConfig.KmsConfig.KmsType.AWS ->
              awsKmsParams {
                roleArn = edpConfig.kmsConfig.awsRoleArn
                roleSession = edpConfig.kmsConfig.awsRoleSessionName
                region = edpConfig.kmsConfig.awsRegion
                workloadIdentityIdTokenAudience = edpConfig.kmsConfig.awsAudience
              }
            EventDataProviderConfig.KmsConfig.KmsType.AWS_CONFIDENTIAL_SPACE ->
              awsKmsParams {
                roleArn = edpConfig.kmsConfig.awsRoleArn
                roleSession = edpConfig.kmsConfig.awsRoleSessionName
                region = edpConfig.kmsConfig.awsRegion
                confidentialSpaceAttestationTokenAudience = edpConfig.kmsConfig.awsAudience
              }
            else -> null
          }
        Triple(
          edpConfig.dataProvider,
          kmsClient,
          TrusTeeConfig(
            kmsClient = kmsClient,
            workloadIdentityProvider = edpConfig.kmsConfig.kmsAudience,
            impersonatedServiceAccount = edpConfig.kmsConfig.serviceAccount,
            awsKmsParams = apiAwsKmsParams,
          ),
        )
      }
    val kmsClients = entries.associate { (dp, kms, _) -> dp to kms }
    val trusTeeConfigs = entries.associate { (dp, _, trustee) -> dp to trustee }
    return kmsClients to trusTeeConfigs
  }

  private fun saveExtraEdpaCerts() {
    saveSecretToFile(trustedCertCollectionSecretId, trustedCertCollectionFilePath)
  }

  private fun saveEdpsCerts() {
    edpsConfig.eventDataProviderConfigList.forEach { edpConfig ->
      saveSecretToFile(
        edpConfig.consentSignalingConfig.certDerSecretId,
        edpConfig.consentSignalingConfig.certDerLocalPath,
      )
      saveSecretToFile(
        edpConfig.consentSignalingConfig.encPrivateDerSecretId,
        edpConfig.consentSignalingConfig.encPrivateDerLocalPath,
      )
      saveSecretToFile(
        edpConfig.consentSignalingConfig.encPrivateSecretId,
        edpConfig.consentSignalingConfig.encPrivateLocalPath,
      )
      saveSecretToFile(edpConfig.tlsConfig.tlsKeySecretId, edpConfig.tlsConfig.tlsKeyLocalPath)
      saveSecretToFile(edpConfig.tlsConfig.tlsPemSecretId, edpConfig.tlsConfig.tlsPemLocalPath)
    }
  }

  private fun buildDuchyMap(): Map<String, DuchyInfo> {
    return duchyInfos.associate { it: DuchyFlags ->
      it.duchyId to DuchyInfo(it.duchyTarget, it.duchyCertHost)
    }
  }

  /**
   * Loads the [Descriptors.Descriptor] for one model line's event template type, from its
   * descriptor blob URI.
   *
   * Descriptor sets are cached by blob URI: model lines sharing a descriptor blob URI only have it
   * downloaded and parsed once, regardless of how many distinct event template type names are
   * looked up within it.
   */
  private class EventDescriptorLoader(
    private val loadDescriptorSet: suspend (String) -> List<Descriptors.Descriptor>
  ) {
    private val descriptorSetsByUri = mutableMapOf<String, List<Descriptors.Descriptor>>()

    suspend fun load(
      descriptorBlobUri: String,
      eventTemplateTypeName: String,
    ): Descriptors.Descriptor {
      val descriptors =
        descriptorSetsByUri.getOrPut(descriptorBlobUri) { loadDescriptorSet(descriptorBlobUri) }
      return descriptors.firstOrNull { it.fullName == eventTemplateTypeName }
        ?: error("Descriptor not found for type: $eventTemplateTypeName")
    }

    /** All descriptors loaded so far, across every descriptor blob URI. */
    fun allDescriptors(): List<Descriptors.Descriptor> = descriptorSetsByUri.values.flatten()
  }

  /**
   * Builds the model line -> [ModelLineInfo] map for [modelLines].
   *
   * The VID index depends only on the population spec, not on which event descriptor a model line
   * selects from it, so [PopulationSpec] and [VidIndexMap] are cached by population-spec blob URI:
   * model lines sharing a population spec only have it downloaded, parsed, and indexed once. Event
   * descriptors are cached separately -- see [EventDescriptorLoader].
   *
   * Population-spec textproto parsing can require descriptors for message types packed in
   * google.protobuf.Any (e.g. event template attributes), so descriptors are loaded first and a
   * single [TypeRegistry] covering every loaded descriptor set is used to parse every population
   * spec. Extra entries in that registry are inert: a [TypeRegistry] is only consulted for the Any
   * fields a given population spec actually contains.
   *
   * @param modelLines model lines to build the map for. Defaults to the flags parsed from the
   *   command line.
   * @param loadDescriptorSet downloads and parses the descriptor set for a descriptor blob URI.
   *   Injectable for testing; defaults to loading from blob storage.
   * @param loadPopulationSpec downloads and parses the [PopulationSpec] for a population-spec blob
   *   URI, given a [TypeRegistry] for resolving Any-packed attributes. Injectable for testing;
   *   defaults to loading from blob storage.
   * @param buildVidIndexMap builds the [VidIndexMap] for a [PopulationSpec]. Injectable for
   *   testing; defaults to [ParallelInMemoryVidIndexMap.build], timed via [metrics].
   */
  internal suspend fun buildModelLineMap(
    modelLines: List<ModelLineFlags> = this.modelLines,
    loadDescriptorSet: suspend (String) -> List<Descriptors.Descriptor> =
      ::loadDescriptorSetFromBlob,
    loadPopulationSpec: suspend (String, TypeRegistry) -> PopulationSpec =
      ::loadPopulationSpecFromBlob,
    buildVidIndexMap: suspend (PopulationSpec) -> VidIndexMap = ::buildVidIndexMapWithMetrics,
  ): Map<String, ModelLineInfo> {
    val eventDescriptorLoader = EventDescriptorLoader(loadDescriptorSet)
    val eventDescriptorByModelLine: Map<String, Descriptors.Descriptor> =
      modelLines.associate { flags ->
        flags.modelLine to
          eventDescriptorLoader.load(
            flags.eventTemplateDescriptorBlobUri,
            flags.eventTemplateTypeName,
          )
      }

    val typeRegistry: TypeRegistry =
      TypeRegistry.newBuilder().add(eventDescriptorLoader.allDescriptors()).build()

    data class PopulationSpecResources(
      val populationSpec: PopulationSpec,
      val vidIndexMap: VidIndexMap,
    )
    val populationSpecResourcesByUri = mutableMapOf<String, PopulationSpecResources>()
    for (flags in modelLines) {
      populationSpecResourcesByUri.getOrPut(flags.populationSpecFileBlobUri) {
        val populationSpec = loadPopulationSpec(flags.populationSpecFileBlobUri, typeRegistry)
        PopulationSpecResources(populationSpec, buildVidIndexMap(populationSpec))
      }
    }

    return modelLines.associate { flags ->
      val resources = populationSpecResourcesByUri.getValue(flags.populationSpecFileBlobUri)
      flags.modelLine to
        ModelLineInfo(
          populationSpec = resources.populationSpec,
          vidIndexMap = resources.vidIndexMap,
          eventDescriptor = eventDescriptorByModelLine.getValue(flags.modelLine),
          localAlias = null,
        )
    }
  }

  /** Downloads and parses the descriptor set at [descriptorBlobUri] from blob storage. */
  private suspend fun loadDescriptorSetFromBlob(
    descriptorBlobUri: String
  ): List<Descriptors.Descriptor> {
    val eventDescriptorBytes =
      getResultsFulfillerConfigAsByteArray(googleProjectId, descriptorBlobUri)
    val fileDescriptorSet =
      DescriptorProtos.FileDescriptorSet.parseFrom(eventDescriptorBytes, EXTENSION_REGISTRY)
    return ProtoReflection.buildDescriptors(listOf(fileDescriptorSet), COMPILED_PROTOBUF_TYPES)
  }

  /**
   * Downloads and parses the [PopulationSpec] at [populationSpecBlobUri] from blob storage, using
   * [typeRegistry] to resolve any event template attribute messages packed in google.protobuf.Any
   * (e.g. a Person event template attribute on each SubPopulation).
   */
  private suspend fun loadPopulationSpecFromBlob(
    populationSpecBlobUri: String,
    typeRegistry: TypeRegistry,
  ): PopulationSpec {
    val configContent = getResultsFulfillerConfigAsByteArray(googleProjectId, populationSpecBlobUri)
    return configContent.inputStream().reader(Charsets.UTF_8).use { reader ->
      parseTextProto(reader, PopulationSpec.getDefaultInstance(), typeRegistry)
    }
  }

  /** Builds the [VidIndexMap] for [populationSpec], timed via [metrics]. */
  private suspend fun buildVidIndexMapWithMetrics(populationSpec: PopulationSpec): VidIndexMap {
    return metrics.vidIndexBuildDuration.measured {
      ParallelInMemoryVidIndexMap.build(populationSpec)
    }
  }

  companion object {
    /**
     * [Descriptors.FileDescriptor]s of protobuf types known at compile-time that may be loaded from
     * a [DescriptorProtos.FileDescriptorSet].
     */
    private val COMPILED_PROTOBUF_TYPES: Iterable<Descriptors.FileDescriptor> =
      (ProtoReflection.WELL_KNOWN_TYPES.asSequence()).asIterable()

    private val EXTENSION_REGISTRY =
      ExtensionRegistry.newInstance()
        .also { EventAnnotationsProto.registerAllExtensions(it) }
        .unmodifiable

    @JvmStatic fun main(args: Array<String>) = commandLineMain(ResultsFulfillerAppRunner(), args)
  }
}
