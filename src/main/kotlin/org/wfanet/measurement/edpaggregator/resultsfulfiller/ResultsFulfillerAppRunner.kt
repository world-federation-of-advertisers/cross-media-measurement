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
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.HeaderKt.TrusTeeKt.EnvelopeEncryptionKt.awsKmsParams
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig.getResultsFulfillerConfigAsByteArray
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfig
import org.wfanet.measurement.edpaggregator.BaseTeeAppRunner
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfillerMetrics.Companion.measured
import org.wfanet.measurement.edpaggregator.runBlockingWithTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.StorageParams
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.ParallelInMemoryVidIndexMap
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

  private val getImpressionsStorageConfig: (StorageParams) -> StorageConfig = { storageParams ->
    StorageConfig(projectId = storageParams.gcsProjectId)
  }

  override fun run() {
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
          if (edpConfig.kmsConfig.kmsType == EventDataProviderConfig.KmsConfig.KmsType.AWS) {
            awsKmsParams {
              roleArn = edpConfig.kmsConfig.awsRoleArn
              roleSession = edpConfig.kmsConfig.awsRoleSessionName
              region = edpConfig.kmsConfig.awsRegion
              audience = edpConfig.kmsConfig.awsAudience
            }
          } else {
            null
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

  private suspend fun buildModelLineMap(): Map<String, ModelLineInfo> {
    return modelLines.associate { it: ModelLineFlags ->
      val configContent: ByteArray =
        getResultsFulfillerConfigAsByteArray(googleProjectId, it.populationSpecFileBlobUri)
      val eventDescriptorBytes =
        getResultsFulfillerConfigAsByteArray(googleProjectId, it.eventTemplateDescriptorBlobUri)
      val fileDescriptorSet =
        DescriptorProtos.FileDescriptorSet.parseFrom(eventDescriptorBytes, EXTENSION_REGISTRY)
      val descriptors: List<Descriptors.Descriptor> =
        ProtoReflection.buildDescriptors(listOf(fileDescriptorSet), COMPILED_PROTOBUF_TYPES)
      val typeName = it.eventTemplateTypeName
      val eventDescriptor =
        descriptors.firstOrNull { it.fullName == typeName }
          ?: error("Descriptor not found for type: $typeName")
      // Build a TypeRegistry containing all descriptors from the supplied
      // FileDescriptorSet so that the PopulationSpec textproto can resolve any
      // event template attribute messages packed in google.protobuf.Any (e.g. a
      // Person event template attribute on each SubPopulation).
      val populationSpecTypeRegistry: TypeRegistry =
        TypeRegistry.newBuilder().add(descriptors).build()
      val populationSpec =
        configContent.inputStream().reader(Charsets.UTF_8).use { reader ->
          parseTextProto(reader, PopulationSpec.getDefaultInstance(), populationSpecTypeRegistry)
        }
      val vidIndexMap =
        metrics.vidIndexBuildDuration.measured { ParallelInMemoryVidIndexMap.build(populationSpec) }
      it.modelLine to
        ModelLineInfo(
          populationSpec = populationSpec,
          vidIndexMap = vidIndexMap,
          eventDescriptor = eventDescriptor,
          localAlias = null,
        )
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
