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

import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.Parser
import com.google.protobuf.TypeRegistry
import java.io.File
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.StorageParams
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import picocli.CommandLine

@CommandLine.Command(name = "results_fulfiller_app_runner")
class ResultsFulfillerAppRunner : Runnable {
  @CommandLine.Option(
    names = ["--edpa-tls-cert-file-path"],
    description = ["User's own TLS cert file path."],
    defaultValue = "",
  )
  lateinit var edpaCertFilePath: String
    private set

  @CommandLine.Option(
    names = ["--edpa-tls-key-file-path"],
    description = ["User's own TLS private key file path."],
    defaultValue = "",
  )
  lateinit var edpaPrivateKeyFilePath: String
    private set

  @CommandLine.Option(
    names = ["--secure-computation-cert-collection-file-path"],
    description = ["Trusted root Cert collection file path."],
    required = false,
  )
  var secureComputationCertCollectionFilePath: String? = null
    private set

  @CommandLine.Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target of the Kingdom public API server"],
    required = true,
  )
  private lateinit var kingdomPublicApiTarget: String

  @CommandLine.Option(
    names = ["--secure-computation-public-api-target"],
    description = ["gRPC target of the Secure Conmputation public API server"],
    required = true,
  )
  private lateinit var secureComputationPublicApiTarget: String

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
    names = ["--secure-computation-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the SecureComputation public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --secure-computation-public-api-target.",
      ],
    required = false,
  )
  private var secureComputationPublicApiCertHost: String? = null

  @CommandLine.Option(
    names = ["--kingdom-cert-collection-file-path"],
    description = ["Kingdom root collections file path"],
    required = true,
  )
  private lateinit var kingdomCertCollectionFilePath: String

  @CommandLine.Option(
    names = ["--subscription-id"],
    description = ["Subscription ID for the queue"],
    required = true,
  )
  private lateinit var subscriptionId: String

  @CommandLine.Option(
    names = ["--google-pub-sub-project-id"],
    description = ["Project ID of Google pub sub subscriber."],
    required = true,
  )
  lateinit var pubSubProjectId: String
    private set

  @CommandLine.Option(
    names = ["--event-template-metadata-type"],
    description =
      [
        "Serialized FileDescriptorSet for EventTemplate metadata types.",
        "This can be specified multiple times.",
      ],
    required = true,
  )
  private lateinit var eventTemplateDescriptorSetFiles: List<File>

  private val getImpressionsStorageConfig: (StorageParams) -> StorageConfig = { storageParams ->
    StorageConfig(projectId = storageParams.gcsProjectId)
  }

  override fun run() {
    val queueSubscriber = createQueueSubscriber()
    val parser = createWorkItemParser()

    // Get client certificates from server flags
    val edpaCertFile = File(edpaCertFilePath)
    val edpaPrivateKeyFile = File(edpaPrivateKeyFilePath)
    val secureComputationCertCollectionFile = File(secureComputationCertCollectionFilePath)
    val secureComputationClientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = edpaCertFile,
        privateKeyFile = edpaPrivateKeyFile,
        trustedCertCollectionFile = secureComputationCertCollectionFile,
      )

    // Build the mutual TLS channel for secure computation API
    val publicChannel =
      buildMutualTlsChannel(
        secureComputationPublicApiTarget,
        secureComputationClientCerts,
        secureComputationPublicApiCertHost,
      )
    val workItemsClient = WorkItemsGrpcKt.WorkItemsCoroutineStub(publicChannel)
    val workItemAttemptsClient = WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(publicChannel)
    val kingdomCertCollectionFile = File(kingdomCertCollectionFilePath)

    val requisitionStubFactory =
      RequisitionStubFactoryImpl(
        cmmsCertHost = kingdomPublicApiCertHost,
        cmmsTarget = kingdomPublicApiTarget,
        trustedCertCollection = kingdomCertCollectionFile,
      )

    val kmsClient = GcpKmsClient().withDefaultCredentials()

    val typeRegistry: TypeRegistry = buildTypeRegistry()

    val resultsFulfillerApp =
      ResultsFulfillerApp(
        subscriptionId = subscriptionId,
        queueSubscriber = queueSubscriber,
        parser = parser,
        workItemsClient = workItemsClient,
        workItemAttemptsClient = workItemAttemptsClient,
        requisitionStubFactory = requisitionStubFactory,
        kmsClient = kmsClient,
        typeRegistry = typeRegistry,
        getImpressionsMetadataStorageConfig = getImpressionsStorageConfig,
        getImpressionsStorageConfig = getImpressionsStorageConfig,
        getRequisitionsStorageConfig = getImpressionsStorageConfig,
      )

    runBlocking { resultsFulfillerApp.run() }
  }

  // @TODO(@marcopremier): Move this and `buildTypeRegistry` on common-jvm
  private fun loadFileDescriptorSets(
    files: Iterable<File>
  ): List<DescriptorProtos.FileDescriptorSet> {
    return files.map { file ->
      file.inputStream().use { input ->
//        DescriptorProtos.FileDescriptorSet.parseFrom(input, EXTENSION_REGISTRY)
        DescriptorProtos.FileDescriptorSet.parseFrom(input)
      }
    }
  }

  private fun buildTypeRegistry(): TypeRegistry {
    return TypeRegistry.newBuilder()
      .apply {
        add(COMPILED_PROTOBUF_TYPES.flatMap { it.messageTypes })
        if (::eventTemplateDescriptorSetFiles.isInitialized) {
          add(
            ProtoReflection.buildDescriptors(
              loadFileDescriptorSets(eventTemplateDescriptorSetFiles),
              COMPILED_PROTOBUF_TYPES,
            )
          )
        }
      }
      .build()
  }

  private fun createQueueSubscriber(): QueueSubscriber {
    logger.info("Creating DefaultGooglePubSubclient: ${pubSubProjectId}")
    val pubSubClient = DefaultGooglePubSubClient()
    return Subscriber(projectId = pubSubProjectId, googlePubSubClient = pubSubClient)
  }

  private fun createWorkItemParser(): Parser<WorkItem> {
    return WorkItem.parser()
  }

  companion object {

    /**
     * [Descriptors.FileDescriptor]s of protobuf types known at compile-time that may be loaded from
     * a [DescriptorProtos.FileDescriptorSet].
     */
    private val COMPILED_PROTOBUF_TYPES: Iterable<Descriptors.FileDescriptor> =
      (ProtoReflection.WELL_KNOWN_TYPES.asSequence() + ResultsFulfillerParams.getDescriptor().file)
        .asIterable()

//    private val EXTENSION_REGISTRY =
//      ExtensionRegistry.newInstance()
//        .also { EventAnnotationsProto.registerAllExtensions(it) }
//        .unmodifiable

    private val logger = Logger.getLogger(this::class.java.name)

    @JvmStatic fun main(args: Array<String>) = commandLineMain(ResultsFulfillerAppRunner(), args)
  }
}
