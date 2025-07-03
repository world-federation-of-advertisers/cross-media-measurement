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
import kotlinx.coroutines.runBlocking
import com.google.protobuf.Parser
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import picocli.CommandLine
import java.io.File
import java.util.logging.Logger
import com.google.protobuf.TypeRegistry
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.StorageParams
import org.wfanet.measurement.edpaggregator.StorageConfig

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
    required = true
  )
  private lateinit var kingdomPublicApiTarget: String

  @CommandLine.Option(
    names = ["--secure-computation-public-api-target"],
    description = ["gRPC target of the Secure Conmputation public API server"],
    required = true
  )
  private lateinit var secureComputationPublicApiTarget: String

  @CommandLine.Option(
    names = ["--kingdom-public-api-cert-host"],
    description = [
      "Expected hostname (DNS-ID) in the Kingdom public API server's TLS certificate.",
      "This overrides derivation of the TLS DNS-ID from --kingdom-public-api-target."
    ],
    required = false
  )
  private var kingdomPublicApiCertHost: String? = null

  @CommandLine.Option(
    names = ["--secure-computation-public-api-cert-host"],
    description = [
      "Expected hostname (DNS-ID) in the SecureComputation public API server's TLS certificate.",
      "This overrides derivation of the TLS DNS-ID from --secure-computation-public-api-target."
    ],
    required = false
  )
  private var secureComputationPublicApiCertHost: String? = null

  @CommandLine.Option(
    names = ["--kingdom-cert-collection-file-path"],
    description = ["Kingdom root collections file path"],
    required = true
  )
  private lateinit var kingdomCertCollectionFilePath: String


  @CommandLine.Option(
    names = ["--subscription-id"],
    description = ["Subscription ID for the queue"],
    required = true
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
    description = [
      "File path to a FileDescriptorSet containing your EventTemplate metadata types.",
    ],
    required = true
  )
  private fun setEventTemplateMetadataType(file: File) {
    val fileDescriptorSet = file.inputStream().use { input ->
      DescriptorProtos.FileDescriptorSet.parseFrom(input)
    }

    eventTemplateDescriptors = ProtoReflection.buildFileDescriptors(listOf(fileDescriptorSet))
  }

  lateinit var eventTemplateDescriptors: List<Descriptors.FileDescriptor>
    private set

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
    val secureComputationClientCerts = SigningCerts.fromPemFiles(
      certificateFile = edpaCertFile,
      privateKeyFile = edpaPrivateKeyFile,
      trustedCertCollectionFile = secureComputationCertCollectionFile
    )

    // Build the mutual TLS channel for secure computation API
    val publicChannel = buildMutualTlsChannel(
      secureComputationPublicApiTarget,
      secureComputationClientCerts,
      secureComputationPublicApiCertHost
    )
    val workItemsClient = WorkItemsGrpcKt.WorkItemsCoroutineStub(publicChannel)
    val workItemAttemptsClient = WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(publicChannel)
    val kingdomCertCollectionFile = File(kingdomCertCollectionFilePath)

    val requisitionStubFactory = RequisitionStubFactoryImpl(
      cmmsCertHost = kingdomPublicApiCertHost,
      cmmsTarget = kingdomPublicApiTarget,
      trustedCertCollection = kingdomCertCollectionFile,
    )

    val kmsClient = GcpKmsClient().withDefaultCredentials()

    val typeRegistryBuilder = TypeRegistry.newBuilder()
    eventTemplateDescriptors
      .flatMap { it.messageTypes }
      .forEach { typeRegistryBuilder.add(it) }
    typeRegistryBuilder.add(ResultsFulfillerParams.getDescriptor())
    val typeRegistry = typeRegistryBuilder.build()

    val resultsFulfillerApp = ResultsFulfillerApp(
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
      getRequisitionsStorageConfig = getImpressionsStorageConfig
    )

    runBlocking {
      resultsFulfillerApp.run()
    }
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
    private val logger = Logger.getLogger(this::class.java.name)
    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(ResultsFulfillerAppRunner(), args)
  }
}
