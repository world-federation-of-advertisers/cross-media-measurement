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
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest
import com.google.cloud.secretmanager.v1.SecretVersionName

@CommandLine.Command(name = "results_fulfiller_app_runner")
class ResultsFulfillerAppRunner : Runnable {
  @CommandLine.Option(
    names = ["--edpa-tls-cert-secret-id"],
    description = ["Secret ID of EDPA TLS cert file."],
    defaultValue = "",
  )
  lateinit var edpaCertSecretId: String
    private set

  @CommandLine.Option(
    names = ["--edpa-tls-key-secret-id"],
    description = ["Secret ID of EDPA TLS key file."],
    defaultValue = "",
  )
  lateinit var edpaPrivateKeySecretId: String
    private set

  @CommandLine.Option(
    names = ["--secure-computation-cert-collection-secret-id"],
    description = ["Secret ID of SecureComputation Trusted root Cert collection file."],
    required = true,
  )
  lateinit var secureComputationCertCollectionSecretId: String
    private set

  @CommandLine.Option(
    names = ["--kingdom-cert-collection-secret-id"],
    description = ["Secret ID of Kingdom root collections file."],
    required = true,
  )
  private lateinit var kingdomCertCollectionSecretId: String

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
    names = ["--subscription-id"],
    description = ["Subscription ID for the queue"],
    required = true,
  )
  private lateinit var subscriptionId: String

  @CommandLine.Option(
    names = ["--google-project-id"],
    description = ["Project ID of EDP Aggregator."],
    required = true,
  )
  lateinit var googleProjectId: String
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

    saveEdpaCerts()

    val queueSubscriber = createQueueSubscriber()
    val parser = createWorkItemParser()

    // Get client certificates from server flags
    val edpaCertFile = File(EDPA_TLS_CERT_FILE_PATH)
    val edpaPrivateKeyFile = File(EDPA_TLS_KEY_FILE_PATH)
    val secureComputationCertCollectionFile = File(SECURE_COMPUTATION_ROOT_CA_FILE_PATH)
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
    val kingdomCertCollectionFile = File(KINGDOM_ROOT_CA_FILE_PATH)

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
        DescriptorProtos.FileDescriptorSet.parseFrom(input, EXTENSION_REGISTRY)
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

  fun saveEdpaCerts() {
    val edpaCert = accessSecretBytes(googleProjectId, edpaCertSecretId, SECRET_VERSION)
    saveSecretToFile(edpaCert, EDPA_TLS_CERT_FILE_PATH)
    val edpaPrivateKey = accessSecretBytes(googleProjectId, edpaPrivateKeySecretId, SECRET_VERSION)
    saveSecretToFile(edpaPrivateKey, EDPA_TLS_KEY_FILE_PATH)
    val secureComputationRootCa = accessSecretBytes(googleProjectId, secureComputationCertCollectionSecretId, SECRET_VERSION)
    saveSecretToFile(secureComputationRootCa, SECURE_COMPUTATION_ROOT_CA_FILE_PATH)
    val kingdomRootCa = accessSecretBytes(googleProjectId, kingdomCertCollectionSecretId, SECRET_VERSION)
    saveSecretToFile(kingdomRootCa, KINGDOM_ROOT_CA_FILE_PATH)
  }

  fun saveSecretToFile(bytes: ByteArray, path: String) {
    val file = File(path)
    file.parentFile?.mkdirs()
    file.writeBytes(bytes)
  }

  fun accessSecretBytes(projectId: String, secretId: String, version: String): ByteArray {
    return SecretManagerServiceClient.create().use { client ->
      val secretVersionName = SecretVersionName.of(projectId, secretId, version)
      val request = AccessSecretVersionRequest.newBuilder()
        .setName(secretVersionName.toString())
        .build()

      val response = client.accessSecretVersion(request)
      response.payload.data.toByteArray()
    }
  }

  private fun createQueueSubscriber(): QueueSubscriber {
    logger.info("Creating DefaultGooglePubSubclient: ${googleProjectId}")
    val pubSubClient = DefaultGooglePubSubClient()
    return Subscriber(projectId = googleProjectId, googlePubSubClient = pubSubClient)
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

    private val EXTENSION_REGISTRY =
      ExtensionRegistry.newInstance()
        .also { EventAnnotationsProto.registerAllExtensions(it) }
        .unmodifiable

    private val logger = Logger.getLogger(this::class.java.name)

    private const val SECRET_VERSION = "latest"
    private const val EDPA_TLS_CERT_FILE_PATH = "/tmp/edpa_certs/edpa_tee_app_tls.pem"
    private const val EDPA_TLS_KEY_FILE_PATH = "/tmp/edpa_certs/edpa_tee_app_tls.key"
    private const val SECURE_COMPUTATION_ROOT_CA_FILE_PATH = "/tmp/edpa_certs/secure_computation_root.pem"
    private const val KINGDOM_ROOT_CA_FILE_PATH = "/tmp/edpa_certs/kingdom_root.pem"

    @JvmStatic fun main(args: Array<String>) = commandLineMain(ResultsFulfillerAppRunner(), args)
  }
}
