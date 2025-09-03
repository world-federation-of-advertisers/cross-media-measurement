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

import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient
import com.google.cloud.secretmanager.v1.SecretVersionName
import com.google.crypto.tink.KmsClient
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.Parser
import java.io.File
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.tink.GCloudWifCredentials
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig.getResultsFulfillerConfigAsByteArray
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig.getConfigAsProtoMessage
import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfigs
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.StorageParams
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.gcloud.kms.GCloudKmsClientFactory
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
    names = ["--trusted-cert-collection-secret-id"],
    description = ["Secret ID of trusted root collections file."],
    required = true,
  )
  private lateinit var trustedCertCollectionSecretId: String

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*", heading = "Duchy info\n")
  lateinit var duchyInfos: List<DuchyFlags>
    private set
  class DuchyFlags {
    @CommandLine.Option(
      names = ["--duchy-id"],
      required = true,
      description = ["Id of the duchy"],
    )
    lateinit var duchyId: String

    @CommandLine.Option(
      names = ["--duchy-target"],
      required = true,
      description = ["Target of the duchy"],
    )
    lateinit var duchyTarget: String

    @CommandLine.Option(
      names = ["--duchy-cert-host"],
      required = true,
      description = ["Duchy cert host"],
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

  private lateinit var kmsClientsMap: MutableMap<String, KmsClient>

  private val getImpressionsStorageConfig: (StorageParams) -> StorageConfig = { storageParams ->
    StorageConfig(projectId = storageParams.gcsProjectId)
  }

  override fun run() {

    // Pull certificates needed to operate from Google Secrets.
    saveEdpaCerts()
    saveEdpsCerts()
    // Create KMS clients for EDPs
    createKmsClients()

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
    val trustedCertCollectionFile = File(TRUSTED_ROOT_CA_FILE_PATH)

    val duchiesMap = buildDuchyMap()

    val requisitionStubFactory =
      RequisitionStubFactoryImpl(
        cmmsCertHost = kingdomPublicApiCertHost,
        cmmsTarget = kingdomPublicApiTarget,
        trustedCertCollection = trustedCertCollectionFile,
        duchies = duchiesMap,
      )

    val modelLinesMap = runBlocking { buildModelLineMap() }

    val resultsFulfillerApp =
      ResultsFulfillerApp(
        subscriptionId = subscriptionId,
        queueSubscriber = queueSubscriber,
        parser = parser,
        workItemsClient = workItemsClient,
        workItemAttemptsClient = workItemAttemptsClient,
        requisitionStubFactory = requisitionStubFactory,
        kmsClients = kmsClientsMap,
        getImpressionsMetadataStorageConfig = getImpressionsStorageConfig,
        getImpressionsStorageConfig = getImpressionsStorageConfig,
        getRequisitionsStorageConfig = getImpressionsStorageConfig,
        modelLineInfoMap = modelLinesMap,
      )

    runBlocking { resultsFulfillerApp.run() }
  }

  fun createKmsClients() {

    kmsClientsMap = mutableMapOf()

    edpsConfig.eventDataProviderConfigList.forEach { edpConfig ->
      val kmsConfig =
        GCloudWifCredentials(
          audience = edpConfig.kmsConfig.kmsAudience,
          subjectTokenType = SUBJECT_TOKEN_TYPE,
          tokenUrl = TOKEN_URL,
          credentialSourceFilePath = CREDENTIAL_SOURCE_FILE_PATH,
          serviceAccountImpersonationUrl =
            EDP_TARGET_SERVICE_ACCOUNT_FORMAT.format(edpConfig.kmsConfig.serviceAccount),
        )

      val kmsClient = GCloudKmsClientFactory().getKmsClient(kmsConfig)

      kmsClientsMap[edpConfig.dataProvider] = kmsClient
    }
  }

  fun buildDuchyMap(): Map<String, DuchyInfo> {
    return duchyInfos.associate { it: DuchyFlags ->
      it.duchyId to DuchyInfo(it.duchyTarget, it.duchyCertHost)
    }
  }

  suspend fun buildModelLineMap(): Map<String, ModelLineInfo> {
    return modelLines.associate { it: ModelLineFlags ->
      val configContent: ByteArray =
        getResultsFulfillerConfigAsByteArray(googleProjectId, it.populationSpecFileBlobUri)
      val populationSpec =
        configContent.inputStream().reader(Charsets.UTF_8).use { reader ->
          parseTextProto(reader, PopulationSpec.getDefaultInstance())
        }
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
      it.modelLine to
        ModelLineInfo(
          populationSpec = populationSpec,
          vidIndexMap = InMemoryVidIndexMap.build(populationSpec),
          eventDescriptor = eventDescriptor,
        )
    }
  }

  fun saveEdpaCerts() {
    val edpaCert = accessSecretBytes(googleProjectId, edpaCertSecretId, SECRET_VERSION)
    saveByteArrayToFile(edpaCert, EDPA_TLS_CERT_FILE_PATH)
    val edpaPrivateKey = accessSecretBytes(googleProjectId, edpaPrivateKeySecretId, SECRET_VERSION)
    saveByteArrayToFile(edpaPrivateKey, EDPA_TLS_KEY_FILE_PATH)
    val secureComputationRootCa =
      accessSecretBytes(googleProjectId, secureComputationCertCollectionSecretId, SECRET_VERSION)
    saveByteArrayToFile(secureComputationRootCa, SECURE_COMPUTATION_ROOT_CA_FILE_PATH)
    val trustedRootCa =
      accessSecretBytes(googleProjectId, trustedCertCollectionSecretId, SECRET_VERSION)
    saveByteArrayToFile(trustedRootCa, TRUSTED_ROOT_CA_FILE_PATH)
  }

  fun saveEdpsCerts() {
    edpsConfig.eventDataProviderConfigList.forEach { edpConfig ->
      val edpCertDer =
        accessSecretBytes(
          googleProjectId,
          edpConfig.consentSignalingConfig.certDerSecretId,
          SECRET_VERSION,
        )
      saveByteArrayToFile(edpCertDer, edpConfig.consentSignalingConfig.certDerLocalPath)
      val edpPrivateDer =
        accessSecretBytes(
          googleProjectId,
          edpConfig.consentSignalingConfig.encPrivateDerSecretId,
          SECRET_VERSION,
        )
      saveByteArrayToFile(edpPrivateDer, edpConfig.consentSignalingConfig.encPrivateDerLocalPath)
      val edpEncPrivate =
        accessSecretBytes(
          googleProjectId,
          edpConfig.consentSignalingConfig.encPrivateSecretId,
          SECRET_VERSION,
        )
      saveByteArrayToFile(edpEncPrivate, edpConfig.consentSignalingConfig.encPrivateLocalPath)
      val edpTlsKey =
        accessSecretBytes(googleProjectId, edpConfig.tlsConfig.tlsKeySecretId, SECRET_VERSION)
      saveByteArrayToFile(edpTlsKey, edpConfig.tlsConfig.tlsKeyLocalPath)
      val edpTlsPem =
        accessSecretBytes(googleProjectId, edpConfig.tlsConfig.tlsPemSecretId, SECRET_VERSION)
      saveByteArrayToFile(edpTlsPem, edpConfig.tlsConfig.tlsPemLocalPath)
    }
  }

  fun saveByteArrayToFile(bytes: ByteArray, path: String) {
    val file = File(path)
    file.parentFile?.mkdirs()
    file.writeBytes(bytes)
  }

  fun accessSecretBytes(projectId: String, secretId: String, version: String): ByteArray {
    return SecretManagerServiceClient.create().use { client ->
      val secretVersionName = SecretVersionName.of(projectId, secretId, version)
      val request =
        AccessSecretVersionRequest.newBuilder().setName(secretVersionName.toString()).build()

      val response = client.accessSecretVersion(request)
      response.payload.data.toByteArray()
    }
  }

  private fun createQueueSubscriber(): QueueSubscriber {
    logger.info("Creating DefaultGooglePubSubclient: ${googleProjectId}.")
    val pubSubClient = DefaultGooglePubSubClient()
    return Subscriber(projectId = googleProjectId, googlePubSubClient = pubSubClient)
  }

  private fun createWorkItemParser(): Parser<WorkItem> {
    return WorkItem.parser()
  }

  companion object {

    private val logger = Logger.getLogger(this::class.java.name)

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

    private const val SECRET_VERSION = "latest"
    private const val EDPA_TLS_CERT_FILE_PATH = "/tmp/edpa_certs/edpa_tee_app_tls.pem"
    private const val EDPA_TLS_KEY_FILE_PATH = "/tmp/edpa_certs/edpa_tee_app_tls.key"
    private const val SECURE_COMPUTATION_ROOT_CA_FILE_PATH =
      "/tmp/edpa_certs/secure_computation_root.pem"
    private const val TRUSTED_ROOT_CA_FILE_PATH = "/tmp/edpa_certs/trusted_root.pem"

    private const val SUBJECT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt"
    private const val TOKEN_URL = "https://sts.googleapis.com/v1/token"
    private const val CREDENTIAL_SOURCE_FILE_PATH =
      "/run/container_launcher/attestation_verifier_claims_token"
    private const val EDP_TARGET_SERVICE_ACCOUNT_FORMAT =
      "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken"

    private const val EVENT_DATA_PROVIDER_CONFIGS_BLOB_KEY = "event-data-provider-configs.textproto"
    private val edpsConfig by lazy {
      runBlocking {
        getConfigAsProtoMessage(
          EVENT_DATA_PROVIDER_CONFIGS_BLOB_KEY,
          EventDataProviderConfigs.getDefaultInstance(),
        )
      }
    }

    @JvmStatic fun main(args: Array<String>) = commandLineMain(ResultsFulfillerAppRunner(), args)
  }
}
