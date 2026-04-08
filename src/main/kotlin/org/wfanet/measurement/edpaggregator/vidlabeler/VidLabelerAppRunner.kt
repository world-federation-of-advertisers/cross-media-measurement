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

import com.google.cloud.logging.LoggingHandler
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient
import com.google.cloud.secretmanager.v1.SecretVersionName
import com.google.crypto.tink.KmsClient
import io.grpc.ClientInterceptors
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.io.File
import java.util.logging.Level
import java.util.logging.LogManager
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.tink.GCloudToAwsWifCredentials
import org.wfanet.measurement.common.crypto.tink.GCloudWifCredentials
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfig
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigs
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams.StorageParams
import org.wfanet.measurement.gcloud.kms.GCloudKmsClientFactory
import org.wfanet.measurement.gcloud.kms.GCloudToAwsKmsClientFactory
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.GooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import picocli.CommandLine

@CommandLine.Command(name = "vid_labeler_app_runner")
class VidLabelerAppRunner : Runnable {
  private val grpcTelemetry by lazy { GrpcTelemetry.create(Instrumentation.openTelemetry) }

  @CommandLine.Option(
    names = ["--edpa-tls-cert-secret-id"],
    description = ["Secret ID of EDPA TLS cert file."],
    required = true,
  )
  private lateinit var edpaCertSecretId: String

  @CommandLine.Option(
    names = ["--edpa-tls-cert-file-path"],
    description = ["Local path where the --edpa-tls-cert-secret-id secret is stored."],
    required = true,
  )
  private lateinit var edpaCertFilePath: String

  @CommandLine.Option(
    names = ["--edpa-tls-key-secret-id"],
    description = ["Secret ID of EDPA TLS key file."],
    required = true,
  )
  private lateinit var edpaPrivateKeySecretId: String

  @CommandLine.Option(
    names = ["--edpa-tls-key-file-path"],
    description = ["Local path where the --edpa-tls-key-secret-id secret is stored."],
    required = true,
  )
  private lateinit var edpaPrivateKeyFilePath: String

  @CommandLine.Option(
    names = ["--secure-computation-cert-collection-secret-id"],
    description = ["Secret ID of SecureComputation Trusted root Cert collection file."],
    required = true,
  )
  private lateinit var secureComputationCertCollectionSecretId: String

  @CommandLine.Option(
    names = ["--secure-computation-cert-collection-file-path"],
    description =
      ["Local path where the --secure-computation-cert-collection-secret-id secret is stored."],
    required = true,
  )
  private lateinit var secureComputationCertCollectionFilePath: String

  @CommandLine.Option(
    names = ["--metadata-storage-cert-collection-secret-id"],
    description = ["Secret ID of Metadata Storage Trusted root Cert collection file."],
    required = true,
  )
  private lateinit var metadataStorageCertCollectionSecretId: String

  @CommandLine.Option(
    names = ["--metadata-storage-cert-collection-file-path"],
    description =
      ["Local path where the --metadata-storage-cert-collection-secret-id secret is stored."],
    required = true,
  )
  private lateinit var metadataStorageCertCollectionFilePath: String

  @CommandLine.Option(
    names = ["--secure-computation-public-api-target"],
    description = ["gRPC target of the Secure Computation public API server"],
    required = true,
  )
  private lateinit var secureComputationPublicApiTarget: String

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
    names = ["--metadata-storage-public-api-target"],
    description = ["gRPC target of the Metadata Storage public API server"],
    required = true,
  )
  private lateinit var metadataStoragePublicApiTarget: String

  @CommandLine.Option(
    names = ["--metadata-storage-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Metadata Storage public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --metadata-storage-public-api-target.",
      ],
    required = false,
  )
  private var metadataStoragePublicApiCertHost: String? = null

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
  private lateinit var googleProjectId: String

  private lateinit var rawImpressionsKmsClients: MutableMap<String, KmsClient>
  private lateinit var vidLabeledImpressionsKmsClients: MutableMap<String, KmsClient>

  private val getStorageConfig: (StorageParams) -> StorageConfig = { storageParams ->
    StorageConfig(projectId = storageParams.gcsProjectId)
  }

  override fun run() {
    saveCerts()
    createKmsClients()

    val pubSubClient = DefaultGooglePubSubClient()
    val queueSubscriber = createQueueSubscriber(pubSubClient)

    // Build the mutual TLS channel for Secure Computation API.
    val secureComputationClientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = File(edpaCertFilePath),
        privateKeyFile = File(edpaPrivateKeyFilePath),
        trustedCertCollectionFile = File(secureComputationCertCollectionFilePath),
      )
    val secureComputationPublicChannel =
      ClientInterceptors.intercept(
        buildMutualTlsChannel(
          secureComputationPublicApiTarget,
          secureComputationClientCerts,
          secureComputationPublicApiCertHost,
        ),
        grpcTelemetry.newClientInterceptor(),
      )
    val workItemsClient = WorkItemsGrpcKt.WorkItemsCoroutineStub(secureComputationPublicChannel)
    val workItemAttemptsClient =
      WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(secureComputationPublicChannel)

    val vidLabelerApp =
      VidLabelerApp(
        subscriptionId = subscriptionId,
        queueSubscriber = queueSubscriber,
        parser = WorkItem.parser(),
        workItemsClient = workItemsClient,
        workItemAttemptsClient = workItemAttemptsClient,
        rawImpressionsKmsClient = rawImpressionsKmsClients,
        vidLabeledImpressionsKmsClient = vidLabeledImpressionsKmsClients,
        getStorageConfig = getStorageConfig,
      )

    runBlockingWithTelemetry { vidLabelerApp.run() }
  }

  /** Creates KMS clients for each EDP from [VidLabelingConfigs]. */
  fun createKmsClients() {
    rawImpressionsKmsClients = mutableMapOf()
    vidLabeledImpressionsKmsClients = mutableMapOf()

    for (config in vidLabelingConfigs.configsList) {
      rawImpressionsKmsClients[config.dataProvider] =
        buildKmsClient(config.rawImpressionsKmsConfig)
      vidLabeledImpressionsKmsClients[config.dataProvider] =
        buildKmsClient(config.vidLabeledImpressionsKmsConfig)
    }
  }

  private fun buildKmsClient(kmsConfig: VidLabelingConfig.KmsConfig): KmsClient {
    return when (kmsConfig.providerConfigCase) {
      VidLabelingConfig.KmsConfig.ProviderConfigCase.GCP -> {
        val gcp = kmsConfig.gcp
        val credentials =
          GCloudWifCredentials(
            audience = kmsConfig.kmsAudience,
            subjectTokenType = SUBJECT_TOKEN_TYPE,
            tokenUrl = TOKEN_URL,
            credentialSourceFilePath = CREDENTIAL_SOURCE_FILE_PATH,
            serviceAccountImpersonationUrl =
              EDP_TARGET_SERVICE_ACCOUNT_FORMAT.format(gcp.serviceAccount),
          )
        GCloudKmsClientFactory().getKmsClient(credentials)
      }
      VidLabelingConfig.KmsConfig.ProviderConfigCase.AWS -> {
        val aws = kmsConfig.aws
        val credentials =
          GCloudToAwsWifCredentials(
            gcloudAudience = kmsConfig.kmsAudience,
            subjectTokenType = SUBJECT_TOKEN_TYPE,
            tokenUrl = TOKEN_URL,
            credentialSourceFilePath = CREDENTIAL_SOURCE_FILE_PATH,
            serviceAccountImpersonationUrl =
              EDP_TARGET_SERVICE_ACCOUNT_FORMAT.format(aws.serviceAccount),
            roleArn = aws.roleArn,
            roleSessionName = aws.roleSessionName,
            region = aws.region,
            awsAudience = aws.audience,
          )
        GCloudToAwsKmsClientFactory().getKmsClient(credentials)
      }
      VidLabelingConfig.KmsConfig.ProviderConfigCase.PROVIDERCONFIG_NOT_SET,
      null ->
        error("KmsConfig provider must be set")
    }
  }

  /** Pulls EDPA certificates from Google Secret Manager and saves them to local files. */
  fun saveCerts() {
    saveSecret(edpaCertSecretId, edpaCertFilePath)
    saveSecret(edpaPrivateKeySecretId, edpaPrivateKeyFilePath)
    saveSecret(secureComputationCertCollectionSecretId, secureComputationCertCollectionFilePath)
    saveSecret(metadataStorageCertCollectionSecretId, metadataStorageCertCollectionFilePath)
  }

  private fun saveSecret(secretId: String, filePath: String) {
    val bytes = accessSecretBytes(googleProjectId, secretId)
    val file = File(filePath)
    file.parentFile?.mkdirs()
    file.writeBytes(bytes)
  }

  private fun accessSecretBytes(projectId: String, secretId: String): ByteArray {
    return SecretManagerServiceClient.create().use { client ->
      val secretVersionName = SecretVersionName.of(projectId, secretId, SECRET_VERSION)
      val request =
        AccessSecretVersionRequest.newBuilder().setName(secretVersionName.toString()).build()
      client.accessSecretVersion(request).payload.data.toByteArray()
    }
  }

  private fun createQueueSubscriber(pubSubClient: GooglePubSubClient): QueueSubscriber {
    logger.info("Creating Subscriber for project: $googleProjectId, subscription: $subscriptionId")
    return Subscriber(
      projectId = googleProjectId,
      googlePubSubClient = pubSubClient,
      maxMessages = 1,
      pullIntervalMillis = 100,
      ackDeadlineExtensionIntervalSeconds = 60,
      ackDeadlineExtensionSeconds = 600,
      blockingContext = Dispatchers.IO,
    )
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)

    private const val SECRET_VERSION = "latest"
    private const val SUBJECT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt"
    private const val TOKEN_URL = "https://sts.googleapis.com/v1/token"
    private const val CREDENTIAL_SOURCE_FILE_PATH =
      "/run/container_launcher/attestation_verifier_claims_token"
    private const val EDP_TARGET_SERVICE_ACCOUNT_FORMAT =
      "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken"

    private const val VID_LABELING_CONFIGS_BLOB_KEY = "vid-labeling-configs.textproto"
    private val vidLabelingConfigs: VidLabelingConfigs by lazy {
      runBlockingWithTelemetry {
        EdpAggregatorConfig.getConfigAsProtoMessage(
          VID_LABELING_CONFIGS_BLOB_KEY,
          VidLabelingConfigs.getDefaultInstance(),
        )
      }
    }

    init {
      configureCloudLoggingHandler()
      EdpaTelemetry.ensureInitialized()
    }

    private fun configureCloudLoggingHandler() {
      try {
        val rootLogger = LogManager.getLogManager().getLogger("")
        if (rootLogger.handlers.none { it is LoggingHandler }) {
          val otelServiceName = System.getenv("OTEL_SERVICE_NAME")
          val handler =
            if (otelServiceName.isNullOrBlank()) {
              LoggingHandler()
            } else {
              LoggingHandler(otelServiceName)
            }
          rootLogger.addHandler(handler)
          logger.info("Configured Google Cloud Logging handler for java.util.logging")
        }
      } catch (e: Exception) {
        logger.log(Level.WARNING, "Failed to configure Google Cloud Logging handler", e)
      }
    }

    @JvmStatic fun main(args: Array<String>) = commandLineMain(VidLabelerAppRunner(), args)
  }
}

private fun <T> runBlockingWithTelemetry(block: suspend () -> T): T {
  return runBlocking(Context.current().asContextElement()) { block() }
}
