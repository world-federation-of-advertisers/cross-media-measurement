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

package org.wfanet.measurement.edpaggregator.vidrankbuilder

import com.google.cloud.logging.LoggingHandler
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient
import com.google.cloud.secretmanager.v1.SecretVersionName
import com.google.crypto.tink.KmsClient
import com.google.protobuf.Parser
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
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig.getConfigAsProtoMessage
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfig
import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfigs
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams.StorageParams
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

/**
 * CLI entry point for the [VidRankBuilderApp] Phase-1 TEE container.
 *
 * Pulls EDPA mTLS material from Secret Manager, builds per-`DataProvider`
 * [KmsClient]s from the EDPA-level `event-data-provider-configs.textproto`
 * via Workload Identity Federation, opens a mutual-TLS channel to the
 * Secure Computation control plane for `WorkItem` / `WorkItemAttempt` writes,
 * subscribes to the Phase-1 Pub/Sub topic, and hands everything to
 * [VidRankBuilderApp.run].
 */
@CommandLine.Command(name = "vid_rank_builder_app_runner")
class VidRankBuilderAppRunner : Runnable {
  private val grpcTelemetry by lazy { GrpcTelemetry.create(Instrumentation.openTelemetry) }

  @CommandLine.Option(
    names = ["--edpa-tls-cert-secret-id"],
    description = ["Secret ID of EDPA TLS cert file."],
    required = true,
  )
  lateinit var edpaCertSecretId: String
    private set

  @CommandLine.Option(
    names = ["--edpa-tls-cert-file-path"],
    description = ["Local path where the --edpa-tls-cert-secret-id secret is stored."],
    required = true,
  )
  lateinit var edpaCertFilePath: String
    private set

  @CommandLine.Option(
    names = ["--edpa-tls-key-secret-id"],
    description = ["Secret ID of EDPA TLS key file."],
    required = true,
  )
  lateinit var edpaPrivateKeySecretId: String
    private set

  @CommandLine.Option(
    names = ["--edpa-tls-key-file-path"],
    description = ["Local path where the --edpa-tls-key-secret-id secret is stored."],
    required = true,
  )
  lateinit var edpaPrivateKeyFilePath: String
    private set

  @CommandLine.Option(
    names = ["--secure-computation-cert-collection-secret-id"],
    description = ["Secret ID of SecureComputation trusted root cert collection file."],
    required = true,
  )
  lateinit var secureComputationCertCollectionSecretId: String
    private set

  @CommandLine.Option(
    names = ["--secure-computation-cert-collection-file-path"],
    description =
      ["Local path where the --secure-computation-cert-collection-secret-id secret is stored."],
    required = true,
  )
  lateinit var secureComputationCertCollectionFilePath: String
    private set

  @CommandLine.Option(
    names = ["--secure-computation-public-api-target"],
    description = ["gRPC target of the Secure Computation public API server."],
    required = true,
  )
  private lateinit var secureComputationPublicApiTarget: String

  @CommandLine.Option(
    names = ["--secure-computation-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Secure Computation public API server's TLS certificate.",
        "Overrides derivation of the TLS DNS-ID from --secure-computation-public-api-target.",
      ],
    required = false,
  )
  private var secureComputationPublicApiCertHost: String? = null

  @CommandLine.Option(
    names = ["--subscription-id"],
    description = ["Subscription ID for the VidRankBuilder Pub/Sub queue."],
    required = true,
  )
  private lateinit var subscriptionId: String

  @CommandLine.Option(
    names = ["--google-project-id"],
    description = ["Google Cloud project ID of the EDP Aggregator."],
    required = true,
  )
  lateinit var googleProjectId: String
    private set

  private lateinit var kmsClientsMap: MutableMap<String, KmsClient>

  private val getStorageConfig: (StorageParams) -> StorageConfig = { storageParams ->
    StorageConfig(projectId = storageParams.gcsProjectId)
  }

  override fun run() {
    saveEdpaCerts()
    createKmsClients()

    val pubSubClient = DefaultGooglePubSubClient()
    val queueSubscriber = createQueueSubscriber(pubSubClient)
    val parser: Parser<WorkItem> = WorkItem.parser()

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

    val vidRankBuilderApp =
      VidRankBuilderApp(
        subscriptionId = subscriptionId,
        queueSubscriber = queueSubscriber,
        parser = parser,
        workItemsClient = workItemsClient,
        workItemAttemptsClient = workItemAttemptsClient,
        kmsClients = kmsClientsMap,
        getRawImpressionStorageConfig = getStorageConfig,
        getSubpoolMapStorageConfig = getStorageConfig,
        getVidRankMapStorageConfig = getStorageConfig,
      )

    runBlockingWithTelemetry { vidRankBuilderApp.run() }
  }

  private fun createKmsClients() {
    kmsClientsMap = mutableMapOf()
    edpsConfig.eventDataProviderConfigList.forEach { edpConfig ->
      val kmsClient =
        when (edpConfig.kmsConfig.kmsType) {
          EventDataProviderConfig.KmsConfig.KmsType.AWS -> {
            val gcloudToAwsConfig =
              GCloudToAwsWifCredentials(
                gcloudAudience = edpConfig.kmsConfig.kmsAudience,
                subjectTokenType = SUBJECT_TOKEN_TYPE,
                tokenUrl = TOKEN_URL,
                credentialSourceFilePath = CREDENTIAL_SOURCE_FILE_PATH,
                serviceAccountImpersonationUrl =
                  EDP_TARGET_SERVICE_ACCOUNT_FORMAT.format(edpConfig.kmsConfig.serviceAccount),
                roleArn = edpConfig.kmsConfig.awsRoleArn,
                roleSessionName = edpConfig.kmsConfig.awsRoleSessionName,
                region = edpConfig.kmsConfig.awsRegion,
                awsAudience = edpConfig.kmsConfig.awsAudience,
              )
            GCloudToAwsKmsClientFactory().getKmsClient(gcloudToAwsConfig)
          }
          EventDataProviderConfig.KmsConfig.KmsType.GCP -> {
            val gcpConfig =
              GCloudWifCredentials(
                audience = edpConfig.kmsConfig.kmsAudience,
                subjectTokenType = SUBJECT_TOKEN_TYPE,
                tokenUrl = TOKEN_URL,
                credentialSourceFilePath = CREDENTIAL_SOURCE_FILE_PATH,
                serviceAccountImpersonationUrl =
                  EDP_TARGET_SERVICE_ACCOUNT_FORMAT.format(edpConfig.kmsConfig.serviceAccount),
              )
            GCloudKmsClientFactory().getKmsClient(gcpConfig)
          }
          EventDataProviderConfig.KmsConfig.KmsType.KMS_TYPE_UNSPECIFIED,
          EventDataProviderConfig.KmsConfig.KmsType.UNRECOGNIZED ->
            error("Unsupported KMS type: ${edpConfig.kmsConfig.kmsType}")
        }
      kmsClientsMap[edpConfig.dataProvider] = kmsClient
    }
  }

  private fun saveEdpaCerts() {
    saveByteArrayToFile(
      accessSecretBytes(googleProjectId, edpaCertSecretId, SECRET_VERSION),
      edpaCertFilePath,
    )
    saveByteArrayToFile(
      accessSecretBytes(googleProjectId, edpaPrivateKeySecretId, SECRET_VERSION),
      edpaPrivateKeyFilePath,
    )
    saveByteArrayToFile(
      accessSecretBytes(googleProjectId, secureComputationCertCollectionSecretId, SECRET_VERSION),
      secureComputationCertCollectionFilePath,
    )
  }

  private fun saveByteArrayToFile(bytes: ByteArray, path: String) {
    val file = File(path)
    file.parentFile?.mkdirs()
    file.writeBytes(bytes)
  }

  private fun accessSecretBytes(projectId: String, secretId: String, version: String): ByteArray {
    return SecretManagerServiceClient.create().use { client ->
      val secretVersionName = SecretVersionName.of(projectId, secretId, version)
      val request =
        AccessSecretVersionRequest.newBuilder().setName(secretVersionName.toString()).build()
      val response = client.accessSecretVersion(request)
      response.payload.data.toByteArray()
    }
  }

  private fun createQueueSubscriber(pubSubClient: GooglePubSubClient): QueueSubscriber {
    logger.info("Creating Subscriber for project: $googleProjectId, subscription: $subscriptionId")
    val subscriber =
      Subscriber(
        projectId = googleProjectId,
        googlePubSubClient = pubSubClient,
        maxMessages = 1,
        pullIntervalMillis = 100,
        ackDeadlineExtensionIntervalSeconds = 60,
        ackDeadlineExtensionSeconds = 600,
        blockingContext = Dispatchers.IO,
      )
    logger.info("Subscriber created successfully")
    return subscriber
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
    private const val EVENT_DATA_PROVIDER_CONFIGS_BLOB_KEY = "event-data-provider-configs.textproto"

    private val edpsConfig by lazy {
      runBlockingWithTelemetry {
        getConfigAsProtoMessage(
          EVENT_DATA_PROVIDER_CONFIGS_BLOB_KEY,
          EventDataProviderConfigs.getDefaultInstance(),
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
            if (otelServiceName.isNullOrBlank()) LoggingHandler()
            else LoggingHandler(otelServiceName)
          rootLogger.addHandler(handler)
          logger.info(
            if (otelServiceName.isNullOrBlank()) {
              "Configured Google Cloud Logging handler for java.util.logging"
            } else {
              "Configured Google Cloud Logging handler for java.util.logging (logName=$otelServiceName)"
            }
          )
        }
      } catch (e: Exception) {
        logger.log(Level.WARNING, "Failed to configure Google Cloud Logging handler", e)
      }
    }

    @JvmStatic fun main(args: Array<String>) = commandLineMain(VidRankBuilderAppRunner(), args)
  }
}

private fun <T> runBlockingWithTelemetry(block: suspend () -> T): T {
  return runBlocking(Context.current().asContextElement()) { block() }
}
