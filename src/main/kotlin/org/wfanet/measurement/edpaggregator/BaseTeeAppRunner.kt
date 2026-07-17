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

package org.wfanet.measurement.edpaggregator

import com.google.cloud.logging.LoggingHandler
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient
import com.google.cloud.secretmanager.v1.SecretVersionName
import com.google.crypto.tink.KmsClient
import io.grpc.Channel
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
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.tink.GCloudToAwsWifCredentials
import org.wfanet.measurement.common.crypto.tink.GCloudWifCredentials
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig.getConfigAsProtoMessage
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfig
import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfigs
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.gcloud.kms.GCloudKmsClientFactory
import org.wfanet.measurement.gcloud.kms.GCloudToAwsKmsClientFactory
import org.wfanet.measurement.gcloud.pubsub.GooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.queue.QueueSubscriber
import picocli.CommandLine

/**
 * Shared boilerplate for EDPA TEE container CLI entry points (e.g. `ResultsFulfillerAppRunner`,
 * `SubpoolAssignerAppRunner`).
 *
 * Provides the common flags (EDPA mTLS material, Secure Computation mTLS, Pub/Sub subscription,
 * Google project), Secret Manager retrieval, per-EDP [KmsClient] construction via Workload Identity
 * Federation, the mutual-TLS channel to the Secure Computation control plane, and Cloud Logging /
 * OpenTelemetry initialization.
 *
 * Subclasses declare additional flags / wiring and implement [Runnable.run].
 */
abstract class BaseTeeAppRunner : Runnable {
  protected val grpcTelemetry: GrpcTelemetry by lazy {
    GrpcTelemetry.create(Instrumentation.openTelemetry)
  }

  @CommandLine.Option(
    names = ["--edpa-tls-cert-secret-id"],
    description = ["Secret ID of EDPA TLS cert file."],
    defaultValue = "",
  )
  lateinit var edpaCertSecretId: String
    private set

  @CommandLine.Option(
    names = ["--edpa-tls-cert-file-path"],
    description = ["Local path where the --edpa-tls-cert-secret-id secret is stored."],
    defaultValue = "",
  )
  lateinit var edpaCertFilePath: String
    private set

  @CommandLine.Option(
    names = ["--edpa-tls-key-secret-id"],
    description = ["Secret ID of EDPA TLS key file."],
    defaultValue = "",
  )
  lateinit var edpaPrivateKeySecretId: String
    private set

  @CommandLine.Option(
    names = ["--edpa-tls-key-file-path"],
    description = ["Local path where the --edpa-tls-key-secret-id secret is stored."],
    defaultValue = "",
  )
  lateinit var edpaPrivateKeyFilePath: String
    private set

  @CommandLine.Option(
    names = ["--secure-computation-cert-collection-secret-id"],
    description = ["Secret ID of SecureComputation Trusted root Cert collection file."],
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
  protected lateinit var secureComputationPublicApiTarget: String

  @CommandLine.Option(
    names = ["--secure-computation-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Secure Computation public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --secure-computation-public-api-target.",
      ],
    required = false,
  )
  protected var secureComputationPublicApiCertHost: String? = null

  @CommandLine.Option(
    names = ["--metadata-storage-cert-collection-secret-id"],
    description = ["Secret ID of Metadata Storage Trusted root Cert collection file."],
    required = true,
  )
  lateinit var metadataStorageCertCollectionSecretId: String
    private set

  @CommandLine.Option(
    names = ["--metadata-storage-cert-collection-file-path"],
    description =
      ["Local path where the --metadata-storage-cert-collection-secret-id secret is stored."],
    required = true,
  )
  lateinit var metadataStorageCertCollectionFilePath: String
    private set

  @CommandLine.Option(
    names = ["--metadata-storage-public-api-target"],
    description = ["gRPC target of the Metadata Storage public API server"],
    required = true,
  )
  protected lateinit var metadataStoragePublicApiTarget: String

  @CommandLine.Option(
    names = ["--metadata-storage-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Metadata Storage public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --edpa-aggregator-public-api-target.",
      ],
    required = false,
  )
  protected var metadataStoragePublicApiCertHost: String? = null

  @CommandLine.Option(
    names = ["--subscription-id"],
    description = ["Subscription ID for the queue."],
    required = true,
  )
  protected lateinit var subscriptionId: String

  @CommandLine.Option(
    names = ["--google-project-id"],
    description = ["Project ID of EDP Aggregator."],
    required = true,
  )
  lateinit var googleProjectId: String
    private set

  /**
   * Lazily loaded EDPA-level `event-data-provider-configs.textproto`. Held as a companion-level
   * singleton so all subclasses share the same instance.
   */
  protected val edpsConfig: EventDataProviderConfigs
    get() = sharedEdpsConfig

  /**
   * Pulls EDPA mTLS cert and key, plus the Secure Computation and Metadata Storage trusted root
   * cert collections, from Secret Manager and writes them to their configured local paths.
   */
  protected fun saveCommonEdpaCerts() {
    saveSecretToFile(edpaCertSecretId, edpaCertFilePath)
    saveSecretToFile(edpaPrivateKeySecretId, edpaPrivateKeyFilePath)
    saveSecretToFile(
      secureComputationCertCollectionSecretId,
      secureComputationCertCollectionFilePath,
    )
    saveSecretToFile(metadataStorageCertCollectionSecretId, metadataStorageCertCollectionFilePath)
  }

  /**
   * Fetches `secretId` (latest version) from Secret Manager in [googleProjectId] and writes it to
   * [path], creating parent directories as needed.
   */
  protected fun saveSecretToFile(secretId: String, path: String) {
    saveByteArrayToFile(accessSecretBytes(googleProjectId, secretId, SECRET_VERSION), path)
  }

  fun saveByteArrayToFile(bytes: ByteArray, path: String) {
    val file = File(path)
    file.parentFile?.mkdirs()
    file.writeBytes(bytes)
  }

  protected fun accessSecretBytes(projectId: String, secretId: String, version: String): ByteArray {
    return SecretManagerServiceClient.create().use { client ->
      val secretVersionName = SecretVersionName.of(projectId, secretId, version)
      val request =
        AccessSecretVersionRequest.newBuilder().setName(secretVersionName.toString()).build()
      client.accessSecretVersion(request).payload.data.toByteArray()
    }
  }

  /** Builds a single [KmsClient] for the given EDP config via WIF. */
  protected fun buildKmsClient(edpConfig: EventDataProviderConfig): KmsClient {
    return when (edpConfig.kmsConfig.kmsType) {
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
  }

  /** Returns an immutable map of `DataProvider` resource name to [KmsClient]. */
  protected fun buildKmsClientsMap(): Map<String, KmsClient> =
    edpsConfig.eventDataProviderConfigList.associate { edpConfig ->
      edpConfig.dataProvider to buildKmsClient(edpConfig)
    }

  protected fun createQueueSubscriber(pubSubClient: GooglePubSubClient): QueueSubscriber {
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

  /**
   * Builds the mutual-TLS gRPC [Channel] to the Secure Computation public API, wrapped with the
   * gRPC OpenTelemetry interceptor.
   */
  protected fun buildSecureComputationPublicChannel(): Channel =
    buildEdpaMutualTlsChannel(
      target = secureComputationPublicApiTarget,
      trustedCertCollectionFilePath = secureComputationCertCollectionFilePath,
      certHost = secureComputationPublicApiCertHost,
    )

  /**
   * Builds the mutual-TLS gRPC [Channel] to the EDP Aggregator Metadata Storage public API, wrapped
   * with the gRPC OpenTelemetry interceptor.
   */
  protected fun buildMetadataStoragePublicChannel(): Channel =
    buildEdpaMutualTlsChannel(
      target = metadataStoragePublicApiTarget,
      trustedCertCollectionFilePath = metadataStorageCertCollectionFilePath,
      certHost = metadataStoragePublicApiCertHost,
    )

  private fun buildEdpaMutualTlsChannel(
    target: String,
    trustedCertCollectionFilePath: String,
    certHost: String?,
  ): Channel {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = File(edpaCertFilePath),
        privateKeyFile = File(edpaPrivateKeyFilePath),
        trustedCertCollectionFile = File(trustedCertCollectionFilePath),
      )
    return ClientInterceptors.intercept(
      buildMutualTlsChannel(target, clientCerts, certHost),
      grpcTelemetry.newClientInterceptor(),
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(BaseTeeAppRunner::class.java.name)

    private const val SECRET_VERSION = "latest"
    private const val SUBJECT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt"
    private const val TOKEN_URL = "https://sts.googleapis.com/v1/token"
    private const val CREDENTIAL_SOURCE_FILE_PATH =
      "/run/container_launcher/attestation_verifier_claims_token"
    private const val EDP_TARGET_SERVICE_ACCOUNT_FORMAT =
      "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken"
    private const val EVENT_DATA_PROVIDER_CONFIGS_BLOB_KEY = "event-data-provider-configs.textproto"

    private val sharedEdpsConfig: EventDataProviderConfigs by lazy {
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
            if (otelServiceName.isNullOrBlank()) {
              LoggingHandler()
            } else {
              LoggingHandler(otelServiceName)
            }
          rootLogger.addHandler(handler)
          if (otelServiceName.isNullOrBlank()) {
            logger.info("Configured Google Cloud Logging handler for java.util.logging")
          } else {
            logger.info(
              "Configured Google Cloud Logging handler for java.util.logging (logName=$otelServiceName)"
            )
          }
        }
      } catch (e: Exception) {
        logger.log(Level.WARNING, "Failed to configure Google Cloud Logging handler", e)
      }
    }
  }
}

fun <T> runBlockingWithTelemetry(block: suspend () -> T): T {
  return runBlocking(Context.current().asContextElement()) { block() }
}
