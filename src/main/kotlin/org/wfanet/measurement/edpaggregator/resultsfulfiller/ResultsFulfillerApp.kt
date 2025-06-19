package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.protobuf.Parser
import com.google.protobuf.TypeRegistry
import com.google.protobuf.kotlin.toByteString
import io.grpc.Channel
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.time.Duration
import java.time.ZoneOffset
import java.util.*
import kotlinx.coroutines.CompletableDeferred
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.StorageParams
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.storage.StorageClient

enum class StorageConfigType {
  REQUISITION,
  IMPRESSION_METADATA,
  IMPRESSION,
}

abstract class ResultsFulfillerApp(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  workItemsClient: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub,
  private val trustedCertCollection: File? = null,
  private val cmmsTarget: String? = null,
  private val cmmsCertHost: String? = null,
  private val cmmsChannel: Channel? = null,
  private val channelShutdownTimeout: Duration = Duration.ofSeconds(3),
) :
  BaseTeeApplication(
    subscriptionId = subscriptionId,
    queueSubscriber = queueSubscriber,
    parser = parser,
    workItemsStub = workItemsClient,
    workItemAttemptsStub = workItemAttemptsClient,
  ) {

  val messageProcessed = CompletableDeferred<WorkItemParams>()

  abstract fun createStorageClient(
    blobUri: String,
    rootDirectory: File? = null,
    projectId: String? = null,
  ): StorageClient

  abstract fun getKmsClient(): KmsClient

  abstract fun getTypeRegistry(): TypeRegistry

  abstract fun getStorageConfig(
    configType: StorageConfigType,
    storageParams: StorageParams,
  ): StorageConfig

  override suspend fun runWork(message: Any) {
    val workItemParams = message.unpack(WorkItemParams::class.java)
    val fulfillerParams = workItemParams.appParams.unpack(ResultsFulfillerParams::class.java)

    val typeRegistry = getTypeRegistry()
    val requisitionsBlobUri = workItemParams.dataPathParams.dataPath

    val storageParams = fulfillerParams.storageParams

    val requisitionsStorageConfig = getStorageConfig(StorageConfigType.REQUISITION, storageParams)
    val impressionsMetadataStorageConfig =
      getStorageConfig(StorageConfigType.IMPRESSION_METADATA, storageParams)
    val impressionsStorageConfig = getStorageConfig(StorageConfigType.IMPRESSION, storageParams)
    val principalName = fulfillerParams.dataProvider
    val requisitionsStub =
      if (cmmsChannel != null) {
        RequisitionsCoroutineStub(cmmsChannel).withPrincipalName(principalName)
      } else {
        val publicChannel by lazy {
          val signingCerts =
            SigningCerts.fromPemFiles(
              certificateFile =
                checkNotNull(
                    getRuntimePath(Paths.get(fulfillerParams.cmmsConnection.clientCertResourcePath))
                  )
                  .toFile(),
              privateKeyFile =
                checkNotNull(
                    getRuntimePath(
                      Paths.get(fulfillerParams.cmmsConnection.clientPrivateKeyResourcePath)
                    )
                  )
                  .toFile(),
              trustedCertCollectionFile = checkNotNull(trustedCertCollection),
            )
          buildMutualTlsChannel(checkNotNull(cmmsTarget), signingCerts, cmmsCertHost)
            .withShutdownTimeout(channelShutdownTimeout)
        }
        RequisitionsCoroutineStub(publicChannel).withPrincipalName(principalName)
      }
    val dataProviderCertificateKey =
      checkNotNull(
        DataProviderCertificateKey.fromName(fulfillerParams.consentParams.edpCertificateName)
      )
    val consentCertificateFile =
      checkNotNull(
          getRuntimePath(Paths.get(fulfillerParams.consentParams.resultCsCertDerResourcePath))
        )
        .toFile()

    val consentPrivateKeyFile =
      checkNotNull(
          getRuntimePath(Paths.get(fulfillerParams.consentParams.resultCsPrivateKeyDerResourcePath))
        )
        .toFile()
    val encryptionPrivateKeyFile =
      checkNotNull(
          getRuntimePath(Paths.get(fulfillerParams.consentParams.privateEncryptionKeyResourcePath))
        )
        .toFile()

    val publicDer = maybeDecodeBase64(consentCertificateFile.readBytes())
    val privateKeyDer = maybeDecodeBase64(consentPrivateKeyFile.readBytes())
    val keysetBytes = maybeDecodeBase64(encryptionPrivateKeyFile.readBytes())

    val decodedKeyFile = Files.createTempFile("decoded-keyset", ".bin").toFile().apply {
      writeBytes(keysetBytes)
    }

    val consentCertificate: X509Certificate =
      publicDer.inputStream().use { input -> readCertificate(input) }
    val consentPrivateEncryptionKey =
      readPrivateKey(privateKeyDer.toByteString(), consentCertificate.publicKey.algorithm)
    val dataProviderResultSigningKeyHandle =
      SigningKeyHandle(consentCertificate, consentPrivateEncryptionKey)

    val kmsClient = getKmsClient()
    val eventReader =
      EventReader(
        kmsClient = kmsClient,
        impressionDekStorageConfig = impressionsMetadataStorageConfig,
        impressionsStorageConfig = impressionsStorageConfig,
        labeledImpressionsDekPrefix =
          fulfillerParams.storageParams.labeledImpressionsBlobDetailsUriPrefix,
      )

    ResultsFulfiller(
        loadPrivateKey(decodedKeyFile),
        requisitionsStub,
        dataProviderCertificateKey,
        dataProviderResultSigningKeyHandle,
        typeRegistry,
        requisitionsBlobUri = requisitionsBlobUri,
        requisitionsStorageConfig = requisitionsStorageConfig,
        random = SecureRandom(),
        zoneId = ZoneOffset.UTC,
        noiserSelector = ContinuousGaussianNoiseSelector(),
        eventReader = eventReader,
      )
      .fulfillRequisitions()

    messageProcessed.complete(workItemParams)
  }

  private fun maybeDecodeBase64(rawBytes: ByteArray): ByteArray {
    val text = String(rawBytes, StandardCharsets.US_ASCII).trim()
    return if (text.matches(Regex("^[A-Za-z0-9+/=\\r\\n]+$"))) {
      Base64.getDecoder().decode(text.replace("\\s+".toRegex(), ""))
    } else {
      rawBytes
    }
  }

}
