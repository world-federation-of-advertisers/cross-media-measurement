package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.protobuf.Parser
import com.google.protobuf.TypeRegistry
import io.grpc.Channel
import java.io.File
import java.nio.file.Paths
import kotlinx.coroutines.CompletableDeferred
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.Storage
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import java.security.cert.X509Certificate
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import java.time.Duration
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey

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
    storageDetails: Storage,
  ): StorageConfig

  override suspend fun runWork(message: Any) {
    val workItemParams = message.unpack(WorkItemParams::class.java)
    val fulfillerParams = workItemParams.appParams.unpack(ResultsFulfillerParams::class.java)

    val typeRegistry = getTypeRegistry()
    val requisitionsBlobUri = workItemParams.dataPathParams.dataPath

    val storageDetails = fulfillerParams.storage

    val requisitionsStorageConfig = getStorageConfig(StorageConfigType.REQUISITION, storageDetails)
    val impressionsMetadataStorageConfig =
      getStorageConfig(StorageConfigType.IMPRESSION_METADATA, storageDetails)
    val impressionsStorageConfig = getStorageConfig(StorageConfigType.IMPRESSION, storageDetails)
    val requisitionsStub =
      if (cmmsChannel != null) {
        RequisitionsCoroutineStub(cmmsChannel)
      } else {
        val publicChannel by lazy {
          val signingCerts = SigningCerts.fromPemFiles(
            certificateFile = checkNotNull(getRuntimePath(Paths.get(fulfillerParams.cmmsConnection.clientCertResourcePath))).toFile(),
            privateKeyFile = checkNotNull(getRuntimePath(Paths.get(fulfillerParams.cmmsConnection.clientPrivateKeyResourcePath))).toFile(),
            trustedCertCollectionFile = checkNotNull(trustedCertCollection),
          )
          buildMutualTlsChannel(checkNotNull(cmmsTarget), signingCerts, cmmsCertHost)
            .withShutdownTimeout(channelShutdownTimeout)
        }
        RequisitionsCoroutineStub(publicChannel)
      }
    val dataProviderCertificateKey = checkNotNull(DataProviderCertificateKey.fromName(fulfillerParams.consent.resultCsCertDerResourcePath))
    val consentCertificateFile = checkNotNull(getRuntimePath(Paths.get(fulfillerParams.consent.resultCsCertDerResourcePath))).toFile()
    val consentPrivateKeyFile = checkNotNull(getRuntimePath(Paths.get(fulfillerParams.consent.resultCsPrivateKeyDerResourcePath))).toFile()
    val encryptionPrivateKeyFile = checkNotNull(getRuntimePath(Paths.get(fulfillerParams.consent.privateEncryptionKeyResourcePath))).toFile()
    val consentCertificate: X509Certificate =
      consentCertificateFile.inputStream().use { input -> readCertificate(input) }
    val consentPrivateEncryptionKey = readPrivateKey(consentPrivateKeyFile.readByteString(), consentCertificate.publicKey.algorithm)
    val dataProviderSigningKeyHandle = SigningKeyHandle(
      consentCertificate,
      consentPrivateEncryptionKey
    )

    val kmsClient = getKmsClient()

    ResultsFulfiller(
        loadPrivateKey(encryptionPrivateKeyFile),
        requisitionsStub,
        dataProviderCertificateKey,
        dataProviderSigningKeyHandle,
        typeRegistry,
        requisitionsBlobUri,
        fulfillerParams.storage.labeledImpressionsBlobUriPrefix,
        kmsClient,
        impressionsStorageConfig,
        impressionsMetadataStorageConfig,
        requisitionsStorageConfig,
      )
      .fulfillRequisitions()

    messageProcessed.complete(workItemParams)
  }


}
