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
  private val cmmsChannel: Channel?,
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
      if (cmmsChannel) {
        RequisitionsCoroutineStub(cmmsChannel)
      } else {
        val publicChannel by lazy {
          buildMutualTlsChannel(cmmsTarget, getClientCerts(), cmmsCertHost)
            .withShutdownTimeout(channelShutdownTimeout)
        }
        val requisitionsStub = RequisitionsCoroutineStub(channel)
      }
    val dataProviderCertificateKey: DataProviderCertificateKey = TODO()
    val dataProviderSigningKeyHandle: SigningKeyHandle = TODO()
    val dataProviderPrivateEncryptionKey: TinkPrivateKeyHandle = TODO()

    val kmsClient = getKmsClient()

    ResultsFulfiller(
        dataProviderPrivateEncryptionKey,
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

  private fun getClientCerts(
    certResourcePath: String,
    privateKeyResourcePath: String,
    certCollectionResourcePath: String,
  ): SigningCerts {
    return SigningCerts.fromPemFiles(
      certificateFile = checkNotNull(getRuntimePath(Paths.get(certResourcePath))).toFile(),
      privateKeyFile = checkNotNull(getRuntimePath(Paths.get(privateKeyResourcePath))).toFile(),
      trustedCertCollectionFile =
      checkNotNull(getRuntimePath(Paths.get(certCollectionResourcePath))).toFile(),
    )
  }
}
