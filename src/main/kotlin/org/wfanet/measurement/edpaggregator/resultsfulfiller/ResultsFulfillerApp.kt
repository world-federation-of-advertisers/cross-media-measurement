package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.protobuf.Parser
import com.google.protobuf.TypeRegistry
import java.io.File
import kotlinx.coroutines.CompletableDeferred
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.StorageDetails
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
        private val requisitionsStub: RequisitionsCoroutineStub,
        private val dataProviderCertificateKey: DataProviderCertificateKey,
        private val dataProviderSigningKeyHandle: SigningKeyHandle,
        private val dataProviderPrivateEncryptionKey: TinkPrivateKeyHandle,
) :
  BaseTeeApplication(
    subscriptionId = subscriptionId,
    queueSubscriber = queueSubscriber,
    parser = parser,
    workItemsStub = workItemsClient,
    workItemAttemptsStub = workItemAttemptsClient
  ) {

  val messageProcessed = CompletableDeferred<WorkItemParams>()

  abstract fun createStorageClient(
    blobUri: String,
    rootDirectory: File? = null,
    projectId: String? = null
  ): StorageClient

  abstract fun getKmsClient(): KmsClient

  abstract fun getTypeRegistry(): TypeRegistry

  abstract fun getStorageConfig(configType: StorageConfigType, storageDetails: StorageDetails): StorageConfig

  override suspend fun runWork(message: Any) {
    val workItemParams = message.unpack(WorkItemParams::class.java)
    val fulfillerParams = workItemParams.appParams.unpack(ResultsFulfillerParams::class.java)

    val typeRegistry = getTypeRegistry()
    val requisitionsBlobUri = workItemParams.dataPathParams.dataPath

    val storageDetails = fulfillerParams.storageDetails

    val requisitionsStorageConfig = getStorageConfig(StorageConfigType.REQUISITION, storageDetails)
    val impressionsMetadataStorageConfig = getStorageConfig(StorageConfigType.IMPRESSION_METADATA, storageDetails)
    val impressionsStorageConfig = getStorageConfig(StorageConfigType.IMPRESSION, storageDetails)

    val kmsClient = getKmsClient()

    ResultsFulfiller(
      dataProviderPrivateEncryptionKey,
      requisitionsStub,
      dataProviderCertificateKey,
      dataProviderSigningKeyHandle,
      typeRegistry,
      requisitionsBlobUri,
      fulfillerParams.storageDetails.labeledImpressionsMetadataBlobUriPrefix,
      kmsClient,
      impressionsStorageConfig,
      impressionsMetadataStorageConfig,
      requisitionsStorageConfig,
    ).fulfillRequisitions()

    messageProcessed.complete(workItemParams)
  }
}
