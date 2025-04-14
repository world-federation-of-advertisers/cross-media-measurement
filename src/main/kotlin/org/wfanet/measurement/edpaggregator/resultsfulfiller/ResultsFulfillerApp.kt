package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.protobuf.Parser
import com.google.protobuf.TypeRegistry
import java.io.File
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.reduce
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.ConsentDetails
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

abstract class ResultsFulfillerApp(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  workItemsClient: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
) :
  BaseTeeApplication(
    subscriptionId = subscriptionId,
    queueSubscriber = queueSubscriber,
    parser = parser,
    workItemsStub = workItemsClient,
    workItemAttemptsStub = workItemAttemptsClient
  ) {
  private val messageProcessed = CompletableDeferred<WorkItem>()

  protected abstract fun createStorageClient(
    blobUri: String,
    rootDirectory: File? = null,
    projectId: String? = null
  ): StorageClient

  abstract fun getKmsClient(): KmsClient

  abstract fun getTypeRegistry(): TypeRegistry

  override suspend fun runWork(message: Any) {
    val workItem = message.unpack(WorkItem::class.java)
    val workItemParams = workItem.workItemParams.unpack(WorkItemParams::class.java)
    val fulfillerParams = workItemParams.appParams.unpack(ResultsFulfillerParams::class.java)

    val typeRegistry = getTypeRegistry()
    val requisitionsBlobUri = workItemParams.dataPathParams.dataPath

    val consentDetails = fulfillerParams.consentDetails
    val storageConfigDetails = fulfillerParams.storageConfigDetails

    val connectionDetails = fulfillerParams.connectionDetails
    val clientCerts = getClientCerts(connectionDetails)
    val publicChannel =
      buildMutualTlsChannel(
        connectionDetails.kingdomPublicApiTarget,
        clientCerts,
        connectionDetails.kingdomPublicApiCertHost,
      )
    val requisitionsStub = RequisitionsGrpcKt.RequisitionsCoroutineStub(publicChannel)

    val certificateKey = DataProviderCertificateKey.fromName(consentDetails.edpCertificateName) ?: throw Exception(
      "Invalid data provider certificate"
    )

    val dataProviderSigningKeyHandle = getEdpSigningKeyHandle(consentDetails)

    val requisitionsStorageConfig = StorageConfig(
      rootDirectory = getFileFromBlobUri(storageConfigDetails.requisitionsStorageConfig.rootDirectoryFileBlobUri),
      projectId = storageConfigDetails.requisitionsStorageConfig.projectId
    )

    val impressionsMetadataStorageConfig = StorageConfig(
      rootDirectory = getFileFromBlobUri(storageConfigDetails.impressionsMetadataStorageConfig.rootDirectoryFileBlobUri),
      projectId = storageConfigDetails.impressionsMetadataStorageConfig.projectId
    )

    val impressionsStorageConfig = StorageConfig(
      rootDirectory = getFileFromBlobUri(storageConfigDetails.impressionsStorageConfig.rootDirectoryFileBlobUri),
      projectId = storageConfigDetails.impressionsStorageConfig.projectId
    )

    val kmsClient = getKmsClient()

    val privateEncryptionKeyFile = getFileFromBlobUri(consentDetails.privateEncryptionKeyBlobUri)
    val privateEncryptionKey = loadPrivateKey(privateEncryptionKeyFile)

    ResultsFulfiller(
      privateEncryptionKey,
      requisitionsStub,
      certificateKey,
      dataProviderSigningKeyHandle,
      typeRegistry,
      requisitionsBlobUri,
      fulfillerParams.labeledImpressionsMetadataBlobUriPrefix,
      kmsClient,
      impressionsStorageConfig,
      impressionsMetadataStorageConfig,
      requisitionsStorageConfig,
    ).fulfillRequisitions()

    messageProcessed.complete(workItem)
  }

  private suspend fun getClientCerts(connectionDetails: ResultsFulfillerParams.ConnectionDetails): SigningCerts {
    val certificateStorageUri = SelectedStorageClient.parseBlobUri(connectionDetails.clientCertBlobUri)
    val certificateFile: File = getFileFromStorageUri(certificateStorageUri.key)

    val privateKeyStorageUri = SelectedStorageClient.parseBlobUri(connectionDetails.clientPrivateKeyBlobUri)
    val privateKeyFile: File = getFileFromStorageUri(privateKeyStorageUri.key)

    val certCollectionStorageUri = SelectedStorageClient.parseBlobUri(connectionDetails.clientCollectionCertBlobUri)
    val certCollectionStorageFile: File = getFileFromStorageUri(certCollectionStorageUri.key)

    return SigningCerts.fromPemFiles(
      certificateFile = certificateFile,
      privateKeyFile = privateKeyFile,
      trustedCertCollectionFile = certCollectionStorageFile,
    )
  }

  private suspend fun getEdpSigningKeyHandle(consentDetails: ConsentDetails): SigningKeyHandle {
    val resultSigningCertStorageUri = SelectedStorageClient.parseBlobUri(consentDetails.resultCsCertDerBlobUri)
    val resultSigningCertKey = resultSigningCertStorageUri.key
    val resultSigningCertFile = getFileFromStorageUri(resultSigningCertKey)

    val resultSigningKeyStorageUri = SelectedStorageClient.parseBlobUri(consentDetails.resultCsPrivateKeyDerBlobUri)
    val resultSigningKeyKey = resultSigningKeyStorageUri.key
    val resultSigningKeyFile = getFileFromStorageUri(resultSigningKeyKey)

    return loadSigningKey(
      resultSigningCertFile,
      resultSigningKeyFile,
    )
  }

  private suspend fun getFileFromStorageUri(key: String): File {
    val uri = SelectedStorageClient.parseBlobUri(key)
    val storageClient = createStorageClient(key)

    val fileBlob = requireNotNull(storageClient.getBlob(uri.key)) { "Missing blob with key: ${uri.key}" }

    val fileData = fileBlob.read().reduce { acc, byteString -> acc.concat(byteString) }
    val tempFile = File.createTempFile("path", ".der").apply { deleteOnExit() }
    tempFile.outputStream().use { outputStream ->
      fileData.writeTo(outputStream)
    }
    return tempFile
  }

  private suspend fun getFileFromBlobUri(
    blobUri: String,
    rootDirectory: File? = null,
    projectId: String? = null,
  ): File {
    val uri = SelectedStorageClient.parseBlobUri(blobUri)
    val storageClient = createStorageClient(blobUri, rootDirectory, projectId)

    val fileBlob = requireNotNull(storageClient.getBlob(uri.key)) { "Missing blob with key: ${uri.key}" }

    val fileData = fileBlob.read().reduce { acc, byteString -> acc.concat(byteString) }
    val tempFile = File.createTempFile("path", ".der").apply { deleteOnExit() }
    tempFile.outputStream().use { outputStream ->
      fileData.writeTo(outputStream)
    }
    return tempFile
  }
}
