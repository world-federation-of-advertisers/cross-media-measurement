package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.Parser
import com.google.protobuf.TypeRegistry
import java.io.File
import java.nio.file.Files
import java.security.Provider
import java.security.cert.X509Certificate
import kotlin.io.path.writeBytes
import kotlinx.coroutines.CompletableDeferred
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.ConnectionDetails
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
        private val kingdomPublicApiTarget: String,
        private val kingdomPublicApiCertHost:String,
        private val trustedCertCollectionFile: File,
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

  fun getRequisitionsStub(connectionDetails: ConnectionDetails, ): RequisitionsCoroutineStub {
    val certificateFile = Files.createTempFile("cert", ".pem").toFile()
    certificateFile.writeBytes(connectionDetails.clientCert.toByteArray())

    val privateKeyFile = Files.createTempFile("key", ".key").toFile()
    privateKeyFile.writeBytes(connectionDetails.clientPrivateKey.toByteArray())

    // Get client certificates from server flags
    val clientCerts = SigningCerts.fromPemFiles(
      certificateFile = certificateFile,
      privateKeyFile = privateKeyFile,
      trustedCertCollectionFile = trustedCertCollectionFile,
    )

    // Build the mutual TLS channel for kingdom public API
    val publicChannel = buildMutualTlsChannel(
      kingdomPublicApiTarget,
      clientCerts,
      kingdomPublicApiCertHost
    )

    // TODO: may need to change if work items client and work item attempts clients use different certs and targets
    return RequisitionsCoroutineStub(publicChannel)
  }

  fun getDataProviderPrivateEncryptionKey(resultCsCertDer: ByteString, resultCsPrivateKeyDer: ByteString): SigningKeyHandle {
    val certificateFile = Files.createTempFile("cert", ".pem").toFile()
    certificateFile.writeBytes(resultCsCertDer.toByteArray())

    val privateKeyFile = Files.createTempFile("key", ".key").toFile()
    privateKeyFile.writeBytes(resultCsPrivateKeyDer.toByteArray())

    return loadSigningKey(
            certificateFile,
            privateKeyFile,
    )
  }

  /** Loads a signing private key from DER files. */
  fun loadSigningKey(certificateDer: File, privateKeyDer: File): SigningKeyHandle {
    val certificate: X509Certificate =
            certificateDer.inputStream().use { input -> readCertificate(input) }
    return SigningKeyHandle(
            certificate,
            readPrivateKey(privateKeyDer.readByteString(), certificate.publicKey.algorithm),
    )
  }

  fun getDataProviderPrivateEncryptionKey(privateEncryptionKey: ByteString): PrivateKeyHandle {
    val encryptionKeyFile = Files.createTempFile("enc_private", ".tink").toFile()
    encryptionKeyFile.writeBytes(privateEncryptionKey.toByteArray())
    return loadPrivateKey(encryptionKeyFile)
  }

  override suspend fun runWork(message: Any) {
    val workItemParams = message.unpack(WorkItemParams::class.java)
    val fulfillerParams = workItemParams.appParams.unpack(ResultsFulfillerParams::class.java)
    fulfillerParams.consentDetails.resultCsCertDer

    // TODO: Logic to create keys
    val dataProviderPrivateEncryptionKey = getDataProviderPrivateEncryptionKey(fulfillerParams.consentDetails.privateEncryptionKey)
    val dataProviderSigningKeyHandle = getDataProviderPrivateEncryptionKey(fulfillerParams.consentDetails.resultCsCertDer, fulfillerParams.consentDetails.resultCsPrivateKeyDer)


    val dataProviderCertificateKey = DataProviderCertificateKey.fromName(fulfillerParams.consentDetails.edpCertificateName)!!

    val typeRegistry = getTypeRegistry()
    val requisitionsBlobUri = workItemParams.dataPathParams.dataPath

    val storageDetails = fulfillerParams.storageDetails

    val requisitionsStorageConfig = getStorageConfig(StorageConfigType.REQUISITION, storageDetails)
    val impressionsMetadataStorageConfig = getStorageConfig(StorageConfigType.IMPRESSION_METADATA, storageDetails)
    val impressionsStorageConfig = getStorageConfig(StorageConfigType.IMPRESSION, storageDetails)

    val kmsClient = getKmsClient()

    val requisitionsStub = getRequisitionsStub(fulfillerParams.connectionDetails)

    ResultsFulfiller(
      dataProviderPrivateEncryptionKey,
      requisitionsStub,
      dataProviderCertificateKey,
      dataProviderSigningKeyHandle,
      typeRegistry,
      requisitionsBlobUri,
      fulfillerParams.storageDetails.labeledImpressionsBlobDetailsUriPrefix,
      kmsClient,
      impressionsStorageConfig,
      impressionsMetadataStorageConfig,
      requisitionsStorageConfig,
    ).fulfillRequisitions()

    messageProcessed.complete(workItemParams)
  }
}
