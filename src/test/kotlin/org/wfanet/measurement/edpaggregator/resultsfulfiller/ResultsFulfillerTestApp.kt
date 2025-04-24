package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Parser
import com.google.protobuf.TypeRegistry
import java.io.File
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

class ResultsFulfillerTestApp(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  workItemsClient: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub,
  kingdomPublicApiTarget: String,
  kingdomPublicApiCertHost:String,
  trustedCertCollectionFile: File,
  private val requisitionsRootDirectory: File,
  private val labeledImpressionsRootDirectory: File,
  private val labeledImpressionsMetadataRootDirectory: File,
  private val kmsClient: FakeKmsClient
): ResultsFulfillerApp(
  subscriptionId,
  queueSubscriber,
  parser,
  workItemsClient,
  workItemAttemptsClient,
  kingdomPublicApiTarget,
  kingdomPublicApiCertHost,
  trustedCertCollectionFile,
) {
  override fun createStorageClient(
    blobUri: String,
    rootDirectory: File?,
    projectId: String?,
  ): StorageClient {
    return SelectedStorageClient(blobUri, rootDirectory, projectId)
  }

  override fun getKmsClient(): KmsClient {
    return kmsClient
  }

  override fun getTypeRegistry(): TypeRegistry {
    val typeRegistry = TypeRegistry.newBuilder()
      .add(TestEvent.getDescriptor())
      .build()
    return typeRegistry
  }

  override fun getStorageConfig(configType: StorageConfigType, storageDetails: ResultsFulfillerParams.StorageDetails): StorageConfig {
    return StorageConfig(
      rootDirectory = when (configType) {
        StorageConfigType.REQUISITION -> requisitionsRootDirectory
        StorageConfigType.IMPRESSION -> labeledImpressionsRootDirectory
        StorageConfigType.IMPRESSION_METADATA ->
          labeledImpressionsMetadataRootDirectory
      }
    )
  }
}
