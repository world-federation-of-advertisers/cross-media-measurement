package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.protobuf.Parser
import com.google.protobuf.TypeRegistry
import java.io.File
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.StorageParams
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

class ResultsFulfillerAppImpl(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  workItemsClient: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub,
  cmmsTarget: String,
  cmmsCertHost: String?,
  trustedCertCollection: File,
): ResultsFulfillerApp(
    subscriptionId,
    queueSubscriber,
    parser,
    workItemsClient,
    workItemAttemptsClient,
    trustedCertCollection=trustedCertCollection,
    cmmsTarget=cmmsTarget,
    cmmsCertHost=cmmsCertHost
) {
  override fun createStorageClient(
    blobUri: String,
    rootDirectory: File?,
    projectId: String?,
  ): StorageClient {
    return SelectedStorageClient(blobUri, rootDirectory, projectId)
  }

  override fun getKmsClient(): KmsClient {
    // TODO: add credentials
    return GcpKmsClient().withDefaultCredentials()
  }

  override fun getTypeRegistry(): TypeRegistry {
    // TODO: add event production event templates
    val typeRegistry = TypeRegistry.newBuilder()
//      .add()
      .build()
    return typeRegistry
  }

  override fun getStorageConfig(configType: StorageConfigType, storageParams: StorageParams): StorageConfig {
    return StorageConfig(
      projectId = when (configType) {
        StorageConfigType.REQUISITION -> storageParams.gcsProjectId
        StorageConfigType.IMPRESSION -> storageParams.gcsProjectId
        StorageConfigType.IMPRESSION_METADATA ->
          storageParams.gcsProjectId
      }
    )
  }
}
