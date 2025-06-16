package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Parser
import com.google.protobuf.TypeRegistry
import io.grpc.Channel
import java.io.File
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
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
  cmmsChannel: Channel,
  private val fileSystemRootDirectory: File,
  private val kmsClient: FakeKmsClient,
) :
  ResultsFulfillerApp(
    subscriptionId,
    queueSubscriber,
    parser,
    workItemsClient,
    workItemAttemptsClient,
    cmmsChannel = cmmsChannel,
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
    return TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()
  }

  override fun getStorageConfig(
    configType: StorageConfigType,
    storageParams: ResultsFulfillerParams.StorageParams,
  ): StorageConfig {
    return StorageConfig(rootDirectory = fileSystemRootDirectory)
  }
}
